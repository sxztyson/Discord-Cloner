import hashlib
import logging
import os
from datetime import datetime, timezone
from threading import Event, Lock, Thread
from uuid import uuid4

from flask import Flask, render_template, request, redirect, url_for, abort, jsonify

from scraper import JobCancelled, load_config, save_config, load_duplicates, run_relay, save_duplicates, get_accounts, token_exists, validate_user_token, find_accessible_account, find_accessible_account_for_category, get_channels_in_category

app = Flask(__name__, static_folder="static", template_folder="templates")

# Suppress noisy poll/ping requests from the terminal log
class _QuietFilter(logging.Filter):
    _SKIP = ('/api/status', '/ping')
    def filter(self, record):
        msg = record.getMessage()
        return not any(s in msg for s in self._SKIP)

logging.getLogger('werkzeug').addFilter(_QuietFilter())

@app.template_filter('shortnum')
def shortnum_filter(n):
    n = int(n or 0)
    if n >= 1_000_000:
        s = f"{n/1_000_000:.1f}".rstrip('0').rstrip('.')
        return f"{s}M"
    if n >= 1_000:
        s = f"{n/1_000:.1f}".rstrip('0').rstrip('.')
        return f"{s}k"
    return str(n)

jobs = {}
jobs_lock = Lock()
bot_status = {"connected": False, "username": None, "notify": None, "ready_at": None}

stats = {"total_jobs": 0, "total_messages": 0, "total_media": 0, "channels": set()}
stats_lock = Lock()


class Job:
    def __init__(self, source_channel_id, webhook_url, include_text, clear_cache, media_types, send_text_only, relay_all=False, filter_user_id=None, use_original_author=True, split_media=True, include_forwarded=True, requester_discord_id=None, requester_channel_id=None, account_name=None, job_config=None):
        self.id = uuid4().hex[:10]
        self.source_channel_id = source_channel_id
        self.webhook_url = webhook_url
        self.include_text = include_text
        self.clear_cache = clear_cache
        self.media_types = media_types
        self.send_text_only = send_text_only
        self.relay_all = relay_all
        self.filter_user_id = filter_user_id
        self.use_original_author = use_original_author
        self.split_media = split_media
        self.include_forwarded = include_forwarded
        self.requester_discord_id = requester_discord_id
        self.requester_channel_id = requester_channel_id
        self.account_name = account_name or "Default"
        self.job_config = job_config  # per-job config with the right userToken
        self.status = "queued"
        self.progress = 0
        self.phase_label = ""
        self.logs = []
        self.error = None
        self.result = None
        self.messages_scanned = 0
        self.media_sent = 0
        self.sent_by_type = {"image": 0, "gif": 0, "video": 0, "sticker": 0, "other": 0, "text": 0}
        self.cancel_event = Event()
        self.thread = None
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = self.created_at


def append_job_log(job, message):
    job.logs.append(str(message))
    if len(job.logs) > 250:
        job.logs = job.logs[-250:]
    job.updated_at = datetime.now(timezone.utc)


def update_job_progress(job, percent, message=None):
    job.progress = max(0, min(100, int(percent)))
    if message:
        append_job_log(job, message)
        job.phase_label = message
    job.updated_at = datetime.now(timezone.utc)


def run_job(job, config):
    job.status = "running"
    effective_config = job.job_config if job.job_config else config
    update_job_progress(job, 0, f"Job {job.id} queued. Account: {job.account_name}")

    def logger(message):
        append_job_log(job, message)

    def progress_callback(percent, message=None):
        update_job_progress(job, percent, message)

    def on_batch(count):
        job.messages_scanned += count
        with stats_lock:
            stats["total_messages"] += count

    def on_send(count, media_type="other"):
        job.media_sent += count
        key = media_type if media_type in job.sent_by_type else "other"
        job.sent_by_type[key] += count

    try:
        job.result = run_relay(
            source_channel_id=job.source_channel_id,
            webhook_url=job.webhook_url,
            include_text=job.include_text,
            clear_cache=job.clear_cache,
            media_types=job.media_types,
            send_text_only=job.send_text_only,
            filter_user_id=job.filter_user_id or None,
            use_original_author=job.use_original_author,
            split_media=job.split_media,
            include_forwarded=job.include_forwarded,
            on_batch=on_batch,
            on_send=on_send,
            config=effective_config,
            logger=logger,
            progress_callback=progress_callback,
            cancel_event=job.cancel_event,
        )
        if job.status != "cancelled":
            job.status = "completed"
            update_job_progress(job, 100, f"Job completed. Sent {job.result} items.")
            with stats_lock:
                stats["total_media"] += job.result or 0
            _notify_requester(job,
                f'✅ **Job `{job.id}` finished!** Sent **{job.result}** item(s) '
                f'from channel `{job.source_channel_id}`.')
    except JobCancelled as exc:
        job.status = "cancelled"
        append_job_log(job, f"Job cancelled: {exc}")
        update_job_progress(job, job.progress, "Job cancelled by user.")
        _notify_requester(job, f'⏹️ **Job `{job.id}`** was cancelled.')
    except Exception as exc:
        job.status = "failed"
        job.error = str(exc)
        append_job_log(job, f"Job failed: {exc}")
        update_job_progress(job, job.progress, "Job finished with errors.")
        _notify_requester(job, f'❌ **Job `{job.id}` failed:** `{exc}`')
    finally:
        job.updated_at = datetime.now(timezone.utc)


def _notify_requester(job, text):
    notify = bot_status.get("notify")
    if notify and job.requester_discord_id:
        notify(job.requester_discord_id, job.requester_channel_id, text)


def create_job(form_data, config):
    media_types = [name for name, enabled in form_data['media_types'].items() if enabled]
    if form_data.get('relay_all'):
        media_types = ['image', 'gif', 'video', 'sticker', 'other']

    # Build per-job config with the correct account token
    account_name = form_data.get('account_name', '').strip()
    job_config = None
    accounts = get_accounts(config)
    if account_name:
        match = next((a for a in accounts if a['name'] == account_name), None)
        if match:
            job_config = dict(config)
            job_config['userToken'] = match['token']
    if job_config is None:
        account_name = accounts[0]['name'] if accounts else 'Default'
        job_config = config

    job = Job(
        source_channel_id=form_data['source_channel_id'],
        webhook_url=form_data['webhook_url'],
        include_text=form_data['include_text'],
        clear_cache=form_data['clear_cache'],
        media_types=media_types,
        send_text_only=form_data['send_text_only'],
        relay_all=form_data.get('relay_all', False),
        filter_user_id=form_data.get('filter_user_id', '').strip() or None,
        use_original_author=form_data.get('use_original_author', True),
        split_media=form_data.get('split_media', True),
        include_forwarded=form_data.get('include_forwarded', True),
        requester_discord_id=form_data.get('requester_discord_id'),
        requester_channel_id=form_data.get('requester_channel_id'),
        account_name=account_name,
        job_config=job_config,
    )

    with jobs_lock:
        jobs[job.id] = job

    with stats_lock:
        stats["total_jobs"] += 1
        stats["channels"].add(job.source_channel_id)

    thread = Thread(target=run_job, args=(job, config), daemon=True)
    job.thread = thread
    thread.start()
    return job


def start_bot(config):  # noqa: C901
    import asyncio as _aio
    import discord as _discord
    from discord import app_commands as _apc

    bot_token = config.get('botToken', '').strip()
    if not bot_token:
        return

    cmd_channel_id = None
    if config.get('commandChannelId'):
        try:
            cmd_channel_id = int(config['commandChannelId'])
        except (ValueError, TypeError):
            pass

    cmd_guild_id = None
    if config.get('commandGuildId'):
        try:
            cmd_guild_id = int(config['commandGuildId'])
        except (ValueError, TypeError):
            pass

    loop = _aio.new_event_loop()
    _aio.set_event_loop(loop)
    intents = _discord.Intents.default()
    intents.message_content = True
    client = _discord.Client(intents=intents)
    tree = _apc.CommandTree(client)

    def channel_ok(channel_id):
        return cmd_channel_id is None or channel_id == cmd_channel_id

    # ── Helpers ──────────────────────────────────────────────────────────────

    def make_panel_embed():
        with jobs_lock:
            jlist = list(jobs.values())
        running = sum(1 for j in jlist if j.status == 'running')
        queued  = sum(1 for j in jlist if j.status == 'queued')
        dups    = load_duplicates()
        total_h = sum(len(v) for v in dups.values())
        cfg     = load_config()
        wh_name = cfg.get('webhookName') or 'Original author'
        embed   = _discord.Embed(
            title='📡  Media Relay — Control Panel',
            description='Use the buttons below to control the scraper.',
            color=0x0ea5e9,
        )
        embed.add_field(name='Jobs', value=f'**{len(jlist)}** total · {running} running · {queued} queued', inline=True)
        embed.add_field(name='Cache', value=f'**{total_h}** hashes · {len(dups)} channel(s)', inline=True)
        embed.add_field(name='Webhook name', value=f'`{wh_name}`', inline=True)
        return embed

    # ── Progress bar helpers ─────────────────────────────────────────────────

    def _pbar(percent, width=18):
        filled = int(width * percent / 100)
        return '`' + '█' * filled + '░' * (width - filled) + f'` **{percent}%**'

    _STATUS_EMOJI = {
        'queued': '⏳', 'running': '⚙️', 'cancelling': '⏹️',
        'completed': '✅', 'failed': '❌', 'cancelled': '⏹️',
    }

    def _job_card(job):
        emoji  = _STATUS_EMOJI.get(job.status, '❓')
        media  = 'All' if job.relay_all else ', '.join(job.media_types) or 'none'
        lines  = [
            f'{emoji} **Job `{job.id}`** — {job.status.capitalize()}',
            _pbar(job.progress),
            f'Channel `{job.source_channel_id}` · Account: **{job.account_name}** · Media: {media}',
            f'Sent: **{job.result or 0}** · Msgs scanned: **{job.messages_scanned}**',
        ]
        if job.error:
            lines.append(f'⚠️ `{job.error}`')
        return '\n'.join(lines)

    class JobDoneView(_discord.ui.View):
        def __init__(self):
            super().__init__(timeout=None)

        @_discord.ui.button(label='▶  New Job', style=_discord.ButtonStyle.primary)
        async def new_job_btn(self, interaction: _discord.Interaction, button: _discord.ui.Button):
            await interaction.response.edit_message(
                embed=make_panel_embed(), content=None, view=ControlPanelView()
            )

    async def _live_progress(msg, job):
        while job.status in ('running', 'queued', 'cancelling'):
            await _aio.sleep(5)
            if job.status not in ('running', 'queued', 'cancelling'):
                break
            try:
                await msg.edit(content=_job_card(job))
            except Exception:
                return
        # Final — replace the progress card with a clean done/failed/cancelled line
        status = job.status
        if status == 'completed':
            summary = f'✅ Job done — sent **{job.result or 0}** item(s) from `{job.source_channel_id}`'
        elif status == 'cancelled':
            summary = f'⏹ Job cancelled — sent **{job.result or 0}** item(s)'
        else:
            summary = f'❌ Job failed — `{job.error or "unknown error"}`'
        try:
            await msg.edit(content=summary, view=JobDoneView())
        except Exception:
            pass

    # ── UI: leaf buttons ─────────────────────────────────────────────────────

    class KillJobButton(_discord.ui.Button):
        def __init__(self, job_id, label=None):
            super().__init__(label=label or f'⏹ Kill {job_id}',
                             style=_discord.ButtonStyle.danger,
                             custom_id=f'kill_{job_id}')
            self.job_id = job_id

        async def callback(self, interaction: _discord.Interaction):
            with jobs_lock:
                job = jobs.get(self.job_id)
                if not job:
                    return await interaction.response.send_message('Job not found.', ephemeral=True)
                if job.status in ('completed', 'failed', 'cancelled'):
                    return await interaction.response.send_message('Already finished.', ephemeral=True)
                job.cancel_event.set()
                job.status = 'cancelling'
                append_job_log(job, 'Cancellation requested via button.')
            self.disabled = True
            self.label = '⏹ Cancelling…'
            await interaction.response.edit_message(view=self.view)

    class RemoveJobButton(_discord.ui.Button):
        def __init__(self, job_id, label=None):
            super().__init__(label=label or f'🗑 Remove {job_id}',
                             style=_discord.ButtonStyle.secondary,
                             custom_id=f'remove_{job_id}')
            self.job_id = job_id

        async def callback(self, interaction: _discord.Interaction):
            with jobs_lock:
                job = jobs.get(self.job_id)
                if not job:
                    return await interaction.response.send_message('Not found.', ephemeral=True)
                if job.status not in ('completed', 'failed', 'cancelled'):
                    return await interaction.response.send_message('Still active — kill it first.', ephemeral=True)
                del jobs[self.job_id]
            self.disabled = True
            self.label = '🗑 Removed'
            await interaction.response.edit_message(view=self.view)

    # ── UI: simple views ─────────────────────────────────────────────────────

    class CacheView(_discord.ui.View):
        def __init__(self):
            super().__init__(timeout=120)

        @_discord.ui.button(label='🗑️ Clear All Hashes', style=_discord.ButtonStyle.danger)
        async def clear_btn(self, interaction: _discord.Interaction, button: _discord.ui.Button):
            save_duplicates({})
            button.disabled = True
            button.label = '✅ Cleared'
            await interaction.response.edit_message(content='✅ All duplicate hashes cleared.', view=self)

    class WebhookNameModal(_discord.ui.Modal, title='Set Default Webhook Name'):
        name = _discord.ui.TextInput(label='Display name', placeholder='e.g. Media Relay',
                                     required=False, max_length=80)

        async def on_submit(self, interaction: _discord.Interaction):
            val = self.name.value.strip() or 'Media Relay'
            cfg = load_config()
            cfg['webhookName'] = val
            save_config(cfg)
            config['webhookName'] = val
            await interaction.response.send_message(f'✅ Webhook name set to **{val}**', ephemeral=True)

    class SettingsView(_discord.ui.View):
        def __init__(self):
            super().__init__(timeout=120)

        @_discord.ui.button(label='✏️ Change Webhook Name', style=_discord.ButtonStyle.secondary)
        async def edit_name(self, interaction: _discord.Interaction, button: _discord.ui.Button):
            await interaction.response.send_modal(WebhookNameModal())

    class JobControlView(_discord.ui.View):
        def __init__(self, jlist):
            super().__init__(timeout=180)
            for job in jlist[:5]:
                if job.status in ('running', 'queued', 'cancelling'):
                    self.add_item(KillJobButton(job.id, label=f'⏹ {job.id}'))
                else:
                    self.add_item(RemoveJobButton(job.id, label=f'🗑 {job.id}'))

    # ── UI: account management (merged token + accounts) ────────────────────

    def _ph(passcode: str) -> str:
        return hashlib.sha256(passcode.encode()).hexdigest()

    class AddAccountModal(_discord.ui.Modal, title='Add / Update Account'):
        acct_name = _discord.ui.TextInput(
            label='Account name (e.g. "Main", "Alt 1")',
            placeholder='Friendly label shown in the job list',
            required=True, max_length=40,
        )
        acct_token = _discord.ui.TextInput(
            label='Discord user token',
            placeholder='Paste token — only you see this (ephemeral)',
            required=True, max_length=120,
        )

        async def on_submit(self, interaction: _discord.Interaction):
            name = self.acct_name.value.strip()
            tok  = self.acct_token.value.strip()

            cfg = load_config()

            # Duplicate token check
            existing = token_exists(cfg, tok)
            if existing and existing != name:
                return await interaction.response.send_message(
                    f'⚠️ That token is already saved as account **{existing}**. '
                    f'Each account must use a unique token.',
                    ephemeral=True,
                )

            # Validate token against Discord API
            await interaction.response.defer(ephemeral=True)
            valid, result = await validate_user_token(tok)
            if not valid:
                return await interaction.followup.send(
                    f'❌ Token is invalid — {result}\nNot saved.',
                    ephemeral=True,
                )

            # Save
            accounts = get_accounts(cfg)
            accounts = [a for a in accounts if a['name'] != name]
            accounts.append({'name': name, 'token': tok})
            cfg['accounts'] = accounts
            cfg['userToken'] = accounts[0]['token']
            save_config(cfg)
            config['accounts'] = accounts
            config['userToken'] = cfg['userToken']

            await interaction.followup.send(
                f'✅ Account **{name}** saved — verified as **{result}**.\n'
                f'-# Token never echoed. This message is only visible to you.',
                ephemeral=True,
            )

    class RemoveAccountModal(_discord.ui.Modal, title='Remove Account'):
        acct_name = _discord.ui.TextInput(
            label='Account name to remove',
            placeholder='Exact name as shown in the accounts list',
            required=True, max_length=40,
        )
        passcode = _discord.ui.TextInput(
            label='Passcode',
            placeholder='Required — set via "Set Passcode" button',
            required=True, max_length=64,
        )

        async def on_submit(self, interaction: _discord.Interaction):
            name    = self.acct_name.value.strip()
            entered = self.passcode.value.strip()
            cfg     = load_config()

            stored = cfg.get('accountPasscodeHash')
            if not stored:
                return await interaction.response.send_message(
                    '⚠️ No passcode is set. Use **Set Passcode** first.', ephemeral=True,
                )
            if _ph(entered) != stored:
                return await interaction.response.send_message(
                    '❌ Wrong passcode.', ephemeral=True,
                )

            accounts = get_accounts(cfg)
            if len(accounts) <= 1:
                return await interaction.response.send_message(
                    '⚠️ Cannot remove the only account.', ephemeral=True,
                )
            match = next((a for a in accounts if a['name'] == name), None)
            if not match:
                names = ', '.join(f'**{a["name"]}**' for a in accounts)
                return await interaction.response.send_message(
                    f'⚠️ Account **{name}** not found. Accounts: {names}', ephemeral=True,
                )

            accounts = [a for a in accounts if a['name'] != name]
            cfg['accounts'] = accounts
            cfg['userToken'] = accounts[0]['token']
            save_config(cfg)
            config['accounts'] = accounts
            config['userToken'] = cfg['userToken']

            await interaction.response.send_message(
                f'✅ Account **{name}** removed.\n-# Only visible to you.', ephemeral=True,
            )

    class AccountsView(_discord.ui.View):
        def __init__(self):
            super().__init__(timeout=120)

        @_discord.ui.button(label='➕ Add / Update Account', style=_discord.ButtonStyle.primary)
        async def add_btn(self, interaction: _discord.Interaction, button: _discord.ui.Button):
            await interaction.response.send_modal(AddAccountModal())

        @_discord.ui.button(label='🗑️ Remove Account', style=_discord.ButtonStyle.danger)
        async def remove_btn(self, interaction: _discord.Interaction, button: _discord.ui.Button):
            cfg = load_config()
            if not cfg.get('accountPasscodeHash'):
                return await interaction.response.send_message(
                    '⚠️ No passcode configured. Set `accountPasscode` in config.json or '
                    'the `ACCOUNT_PASSCODE` Replit Secret to enable removals.',
                    ephemeral=True,
                )
            await interaction.response.send_modal(RemoveAccountModal())

    # ── Relay session state (chat-based flow) ───────────────────────────────

    relay_sessions = {}   # discord user_id (int) → RelaySession

    class RelaySession:
        def __init__(self, user_id, discord_channel_id):
            self.user_id             = user_id
            self.discord_channel_id  = discord_channel_id
            self.step                = 'channel_id'  # channel_id | webhook | user_filter | options
            self.channel_id          = None
            self.channel_name        = None
            self.channel_nsfw        = False
            self.webhook_url         = None
            self.user_filter         = None
            self.account_name        = None   # selected account
            self.prompt_msg          = None   # bot message edited in-place
            self.options_view        = None   # RelayOptionsView kept for user_filter return

    category_sessions = {}  # discord user_id (int) → CategorySession

    class CategorySession:
        def __init__(self, user_id, discord_channel_id, category_id, category_name, channels, account_name, account_token):
            self.user_id = user_id
            self.discord_channel_id = discord_channel_id
            self.category_id = category_id
            self.category_name = category_name
            self.channels = channels  # list of {"id": ..., "name": ...}
            self.account_name = account_name
            self.account_token = account_token
            self.prompt_msg = None
            self.selected_media = ['image', 'gif', 'video', 'sticker', 'other', 'captions', 'text_only', 'forwarded']
            self.selected_options = ['use_original_author', 'split_media']


    # ── UI: shared selects ───────────────────────────────────────────────────

    class MediaTypeSelect(_discord.ui.Select):
        def __init__(self):
            super().__init__(
                placeholder='What to relay…',
                min_values=0, max_values=8, row=0,
                options=[
                    _discord.SelectOption(label='Images',                    value='image',      emoji='🖼️', default=True),
                    _discord.SelectOption(label='GIFs',                      value='gif',        emoji='🎞️', default=True),
                    _discord.SelectOption(label='Videos',                    value='video',      emoji='🎬', default=True),
                    _discord.SelectOption(label='Stickers',                  value='sticker',    emoji='🎭', default=True),
                    _discord.SelectOption(label='Other files',               value='other',      emoji='📎', default=True),
                    _discord.SelectOption(label='Text with media (captions)',value='captions',   emoji='📝', default=True),
                    _discord.SelectOption(label='Text-only messages',        value='text_only',  emoji='💬', default=True),
                    _discord.SelectOption(label='Forwarded content',         value='forwarded',  emoji='🔀', default=True),
                ],
            )

        async def callback(self, interaction: _discord.Interaction):
            self.view.selected_media = list(self.values)
            await interaction.response.defer()

    class OptionsSelect(_discord.ui.Select):
        def __init__(self):
            super().__init__(
                placeholder='Options…',
                min_values=0, max_values=4, row=1,
                options=[
                    _discord.SelectOption(label='⚡ Ultimate — Relay Everything', value='relay_all',           emoji='🌐'),
                    _discord.SelectOption(label='Use original author name & avatar', value='use_original_author', emoji='👤', default=True),
                    _discord.SelectOption(label='Split multi-media (one file/msg)',  value='split_media',         emoji='✂️', default=True),
                    _discord.SelectOption(label='Clear duplicate cache first',       value='clear_cache',         emoji='🗑️'),
                ],
            )

        async def callback(self, interaction: _discord.Interaction):
            self.view.selected_options = list(self.values)
            await interaction.response.defer()

    # ── UI: relay options (step 3) ───────────────────────────────────────────

    class AccountSelect(_discord.ui.Select):
        def __init__(self, accounts, default_name):
            options = [
                _discord.SelectOption(
                    label=a['name'],
                    value=a['name'],
                    description=f"Token: {a['token'][:8]}••••••••",
                    default=(a['name'] == default_name),
                )
                for a in accounts[:25]
            ]
            super().__init__(placeholder='Account to use…', min_values=1, max_values=1, row=3, options=options)

        async def callback(self, interaction: _discord.Interaction):
            self.view.session.account_name = self.values[0]
            await interaction.response.defer()

    class RelayOptionsView(_discord.ui.View):
        def __init__(self, session):
            super().__init__(timeout=300)
            self.session          = session
            self.selected_media   = ['image', 'gif', 'video', 'sticker', 'other', 'captions', 'text_only', 'forwarded']
            self.selected_options = ['use_original_author', 'split_media']
            self.add_item(MediaTypeSelect())
            self.add_item(OptionsSelect())
            accounts = get_accounts(config)
            if len(accounts) > 1:
                default = session.account_name or accounts[0]['name']
                session.account_name = default
                self.add_item(AccountSelect(accounts, default))
            elif accounts:
                session.account_name = accounts[0]['name']

        def _content(self):
            uf = f'`{self.session.user_filter}`' if self.session.user_filter else 'everyone'
            acct = f'`{self.session.account_name}`' if self.session.account_name else 'Default'
            return (
                f'📡 **Relay Setup** — Step 3/3\n'
                f'Channel `{self.session.channel_id}` · Webhook: set · User filter: {uf} · Account: {acct}\n'
                f'Pick media types & options, then click **Start Job**.'
            )

        async def on_timeout(self):
            relay_sessions.pop(self.session.user_id, None)
            try:
                await self.session.prompt_msg.edit(content='⏱️ Relay setup timed out.', view=None)
            except Exception:
                pass

        @_discord.ui.button(label='👤  User Filter', style=_discord.ButtonStyle.secondary, row=2)
        async def user_filter_btn(self, interaction: _discord.Interaction, button: _discord.ui.Button):
            self.session.step = 'user_filter'
            uf_note = f' (current: `{self.session.user_filter}`)' if self.session.user_filter else ''
            await interaction.response.edit_message(
                content=(
                    f'📡 **Relay Setup** — User Filter\n'
                    f'Channel `{self.session.channel_id}` · Webhook: set\n\n'
                    f'Type the **user ID** to filter by{uf_note}, or type `skip` to scrape everyone:'
                ),
                view=None,
            )

        @_discord.ui.button(label='🚀  Start Job', style=_discord.ButtonStyle.success, row=2)
        async def start_btn(self, interaction: _discord.Interaction, button: _discord.ui.Button):
            opts      = self.selected_options
            relay_all     = 'relay_all'  in opts
            has_captions  = relay_all or ('captions'  in self.selected_media)
            has_text_only = relay_all or ('text_only' in self.selected_media)
            has_forwarded = relay_all or ('forwarded' in self.selected_media)
            media = {
                k: relay_all or (k in self.selected_media)
                for k in ('image', 'gif', 'video', 'sticker', 'other')
            }
            if not relay_all and not any(media.values()) and not has_captions and not has_text_only:
                return await interaction.response.send_message(
                    '⚠️ Pick at least one content type.', ephemeral=True
                )
            form = {
                'source_channel_id':   self.session.channel_id,
                'webhook_url':         self.session.webhook_url,
                'filter_user_id':      self.session.user_filter or '',
                'relay_all':           relay_all,
                'include_text':        has_captions or has_text_only,
                'send_text_only':      has_text_only,
                'include_forwarded':   has_forwarded,
                'clear_cache':         'clear_cache'         in opts,
                'use_original_author': 'use_original_author' in opts,
                'split_media':         'split_media'         in opts,
                'media_types':         media,
                'account_name':        self.session.account_name or '',
                'requester_discord_id': self.session.user_id,
                'requester_channel_id': self.session.discord_channel_id,
            }
            job        = create_job(form, config)
            relay_sessions.pop(self.session.user_id, None)
            for item in self.children:
                item.disabled = True
            # Initial card — live progress task will keep editing it
            await interaction.response.edit_message(content=_job_card(job), view=self)
            _aio.create_task(_live_progress(self.session.prompt_msg, job))

        @_discord.ui.button(label='✖  Cancel', style=_discord.ButtonStyle.secondary, row=2)
        async def cancel_btn(self, interaction: _discord.Interaction, button: _discord.ui.Button):
            relay_sessions.pop(self.session.user_id, None)
            await interaction.response.edit_message(content='Relay setup cancelled.', view=None)

    class CategoryMediaTypeSelect(_discord.ui.Select):
        def __init__(self):
            super().__init__(
                placeholder='What to relay…',
                min_values=0, max_values=8, row=0,
                options=[
                    _discord.SelectOption(label='Images',                    value='image',      emoji='🖼️', default=True),
                    _discord.SelectOption(label='GIFs',                      value='gif',        emoji='🎞️', default=True),
                    _discord.SelectOption(label='Videos',                    value='video',      emoji='🎬', default=True),
                    _discord.SelectOption(label='Stickers',                  value='sticker',    emoji='🎭', default=True),
                    _discord.SelectOption(label='Other files',               value='other',      emoji='📎', default=True),
                    _discord.SelectOption(label='Text with media (captions)',value='captions',   emoji='📝', default=True),
                    _discord.SelectOption(label='Text-only messages',        value='text_only',  emoji='💬', default=True),
                    _discord.SelectOption(label='Forwarded content',         value='forwarded',  emoji='🔀', default=True),
                ],
            )

        async def callback(self, interaction: _discord.Interaction):
            self.view.selected_media = list(self.values)
            await interaction.response.defer()

    class CategoryOptionsSelect(_discord.ui.Select):
        def __init__(self):
            super().__init__(
                placeholder='Options…',
                min_values=0, max_values=4, row=1,
                options=[
                    _discord.SelectOption(label='⚡ Ultimate — Relay Everything', value='relay_all',           emoji='🌐'),
                    _discord.SelectOption(label='Use original author name & avatar', value='use_original_author', emoji='👤', default=True),
                    _discord.SelectOption(label='Split multi-media (one file/msg)',  value='split_media',         emoji='✂️', default=True),
                    _discord.SelectOption(label='Clear duplicate cache first',       value='clear_cache',         emoji='🗑️'),
                ],
            )

        async def callback(self, interaction: _discord.Interaction):
            self.view.selected_options = list(self.values)
            await interaction.response.defer()

    class CategoryClonerView(_discord.ui.View):
        def __init__(self, session):
            super().__init__(timeout=300)
            self.session = session
            self.selected_media = ['image', 'gif', 'video', 'sticker', 'other', 'captions', 'text_only', 'forwarded']
            self.selected_options = ['use_original_author', 'split_media']
            self.add_item(CategoryMediaTypeSelect())
            self.add_item(CategoryOptionsSelect())

        def _content(self):
            channels_list = ", ".join(f"`#{ch['name']}`" for ch in self.session.channels[:6])
            if len(self.session.channels) > 6:
                channels_list += f" and {len(self.session.channels) - 6} more"
            return (
                f'📁 **Category Cloner Setup**\n'
                f'Source Category: **{self.session.category_name}** (`{self.session.category_id}`)\n'
                f'Found **{len(self.session.channels)}** text channels: {channels_list}\n'
                f'Account: `{self.session.account_name}`\n\n'
                f'Pick media types & options, then click **Create & Sync Category**.'
            )

        async def on_timeout(self):
            category_sessions.pop(self.session.user_id, None)
            try:
                await self.session.prompt_msg.edit(content='⏱️ Category cloner setup timed out.', view=None)
            except Exception:
                pass

        @_discord.ui.button(label='🚀  Create & Sync Category', style=_discord.ButtonStyle.success, row=2)
        async def start_btn(self, interaction: _discord.Interaction, button: _discord.ui.Button):
            if interaction.user.id != self.session.user_id:
                return await interaction.response.send_message("This is not your session.", ephemeral=True)
                
            guild = interaction.guild
            # Check permissions
            bot_member = guild.me
            perms = bot_member.guild_permissions
            if not perms.manage_channels or not perms.manage_webhooks:
                return await interaction.response.send_message(
                    "❌ Error: Bot needs **Manage Channels** and **Manage Webhooks** permissions to run category cloning.",
                    ephemeral=True
                )
                
            # Disable controls to prevent double clicking
            for item in self.children:
                item.disabled = True
            await interaction.response.edit_message(content="⏳ Initializing category creation...", view=self)
            
            # Start a background task to handle creation & job queues
            _aio.create_task(self.run_cloning(interaction))

        async def run_cloning(self, interaction):
            guild = interaction.guild
            try:
                # 1. Create Category
                await self.session.prompt_msg.edit(content=f"📁 Creating category **{self.session.category_name}**...")
                dest_category = await guild.create_category(name=self.session.category_name)
                
                # 2. Loop and create channels + webhooks + jobs
                created_count = 0
                jobs_started = []
                
                opts = self.selected_options
                relay_all = 'relay_all' in opts
                has_captions = relay_all or ('captions' in self.selected_media)
                has_text_only = relay_all or ('text_only' in self.selected_media)
                has_forwarded = relay_all or ('forwarded' in self.selected_media)
                media = {
                    k: relay_all or (k in self.selected_media)
                    for k in ('image', 'gif', 'video', 'sticker', 'other')
                }
                
                # Retrieve bot's avatar as bytes once to avoid repeated calls
                avatar_bytes = None
                try:
                    if client.user.avatar:
                        avatar_bytes = await client.user.avatar.read()
                    else:
                        avatar_bytes = await client.user.default_avatar.read()
                except Exception:
                    pass
                
                for idx, ch in enumerate(self.session.channels, start=1):
                    await self.session.prompt_msg.edit(
                        content=f"⏳ Channel {idx}/{len(self.session.channels)}: Creating `#{ch['name']}`..."
                    )
                    
                    # Create Text Channel
                    dest_ch = await guild.create_text_channel(name=ch["name"], category=dest_category, nsfw=ch.get("nsfw", False))
                    
                    # Create Webhook
                    webhook = await dest_ch.create_webhook(name=ch["name"], avatar=avatar_bytes)
                    
                    # Create job form
                    form = {
                        'source_channel_id': ch["id"],
                        'webhook_url': webhook.url,
                        'filter_user_id': '',
                        'relay_all': relay_all,
                        'include_text': has_captions or has_text_only,
                        'send_text_only': has_text_only,
                        'include_forwarded': has_forwarded,
                        'clear_cache': 'clear_cache' in opts,
                        'use_original_author': 'use_original_author' in opts,
                        'split_media': 'split_media' in opts,
                        'media_types': media,
                        'account_name': self.session.account_name,
                        'requester_discord_id': self.session.user_id,
                        'requester_channel_id': self.session.discord_channel_id,
                    }
                    
                    job = create_job(form, config)
                    jobs_started.append(job.id)
                    created_count += 1
                    
                # Done!
                category_sessions.pop(self.session.user_id, None)
                
                # Format final success message
                job_ids_str = ", ".join(f"`{jid}`" for jid in jobs_started[:5])
                if len(jobs_started) > 5:
                    job_ids_str += f" and {len(jobs_started) - 5} more"
                    
                embed = _discord.Embed(
                    title="✅ Category Cloned Successfully!",
                    description=(
                        f"Created category **{self.session.category_name}** with **{created_count}** channels.\n"
                        f"Queued **{len(jobs_started)}** relay jobs: {job_ids_str}.\n\n"
                        f"You can monitor job progress in the web dashboard or using `/info`."
                    ),
                    color=0x22c55e
                )
                await self.session.prompt_msg.edit(content=None, embed=embed, view=None)
                
            except Exception as e:
                # Handle error
                embed = _discord.Embed(
                    title="❌ Category Cloning Failed",
                    description=f"An error occurred: `{str(e)}`",
                    color=0xef4444
                )
                await self.session.prompt_msg.edit(content=None, embed=embed, view=None)
                category_sessions.pop(self.session.user_id, None)

        @_discord.ui.button(label='✖  Cancel', style=_discord.ButtonStyle.secondary, row=2)
        async def cancel_btn(self, interaction: _discord.Interaction, button: _discord.ui.Button):
            if interaction.user.id != self.session.user_id:
                return await interaction.response.send_message("This is not your session.", ephemeral=True)
            category_sessions.pop(self.session.user_id, None)
            await interaction.response.edit_message(content='Category cloning cancelled.', view=None)


    # ── UI: channel ID (private modal at step 1) ─────────────────────────────

    class CategoryIDModal(_discord.ui.Modal, title='Enter Source Category ID (private)'):
        category_id_field = _discord.ui.TextInput(
            label='Source Category ID',
            placeholder='Right-click the category → Copy Category ID',
            required=True,
            max_length=25,
        )

        def __init__(self, user_id, discord_channel_id):
            super().__init__()
            self.user_id = user_id
            self.discord_channel_id = discord_channel_id

        async def on_submit(self, interaction: _discord.Interaction):
            category_id = self.category_id_field.value.strip()
            if not category_id.isdigit() or len(category_id) < 15:
                return await interaction.response.send_message(
                    f'⚠️ `{category_id}` doesn\'t look like a valid category ID (numbers only, 15+ digits).',
                    ephemeral=True,
                )
                
            await interaction.response.defer(ephemeral=True)
            
            cfg = load_config()
            # Find accessible account
            acct_name, acct_token = await find_accessible_account_for_category(cfg, category_id)
            if not acct_token:
                return await interaction.followup.send(
                    "❌ Error: None of the configured user tokens have access to read this category.",
                    ephemeral=True
                )
                
            # Get category details
            cat_name, text_channels, err = await get_channels_in_category(acct_token, category_id)
            if err:
                return await interaction.followup.send(f"❌ Error: {err}", ephemeral=True)
                
            if not text_channels:
                return await interaction.followup.send(
                    f"❌ Error: Category **{cat_name}** does not contain any text or announcement channels.",
                    ephemeral=True
                )
                
            # Create session
            session = CategorySession(
                user_id=self.user_id,
                discord_channel_id=self.discord_channel_id,
                category_id=category_id,
                category_name=cat_name,
                channels=text_channels,
                account_name=acct_name,
                account_token=acct_token
            )
            category_sessions[self.user_id] = session
            
            view = CategoryClonerView(session)
            session.prompt_msg = await interaction.followup.send(content=view._content(), view=view, ephemeral=True)

    class ChannelIDModal(_discord.ui.Modal, title='Enter Source Channel ID (private)'):
        channel_id_field = _discord.ui.TextInput(
            label='Source Channel ID',
            placeholder='Right-click the channel → Copy Channel ID',
            required=True,
            max_length=25,
        )

        def __init__(self, user_id, discord_channel_id):
            super().__init__()
            self.user_id = user_id
            self.discord_channel_id = discord_channel_id

        async def on_submit(self, interaction: _discord.Interaction):
            text = self.channel_id_field.value.strip()
            if not text.isdigit() or len(text) < 15:
                return await interaction.response.send_message(
                    f'⚠️ `{text}` doesn\'t look like a valid channel ID (numbers only, 15+ digits).',
                    ephemeral=True,
                )
                
            await interaction.response.defer(ephemeral=True)
            
            cfg = load_config()
            # Find accessible account
            acct_name, acct_token = await find_accessible_account(cfg, text)
            if not acct_token:
                return await interaction.followup.send(
                    "❌ Error: None of the configured user tokens have access to read this channel.",
                    ephemeral=True
                )
                
            # Fetch channel details
            import aiohttp
            channel_name = "scraped-channel"
            channel_nsfw = False
            headers = {
                "Authorization": acct_token,
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            }
            try:
                async with aiohttp.ClientSession(headers=headers) as s:
                    async with s.get(f"https://discord.com/api/v10/channels/{text}", timeout=10) as r:
                        if r.status == 200:
                            ch_data = await r.json()
                            channel_name = ch_data.get("name", "scraped-channel")
                            channel_nsfw = ch_data.get("nsfw", False)
            except Exception:
                pass
                
            session = RelaySession(self.user_id, self.discord_channel_id)
            session.channel_id = text
            session.channel_name = channel_name
            session.channel_nsfw = channel_nsfw
            session.account_name = acct_name
            session.step = 'webhook'
            relay_sessions[self.user_id] = session
            
            view = WebhookDestinationView(session)
            session.prompt_msg = await interaction.followup.send(
                f'📡 **Relay Setup** <@{self.user_id}> — Step 2/3\n'
                f'Source Channel: `#{channel_name}` (`{text}`)\n\n'
                f'Choose where to post the relayed messages:',
                view=view,
                ephemeral=True
            )

    # ── UI: webhook URL (private modal at step 2) ────────────────────────────

    class WebhookModal(_discord.ui.Modal, title='Enter Webhook URL (private)'):
        url_field = _discord.ui.TextInput(
            label='Destination Webhook URL',
            placeholder='https://discord.com/api/webhooks/…',
            required=True,
            max_length=250,
        )

        def __init__(self, session):
            super().__init__()
            self.session = session

        async def on_submit(self, interaction: _discord.Interaction):
            text = self.url_field.value.strip()
            valid = (text.startswith('https://discord.com/api/webhooks/') or
                     text.startswith('https://discordapp.com/api/webhooks/'))
            if not valid:
                return await interaction.response.send_message(
                    '⚠️ That doesn\'t look like a Discord webhook URL. '
                    'Click the button again to retry.', ephemeral=True,
                )
            self.session.webhook_url = text
            self.session.step = 'options'
            view = RelayOptionsView(self.session)
            self.session.options_view = view
            await self.session.prompt_msg.edit(content=view._content(), view=view)
            await interaction.response.send_message(
                '✅ Webhook URL saved privately — never shown in chat.', ephemeral=True,
            )

    class DestinationChannelSelect(_discord.ui.ChannelSelect):
        def __init__(self):
            super().__init__(
                placeholder="Pick an existing channel...",
                channel_types=[_discord.ChannelType.text, _discord.ChannelType.news],
                min_values=1,
                max_values=1,
                row=0
            )

        async def callback(self, interaction: _discord.Interaction):
            if interaction.user.id != self.view.session.user_id:
                return await interaction.response.send_message('This isn\'t your setup.', ephemeral=True)
                
            selected_app_ch = self.values[0]
            guild = interaction.guild
            selected_ch = guild.get_channel(selected_app_ch.id)
            if not selected_ch:
                try:
                    selected_ch = await guild.fetch_channel(selected_app_ch.id)
                except Exception:
                    return await interaction.response.send_message(
                        "❌ Error: Could not find or access the selected channel in this server.",
                        ephemeral=True
                    )
            # Check permissions
            bot_member = guild.me
            perms = bot_member.guild_permissions
            if not perms.manage_webhooks:
                return await interaction.response.send_message(
                    "❌ Error: Bot needs **Manage Webhooks** permission to create webhooks automatically.",
                    ephemeral=True
                )
                
            await interaction.response.defer(ephemeral=True)
            
            # Fetch bot avatar
            avatar_bytes = None
            try:
                if client.user.avatar:
                    avatar_bytes = await client.user.avatar.read()
                else:
                    avatar_bytes = await client.user.default_avatar.read()
            except Exception:
                pass
                
            # Create webhook
            try:
                webhook = await selected_ch.create_webhook(name=selected_ch.name, avatar=avatar_bytes)
                self.view.session.webhook_url = webhook.url
                self.view.session.step = 'options'
                
                # Show Step 3 options view
                opt_view = RelayOptionsView(self.view.session)
                self.view.session.options_view = opt_view
                await self.view.session.prompt_msg.edit(content=opt_view._content(), view=opt_view)
                await interaction.followup.send(f"✅ Webhook created in `#{selected_ch.name}` automatically.", ephemeral=True)
            except Exception as e:
                await interaction.followup.send(f"❌ Failed to create webhook: `{e}`", ephemeral=True)

    class WebhookDestinationView(_discord.ui.View):
        def __init__(self, session):
            super().__init__(timeout=300)
            self.session = session
            self.add_item(DestinationChannelSelect())

        async def on_timeout(self):
            relay_sessions.pop(self.session.user_id, None)
            try:
                await self.session.prompt_msg.edit(content='⏱️ Relay setup timed out.', view=None)
            except Exception:
                pass

        @_discord.ui.button(label='🆕  Create Channel', style=_discord.ButtonStyle.success, row=1)
        async def create_ch_btn(self, interaction: _discord.Interaction, button: _discord.ui.Button):
            if interaction.user.id != self.session.user_id:
                return await interaction.response.send_message('This isn\'t your setup.', ephemeral=True)
                
            guild = interaction.guild
            # Check permissions
            bot_member = guild.me
            perms = bot_member.guild_permissions
            if not perms.manage_channels or not perms.manage_webhooks:
                return await interaction.response.send_message(
                    "❌ Error: Bot needs **Manage Channels** and **Manage Webhooks** permissions to clone channels.",
                    ephemeral=True
                )
                
            await interaction.response.defer(ephemeral=True)
            
            try:
                # 1. Create text channel
                dest_ch = await guild.create_text_channel(
                    name=self.session.channel_name,
                    category=interaction.channel.category if isinstance(interaction.channel, _discord.TextChannel) else None,
                    nsfw=self.session.channel_nsfw
                )
                
                # 2. Fetch bot avatar
                avatar_bytes = None
                try:
                    if client.user.avatar:
                        avatar_bytes = await client.user.avatar.read()
                    else:
                        avatar_bytes = await client.user.default_avatar.read()
                except Exception:
                    pass
                    
                # 3. Create webhook
                webhook = await dest_ch.create_webhook(name=dest_ch.name, avatar=avatar_bytes)
                
                self.session.webhook_url = webhook.url
                self.session.step = 'options'
                
                # Show Step 3 options view
                opt_view = RelayOptionsView(self.session)
                self.session.options_view = opt_view
                await self.session.prompt_msg.edit(content=opt_view._content(), view=opt_view)
                await interaction.followup.send(f"✅ Created channel `#{dest_ch.name}` and generated webhook.", ephemeral=True)
            except Exception as e:
                await interaction.followup.send(f"❌ Failed to create channel/webhook: `{e}`", ephemeral=True)

        @_discord.ui.button(label='🔗  Enter Webhook URL (private)', style=_discord.ButtonStyle.primary, row=1)
        async def enter_btn(self, interaction: _discord.Interaction, button: _discord.ui.Button):
            if interaction.user.id != self.session.user_id:
                return await interaction.response.send_message('This isn\'t your setup.', ephemeral=True)
            await interaction.response.send_modal(WebhookModal(self.session))

        @_discord.ui.button(label='✖  Cancel', style=_discord.ButtonStyle.secondary, row=1)
        async def cancel_btn(self, interaction: _discord.Interaction, button: _discord.ui.Button):
            if interaction.user.id != self.session.user_id:
                return await interaction.response.send_message('This isn\'t your setup.', ephemeral=True)
            relay_sessions.pop(self.session.user_id, None)
            await interaction.response.edit_message(content='Relay setup cancelled.', view=None)

    # ── UI: main control panel ───────────────────────────────────────────────

    class ControlPanelView(_discord.ui.View):
        def __init__(self):
            super().__init__(timeout=None)

        @_discord.ui.button(label='▶  Start Relay', style=_discord.ButtonStyle.primary,
                            custom_id='cp_start', row=0)
        async def start_btn(self, interaction: _discord.Interaction, button: _discord.ui.Button):
            user_id = interaction.user.id
            if user_id in relay_sessions:
                return await interaction.response.send_message(
                    '⚠️ You already have an active relay setup in progress — '
                    'finish or cancel it first.', ephemeral=True
                )
            await interaction.response.send_modal(ChannelIDModal(user_id, interaction.channel_id))

        @_discord.ui.button(label='📁  Scrape Category', style=_discord.ButtonStyle.primary,
                            custom_id='cp_category', row=0)
        async def category_btn(self, interaction: _discord.Interaction, button: _discord.ui.Button):
            user_id = interaction.user.id
            if user_id in category_sessions:
                return await interaction.response.send_message(
                    '⚠️ You already have an active category cloner setup in progress — '
                    'finish or cancel it first.', ephemeral=True
                )
            await interaction.response.send_modal(CategoryIDModal(user_id, interaction.channel_id))

        @_discord.ui.button(label='📋  Jobs', style=_discord.ButtonStyle.secondary,
                            custom_id='cp_jobs', row=0)
        async def jobs_btn(self, interaction: _discord.Interaction, button: _discord.ui.Button):
            with jobs_lock:
                jlist = sorted(jobs.values(), key=lambda j: j.created_at, reverse=True)
            if not jlist:
                return await interaction.response.send_message('No jobs yet.', ephemeral=True)
            embed = _discord.Embed(title=f'Jobs ({len(jlist)})', color=0x0ea5e9)
            for j in jlist[:8]:
                user_tag   = f'\nUser: `{j.filter_user_id}`' if j.filter_user_id else ''
                err_tag    = f'\n⚠️ {j.error}'              if j.error           else ''
                author_tag = ' · orig' if j.use_original_author else ' · custom'
                split_tag  = ' · split' if j.split_media else ' · bundled'
                media_tag  = 'All' if j.relay_all else ', '.join(j.media_types)
                embed.add_field(
                    name=f'`{j.id}`  {j.status} {j.progress}%',
                    value=f'`{j.source_channel_id}`{user_tag}\n{media_tag}{author_tag}{split_tag}{err_tag}',
                    inline=True,
                )
            await interaction.response.send_message(embed=embed, view=JobControlView(jlist), ephemeral=True)

        @_discord.ui.button(label='📜  Logs', style=_discord.ButtonStyle.secondary,
                            custom_id='cp_logs', row=0)
        async def logs_btn(self, interaction: _discord.Interaction, button: _discord.ui.Button):
            with jobs_lock:
                jlist = sorted(jobs.values(), key=lambda j: j.updated_at, reverse=True)
            if not jlist:
                return await interaction.response.send_message('No jobs yet.', ephemeral=True)
            job    = jlist[0]
            recent = job.logs[-20:] if job.logs else []
            if not recent:
                return await interaction.response.send_message(f'No logs for job `{job.id}` yet.', ephemeral=True)
            body = '\n'.join(recent)
            if len(body) > 1800:
                body = '…' + body[-1799:]
            await interaction.response.send_message(
                f'**Logs — job `{job.id}`** ({job.status} {job.progress}%):\n```\n{body}\n```',
                ephemeral=True,
            )

        @_discord.ui.button(label='💾  Cache', style=_discord.ButtonStyle.secondary,
                            custom_id='cp_cache', row=0)
        async def cache_btn(self, interaction: _discord.Interaction, button: _discord.ui.Button):
            dups  = load_duplicates()
            total = sum(len(v) for v in dups.values())
            await interaction.response.send_message(
                f'**Duplicate Cache**\n{len(dups)} channel(s) · **{total}** hashes',
                view=CacheView(), ephemeral=True,
            )

        @_discord.ui.button(label='👥  Accounts', style=_discord.ButtonStyle.primary,
                            custom_id='cp_accounts', row=1)
        async def accounts_btn(self, interaction: _discord.Interaction, button: _discord.ui.Button):
            cfg = load_config()
            accounts = get_accounts(cfg)
            passcode_set = bool(cfg.get('accountPasscodeHash'))
            embed = _discord.Embed(
                title=f'Accounts ({len(accounts)})',
                description=(
                    '➕ **Add / Update Account** — validates token live, blocks duplicates\n'
                    '🗑️ **Remove Account** — requires master passcode (set in config/secrets)'
                    + ('\n\n⚠️ No passcode configured — removals are disabled.' if not passcode_set else
                       '\n\n🔒 Passcode configured.')
                ),
                color=0x0ea5e9,
            )
            for a in accounts:
                embed.add_field(name=a['name'], value=f'`{a["token"][:8]}••••••••`', inline=True)
            await interaction.response.send_message(embed=embed, view=AccountsView(), ephemeral=True)

        @_discord.ui.button(label='⚙️  Settings', style=_discord.ButtonStyle.secondary,
                            custom_id='cp_settings', row=1)
        async def settings_btn(self, interaction: _discord.Interaction, button: _discord.ui.Button):
            cfg = load_config()
            embed = _discord.Embed(title='Settings', color=0x0ea5e9)
            embed.add_field(name='Max File Size', value=f'{cfg.get("maxFileSizeMb", 25)} MB',        inline=True)
            embed.add_field(name='Webhook Name',  value=cfg.get('webhookName') or 'Server default',  inline=True)
            await interaction.response.send_message(embed=embed, view=SettingsView(), ephemeral=True)

        @_discord.ui.button(label='🔄  Refresh', style=_discord.ButtonStyle.secondary,
                            custom_id='cp_refresh', row=1)
        async def refresh_btn(self, interaction: _discord.Interaction, button: _discord.ui.Button):
            await interaction.response.edit_message(embed=make_panel_embed(), view=self)

    # ── Slash commands (3 only) ──────────────────────────────────────────────

    def _slash_channel_ok(interaction):
        return cmd_channel_id is None or interaction.channel_id == cmd_channel_id

    async def _slash_deny(interaction):
        await interaction.response.send_message('Commands restricted to a specific channel.', ephemeral=True)

    @tree.command(name='scrape_category', description='Scrape all channels under a source category and clone them here')
    @_apc.describe(category_id='The ID of the source category to clone')
    async def cmd_scrape_category(interaction: _discord.Interaction, category_id: str):
        if not _slash_channel_ok(interaction):
            return await _slash_deny(interaction)
            
        if not interaction.guild:
            return await interaction.response.send_message("⚠️ This command can only be used in a server.", ephemeral=True)
            
        user_id = interaction.user.id
        if user_id in category_sessions:
            return await interaction.response.send_message("⚠️ You already have an active category cloner session. Complete or cancel it first.", ephemeral=True)
            
        await interaction.response.defer(ephemeral=True)
        
        cfg = load_config()
        # Find accessible account
        acct_name, acct_token = await find_accessible_account_for_category(cfg, category_id)
        if not acct_token:
            return await interaction.followup.send(
                "❌ Error: None of the configured user tokens have access to read this category.",
                ephemeral=True
            )
            
        # Get category details
        cat_name, text_channels, err = await get_channels_in_category(acct_token, category_id)
        if err:
            return await interaction.followup.send(f"❌ Error: {err}", ephemeral=True)
            
        if not text_channels:
            return await interaction.followup.send(
                f"❌ Error: Category **{cat_name}** does not contain any text or announcement channels.",
                ephemeral=True
            )
            
        # Create session
        session = CategorySession(
            user_id=user_id,
            discord_channel_id=interaction.channel_id,
            category_id=category_id,
            category_name=cat_name,
            channels=text_channels,
            account_name=acct_name,
            account_token=acct_token
        )
        category_sessions[user_id] = session
        
        view = CategoryClonerView(session)
        session.prompt_msg = await interaction.followup.send(content=view._content(), view=view, ephemeral=True)

    @tree.command(name='scrape', description='Open the scraper control panel')
    async def cmd_scrape(interaction: _discord.Interaction):
        if not _slash_channel_ok(interaction):
            return await _slash_deny(interaction)
        await interaction.response.send_message(embed=make_panel_embed(), view=ControlPanelView())

    @tree.command(name='scraper', description='Open the scraper control panel')
    async def cmd_scraper(interaction: _discord.Interaction):
        if not _slash_channel_ok(interaction):
            return await _slash_deny(interaction)
        await interaction.response.send_message(embed=make_panel_embed(), view=ControlPanelView())

    @tree.command(name='help', description='How to use the scraper bot')
    async def cmd_help(interaction: _discord.Interaction):
        embed = _discord.Embed(title='📡 Media Relay — Help', color=0x0ea5e9)
        embed.add_field(
            name='Opening the dashboard',
            value='Type `!scraper` in chat or use `/scraper` — drops the full control panel.',
            inline=False,
        )
        embed.add_field(
            name='▶ Start Relay',
            value='Step 1: type channel ID in chat · Step 2: click private button for webhook URL\n'
                  'Step 3: pick content types + options → Start Job',
            inline=False,
        )
        embed.add_field(
            name='📁 Category Cloner',
            value='Use `/scrape_category <category_id>` or `!scrape_category <category_id>` in a server.\n'
                  'Recreates the entire category and its text channels, makes webhooks, and queues up all relay jobs automatically in one click.',
            inline=False,
        )
        embed.add_field(
            name='Content types (MediaTypeSelect)',
            value='🖼️ Images · 🎞️ GIFs · 🎬 Videos · 🎭 Stickers · 📎 Other files\n'
                  '💬 **Text messages** — include & relay text content\n'
                  '🔀 **Forwarded content** — include text from forwarded message snapshots',
            inline=False,
        )
        embed.add_field(
            name='Options',
            value='⚡ **Ultimate** — enables every content type + all options\n'
                  '👤 **Use original author** — webhook shows sender\'s name/avatar\n'
                  '✂️ **Split multi-media** — one file per message (off = bundle up to 10)\n'
                  '🗑️ **Clear cache** — flush duplicate hashes before starting',
            inline=False,
        )
        embed.add_field(
            name='📋 Jobs / 📜 Logs / 💾 Cache',
            value='**Jobs** — view all jobs, kill running ones, remove finished ones\n'
                  '**Logs** — show the last 20 log lines from the most recent job\n'
                  '**Cache** — view duplicate hash counts and clear all hashes',
            inline=False,
        )
        embed.add_field(
            name='👥 Accounts / ⚙️ Settings',
            value='**Accounts** — add/update tokens (validates live), remove with passcode, no duplicate tokens allowed\n'
                  '**Settings** — change the default webhook display name',
            inline=False,
        )
        embed.set_footer(text='Use /info for live statistics.')
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @tree.command(name='info', description='Show detailed scraper statistics')
    async def cmd_info(interaction: _discord.Interaction):
        if not _slash_channel_ok(interaction):
            return await _slash_deny(interaction)
        cfg = load_config()
        with jobs_lock:
            jlist = list(jobs.values())

        now = _discord.utils.utcnow()

        # ── Counts ───────────────────────────────────────────────────────────
        running_jobs = [j for j in jlist if j.status == 'running']
        queued_jobs  = [j for j in jlist if j.status == 'queued']
        done_jobs    = [j for j in jlist if j.status == 'completed']
        fail_jobs    = [j for j in jlist if j.status == 'failed']
        cancel_jobs  = [j for j in jlist if j.status == 'cancelled']

        ses_media = sum(j.result or 0 for j in jlist)
        ses_msgs  = sum(j.messages_scanned for j in jlist)
        ses_chs   = len({j.source_channel_id for j in jlist})

        finished = len(done_jobs) + len(fail_jobs) + len(cancel_jobs)
        success_rate = round(len(done_jobs) / finished * 100) if finished else 0
        avg_items = round(ses_media / len(done_jobs), 1) if done_jobs else 0

        # ── Lifetime stats ───────────────────────────────────────────────────
        with stats_lock:
            lt_jobs  = stats['total_jobs']
            lt_media = stats['total_media']
            lt_msgs  = stats['total_messages']
            lt_chs   = len(stats['channels'])

        # ── Config / cache ───────────────────────────────────────────────────
        tok = cfg.get('userToken', '')
        tok_display = f'`{tok[:8]}••••••••`' if len(tok) >= 8 else '`[not set]`'
        dups    = load_duplicates()
        total_h = sum(len(v) for v in dups.values())

        # ── Uptime ───────────────────────────────────────────────────────────
        ready_at = bot_status.get('ready_at')
        if ready_at:
            delta   = now - ready_at
            hours, rem = divmod(int(delta.total_seconds()), 3600)
            mins, secs = divmod(rem, 60)
            uptime_str = f'{hours}h {mins}m {secs}s'
        else:
            uptime_str = 'unknown'

        # ── Build embed ──────────────────────────────────────────────────────
        embed = _discord.Embed(
            title='📊 Scraper Statistics',
            color=0x0ea5e9,
            timestamp=now,
        )
        # Row 1 — config
        embed.add_field(name='User Token',    value=tok_display,                           inline=True)
        embed.add_field(name='Max File Size', value=f'{cfg.get("maxFileSizeMb", 25)} MB', inline=True)
        embed.add_field(name='Uptime',        value=uptime_str,                            inline=True)

        # Row 2 — cache
        embed.add_field(
            name='Duplicate Cache',
            value=f'**{total_h}** hashes across **{len(dups)}** channel(s)',
            inline=False,
        )

        # Row 3 — session summary
        embed.add_field(
            name='Session — Jobs',
            value=(f'Total: **{len(jlist)}** · Running: **{len(running_jobs)}** · '
                   f'Queued: **{len(queued_jobs)}**\n'
                   f'Done: **{len(done_jobs)}** · Failed: **{len(fail_jobs)}** · '
                   f'Cancelled: **{len(cancel_jobs)}**\n'
                   f'Success rate: **{success_rate}%**'),
            inline=True,
        )
        embed.add_field(
            name='Session — Output',
            value=(f'Media sent: **{ses_media}**\n'
                   f'Avg per job: **{avg_items}**\n'
                   f'Msgs scanned: **{ses_msgs}**\n'
                   f'Channels: **{ses_chs}**'),
            inline=True,
        )

        # Row 4 — lifetime
        embed.add_field(
            name='All Time (since start)',
            value=(f'Jobs: **{lt_jobs}** · Media sent: **{lt_media}**\n'
                   f'Messages scanned: **{lt_msgs}** · Unique channels: **{lt_chs}**'),
            inline=False,
        )

        # Row 5 — active job progress bars (up to 4)
        if running_jobs or queued_jobs:
            active = (running_jobs + queued_jobs)[:4]
            lines  = [f'`{j.id}` {_pbar(j.progress, 12)} {j.status}  `{j.source_channel_id}`'
                      for j in active]
            embed.add_field(
                name=f'Active jobs ({len(running_jobs + queued_jobs)})',
                value='\n'.join(lines),
                inline=False,
            )

        await interaction.response.send_message(embed=embed, ephemeral=True)

    # ── Events ───────────────────────────────────────────────────────────────

    # ── DM / channel notification helper ────────────────────────────────────

    async def _send_notification(user_id, channel_id, text):
        try:
            user = await client.fetch_user(user_id)
            await user.send(text)
            return
        except Exception:
            pass
        if channel_id:
            try:
                ch = client.get_channel(channel_id) or await client.fetch_channel(channel_id)
                await ch.send(f'<@{user_id}> {text}')
            except Exception:
                pass

    def _notify(user_id, channel_id, text):
        if not loop.is_closed():
            _aio.run_coroutine_threadsafe(_send_notification(user_id, channel_id, text), loop)

    @client.event
    async def on_ready():
        bot_status['connected'] = True
        bot_status['username'] = str(client.user)
        bot_status['notify'] = _notify
        bot_status['ready_at'] = _discord.utils.utcnow()
        client.add_view(ControlPanelView())
        if cmd_guild_id:
            guild_obj = _discord.Object(id=cmd_guild_id)
            tree.copy_global_to(guild=guild_obj)
            await tree.sync(guild=guild_obj)
            print(f'[Bot] Slash commands synced to guild {cmd_guild_id} (instant)')
        else:
            await tree.sync()
            print('[Bot] Slash commands synced globally (up to 1 h)')
        print(f'[Bot] Ready as {client.user}')

    @client.event
    async def on_disconnect():
        bot_status['connected'] = False

    @client.event
    async def on_message(message: _discord.Message):
        if message.author.bot:
            return

        # ── !scraper / !scrape → drop control panel ──────────────────────────
        if message.content.strip().lower() in ('!scraper', '!scrape'):
            if not channel_ok(message.channel.id):
                return
            try:
                await message.delete()
            except (_discord.Forbidden, _discord.HTTPException):
                pass
            await message.channel.send(embed=make_panel_embed(), view=ControlPanelView())
            return

        # ── !scrape_category → clone category ─────────────────────────────────
        if message.content.strip().lower().startswith('!scrape_category'):
            if not channel_ok(message.channel.id):
                return
            if not message.guild:
                await message.channel.send("⚠️ This command can only be used in a server.")
                return
                
            parts = message.content.strip().split()
            if len(parts) < 2:
                await message.channel.send("⚠️ Usage: `!scrape_category <category_id>`")
                return
                
            category_id = parts[1]
            if not category_id.isdigit() or len(category_id) < 15:
                await message.channel.send("⚠️ Invalid category ID.")
                return
                
            user_id = message.author.id
            if user_id in category_sessions:
                await message.channel.send("⚠️ You already have an active category cloner session. Complete or cancel it first.")
                return
                
            try:
                await message.delete()
            except (_discord.Forbidden, _discord.HTTPException):
                pass
                
            # Send initial defer message
            prompt = await message.channel.send("⏳ Fetching category information...")
            
            cfg = load_config()
            acct_name, acct_token = await find_accessible_account_for_category(cfg, category_id)
            if not acct_token:
                await prompt.edit(content="❌ Error: None of the configured user tokens have access to read this category.")
                return
                
            cat_name, text_channels, err = await get_channels_in_category(acct_token, category_id)
            if err:
                await prompt.edit(content=f"❌ Error: {err}")
                return
                
            if not text_channels:
                await prompt.edit(content=f"❌ Error: Category **{cat_name}** does not contain any text or announcement channels.")
                return
                
            session = CategorySession(
                user_id=user_id,
                discord_channel_id=message.channel.id,
                category_id=category_id,
                category_name=cat_name,
                channels=text_channels,
                account_name=acct_name,
                account_token=acct_token
            )
            category_sessions[user_id] = session
            
            view = CategoryClonerView(session)
            await prompt.edit(content=view._content(), view=view)
            session.prompt_msg = prompt
            return

        # ── relay setup steps ───────────────────────────────────────────────
        user_id = message.author.id
        session = relay_sessions.get(user_id)
        if not session:
            return
        if message.channel.id != session.discord_channel_id:
            return

        text = message.content.strip()

        # Delete the user's typed message to keep the channel tidy
        try:
            await message.delete()
        except (_discord.Forbidden, _discord.HTTPException):
            pass

        if session.step == 'user_filter':
            if text.lower() in ('skip', 'none', '-'):
                session.user_filter = None
            elif text.isdigit():
                session.user_filter = text
            else:
                uf_note = f' (current: `{session.user_filter}`)' if session.user_filter else ''
                await session.prompt_msg.edit(
                    content=(
                        f'📡 **Relay Setup** — User Filter\n'
                        f'Channel `{session.channel_id}` · Webhook: set\n\n'
                        f'⚠️ Not a valid user ID. Type the **user ID**{uf_note} or `skip`:'
                    )
                )
                return
            session.step = 'options'
            view = session.options_view
            await session.prompt_msg.edit(content=view._content(), view=view)

    # ── Run ──────────────────────────────────────────────────────────────────

    try:
        loop.run_until_complete(client.start(bot_token))
    except Exception as exc:
        print(f'[Bot] Fatal: {exc}')
    finally:
        bot_status['connected'] = False


@app.route('/ping')
def ping():
    return 'pong', 200


@app.route('/clear-cache', methods=['POST'])
def clear_cache_route():
    channel_id = request.form.get('channel_id', '').strip()
    duplicates = load_duplicates()
    if channel_id:
        duplicates.pop(channel_id, None)
    else:
        duplicates = {}
    save_duplicates(duplicates)
    active_job_id = request.form.get('active_job_id', '')
    return redirect(url_for('index', active_job_id=active_job_id) if active_job_id else url_for('index'))


@app.route('/remove-job/<job_id>', methods=['POST'])
def remove_job(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
        if job is None:
            abort(404)
        if job.status in ('completed', 'failed', 'cancelled'):
            del jobs[job_id]
    return redirect(url_for('index'))


@app.route('/kill/<job_id>', methods=['POST'])
def kill_job(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
        if job is None:
            abort(404)
        if job.status in ("completed", "failed", "cancelled"):
            return redirect(url_for('index', active_job_id=job.id))
        job.cancel_event.set()
        job.status = "cancelling"
        append_job_log(job, "Cancellation requested.")
    return redirect(url_for('index', active_job_id=job.id))


@app.route('/', methods=['GET', 'POST'])
def index():
    config = load_config()
    errors = []
    success = None
    active_job_id = request.args.get('active_job_id')
    form_data = {
        'source_channel_id': '',
        'webhook_url': '',
        'filter_user_id': '',
        'include_text': False,
        'clear_cache': False,
        'send_text_only': False,
        'relay_all': False,
        'use_original_author': True,
        'split_media': True,
        'include_forwarded': False,
        'account_name': '',
        'media_types': {
            'image': True,
            'gif': True,
            'video': True,
            'sticker': True,
            'other': True,
        },
    }

    if request.method == 'POST':
        form_data['source_channel_id'] = request.form.get('source_channel_id', '').strip()
        form_data['webhook_url'] = request.form.get('webhook_url', '').strip()
        form_data['filter_user_id'] = request.form.get('filter_user_id', '').strip()
        form_data['include_text'] = request.form.get('include_text') == 'on'
        form_data['clear_cache'] = request.form.get('clear_cache') == 'on'
        form_data['send_text_only'] = request.form.get('send_text_only') == 'on'
        form_data['relay_all'] = request.form.get('relay_all') == 'on'
        form_data['use_original_author'] = request.form.get('use_original_author') == 'on'
        form_data['split_media'] = request.form.get('split_media') == 'on'
        form_data['include_forwarded'] = request.form.get('include_forwarded') == 'on'
        form_data['account_name'] = request.form.get('account_name', '').strip()
        for media_type in form_data['media_types']:
            form_data['media_types'][media_type] = request.form.get(f'media_type_{media_type}') == 'on'

        if form_data['relay_all']:
            form_data['include_text'] = True
            form_data['send_text_only'] = True
            form_data['include_forwarded'] = True
            for media_type in form_data['media_types']:
                form_data['media_types'][media_type] = True

        if not form_data['source_channel_id']:
            errors.append('Source Channel ID is required.')
        if not form_data['webhook_url']:
            errors.append('Webhook URL is required.')
        if not any(form_data['media_types'].values()) and not form_data['send_text_only']:
            errors.append('Select at least one media type or enable text-only messages.')

        if not errors:
            job = create_job(form_data, config)
            success = f"Started job {job.id}. Refresh the page to see status updates."

    with jobs_lock:
        active_jobs = sorted(jobs.values(), key=lambda j: j.created_at, reverse=True)
        selected_job = None
        if active_job_id:
            selected_job = jobs.get(active_job_id)
        if selected_job is None and active_jobs:
            selected_job = active_jobs[0]

    if form_data.get('relay_all'):
        selected_media = ['All']
    else:
        selected_media = [name for name, enabled in form_data['media_types'].items() if enabled]

    duplicates = load_duplicates()
    total_cache_entries = sum(len(v) for v in duplicates.values())
    cache_channels = len(duplicates)
    has_active_jobs = any(j.status in ('running', 'queued', 'cancelling') for j in active_jobs)

    return render_template(
        'index.html',
        config=config,
        errors=errors,
        success=success,
        form=form_data,
        jobs=active_jobs,
        selected_job=selected_job,
        selected_media=selected_media,
        total_cache_entries=total_cache_entries,
        cache_channels=cache_channels,
        has_active_jobs=has_active_jobs,
        active_job_id=active_job_id or (selected_job.id if selected_job else ''),
        bot=bot_status,
        accounts=get_accounts(config),
    )


@app.route('/api/status')
def api_status():
    with jobs_lock:
        jobs_list = sorted(jobs.values(), key=lambda j: j.created_at, reverse=True)
    return jsonify({
        'jobs': [{
            'id': j.id,
            'status': j.status,
            'progress': j.progress,
            'phase_label': j.phase_label,
            'messages_scanned': j.messages_scanned,
            'media_sent': j.media_sent,
            'sent_by_type': j.sent_by_type,
            'error': j.error,
            'logs': j.logs[-80:],
        } for j in jobs_list],
        'has_active_jobs': any(j.status in ('running', 'queued', 'cancelling') for j in jobs_list),
    })


if __name__ == '__main__':
    _config = load_config()
    if _config.get('botToken'):
        Thread(target=start_bot, args=(_config,), daemon=True).start()
        print("[Bot] Thread started.")
    else:
        print("[Bot] No botToken in config — bot disabled.")
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
