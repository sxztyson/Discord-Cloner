import asyncio
import hashlib
import io
import json
import os
import re
import sys

import aiohttp
import discord

SCAN_CHUNK_BATCHES = 100  # batches per scan chunk (100 × 100 msgs = 10,000 messages)


class JobCancelled(Exception):
    pass

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")
DUPLICATES_FILE = "duplicates.json"


def load_config():
    cfg = {}
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            try:
                cfg = json.load(f)
            except json.JSONDecodeError:
                cfg = {}

    # Replit Secrets / environment variable overrides
    _env_map = {
        "USER_TOKEN":         "userToken",
        "BOT_TOKEN":          "botToken",
        "COMMAND_GUILD_ID":   "commandGuildId",
        "COMMAND_CHANNEL_ID": "commandChannelId",
        "WEBHOOK_NAME":       "webhookName",
        "WEBHOOK_AVATAR_URL": "webhookAvatarUrl",
        "ACCOUNT_PASSCODE":   "accountPasscode",
    }
    for env_key, cfg_key in _env_map.items():
        val = os.environ.get(env_key, "").strip()
        if val:
            cfg[cfg_key] = val

    cfg.setdefault("maxFileSizeMb", int(os.environ.get("MAX_FILE_SIZE_MB", 25)))

    # Multi-account env var: USER_TOKENS="Name1:tok1,Name2:tok2"
    multi = os.environ.get("USER_TOKENS", "").strip()
    if multi:
        parsed = []
        for part in multi.split(","):
            if ":" in part:
                name, tok = part.split(":", 1)
                tok = tok.strip()
                if tok:
                    parsed.append({"name": name.strip(), "token": tok})
        if parsed:
            existing = {a["name"]: a for a in cfg.get("accounts", [])}
            for a in parsed:
                existing[a["name"]] = a
            cfg["accounts"] = list(existing.values())

    # Hash plain-text passcode so the raw value doesn't sit in memory
    if cfg.get("accountPasscode"):
        cfg["accountPasscodeHash"] = hashlib.sha256(cfg.pop("accountPasscode").encode()).hexdigest()

    has_accounts = bool(cfg.get("accounts"))
    if not cfg.get("userToken") and not has_accounts:
        print("WARNING: No user token configured. Add one via the bot's Accounts panel.")

    # Ensure userToken mirrors the first account for backwards compat
    if not cfg.get("userToken") and has_accounts:
        cfg["userToken"] = cfg["accounts"][0]["token"]

    return cfg


def get_accounts(cfg):
    """Return list of {name, token} dicts. Falls back to single Default account."""
    accounts = cfg.get("accounts", [])
    if not accounts:
        tok = cfg.get("userToken", "")
        if tok:
            accounts = [{"name": "Default", "token": tok}]
    return accounts


def token_exists(cfg, token):
    """Return account name if this token is already saved, else None."""
    for acct in cfg.get("accounts", []):
        if acct.get("token") == token:
            return acct["name"]
    if not cfg.get("accounts") and cfg.get("userToken") == token:
        return "Default"
    return None


async def validate_user_token(token):
    """Return (True, display_name) if valid, (False, error_str) if not."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://discord.com/api/v10/users/@me",
                headers={"Authorization": token},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    name = data.get("global_name") or data.get("username", "Unknown")
                    return True, name
                return False, f"Discord rejected it (HTTP {r.status})"
    except Exception as exc:
        return False, f"Network error: {exc}"


def save_config(data):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def load_duplicates():
    if os.path.exists(DUPLICATES_FILE):
        try:
            with open(DUPLICATES_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            return {}
    return {}


def save_duplicates(data):
    with open(DUPLICATES_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def calculate_hash(data):
    return hashlib.sha256(data).hexdigest()


MENTION_USER_RE = re.compile(r"<@!?(?P<id>\d+)>")
MENTION_ROLE_RE = re.compile(r"<@&(?P<id>\d+)>")
MENTION_CHANNEL_RE = re.compile(r"<#(?P<id>\d+)>")


def normalize_message_content(content, message):
    if not content:
        return content

    mention_map = {}
    for mention in message.get("mentions", []) or []:
        if isinstance(mention, dict) and mention.get("id"):
            name = mention.get("global_name") or mention.get("username")
            discriminator = mention.get("discriminator")
            if not name and discriminator:
                name = f"User#{discriminator}"
            if name:
                mention_map[mention["id"]] = name

    role_map = {}
    for role in message.get("role_mentions", []) or []:
        if isinstance(role, dict) and role.get("id"):
            role_map[role["id"]] = role.get("name") or f"role_{role['id']}"

    channel_map = {}
    for channel in message.get("mention_channels", []) or []:
        if isinstance(channel, dict) and channel.get("id"):
            channel_map[channel["id"]] = f"#{channel.get('name', channel['id'])}"

    def user_repl(match):
        mention_id = match.group("id")
        return mention_map.get(mention_id, f"@{mention_id}")

    def role_repl(match):
        role_id = match.group("id")
        return role_map.get(role_id, f"@role_{role_id}")

    def channel_repl(match):
        channel_id = match.group("id")
        return channel_map.get(channel_id, f"#channel_{channel_id}")

    content = MENTION_USER_RE.sub(user_repl, content)
    content = MENTION_ROLE_RE.sub(role_repl, content)
    content = MENTION_CHANNEL_RE.sub(channel_repl, content)
    content = content.replace("@everyone", "everyone").replace("@here", "here")
    return content


def extract_forwarded_text(message):
    forwarded_parts = []
    referenced = message.get('referenced_message') or message.get('message_reference')
    if isinstance(referenced, dict):
        ref_content = normalize_message_content(referenced.get('content', ''), referenced)
        if ref_content:
            author_name, _ = build_author_data(referenced.get('author', {}))
            forwarded_parts.append(f"Forwarded from {author_name}: {ref_content}")

    for snapshot in message.get('message_snapshots', []):
        snapshot_message = snapshot.get('message', {}) if isinstance(snapshot, dict) else {}
        if isinstance(snapshot_message, dict):
            snapshot_text = normalize_message_content(snapshot_message.get('content', ''), snapshot_message)
            if snapshot_text:
                snapshot_author, _ = build_author_data(snapshot_message.get('author', {}))
                forwarded_parts.append(f"Forwarded from {snapshot_author}: {snapshot_text}")

    return '\n\n'.join(forwarded_parts).strip()


def build_author_data(author):
    if not isinstance(author, dict):
        return "Discord User", None

    display_name = author.get("global_name") or author.get("username") or "Discord User"
    avatar_hash = author.get("avatar")
    user_id = author.get("id")
    if avatar_hash and user_id:
        extension = "gif" if avatar_hash.startswith("a_") else "png"
        avatar_url = f"https://cdn.discordapp.com/avatars/{user_id}/{avatar_hash}.{extension}"
    else:
        avatar_url = None

    return display_name, avatar_url


def normalize_media_filename(url, fallback):
    basename = url.split("?")[0].rstrip("/").split("/")[-1]
    return basename or fallback


IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "webp", "bmp", "tiff", "svg"}
VIDEO_EXTENSIONS = {"mp4", "mov", "webm", "avi", "mkv", "flv", "wmv"}
GIF_EXTENSIONS = {"gif"}


def classify_attachment(att):
    content_type = (att.get("content_type") or "").lower()
    if content_type == "image/gif":
        return "gif"
    if content_type.startswith("image"):
        return "image"
    if content_type.startswith("video"):
        return "video"

    filename = (att.get("filename") or "").lower()
    extension = filename.rsplit(".", 1)[-1]
    if extension in GIF_EXTENSIONS:
        return "gif"
    if extension in IMAGE_EXTENSIONS:
        return "image"
    if extension in VIDEO_EXTENSIONS:
        return "video"
    return "other"


def extract_media_entries(message, allowed_types=None):
    entries = []

    def add_attachment(att):
        url = att.get("url") or att.get("proxy_url")
        if not url:
            return
        filename = att.get("filename") or normalize_media_filename(url, "attachment")
        media_type = classify_attachment(att)
        if allowed_types is not None and media_type not in allowed_types:
            return
        if any(entry["url"] == url for entry in entries):
            return
        entries.append({
            "url": url,
            "filename": filename,
            "size": att.get("size", 0),
            "type": media_type,
        })

    for att in message.get("attachments", []):
        add_attachment(att)

    for snap in message.get("message_snapshots", []):
        for att in snap.get("message", {}).get("attachments", []):
            add_attachment(att)

    for embed in message.get("embeds", []):
        for key in ("image", "thumbnail", "video"):
            blob = embed.get(key)
            if blob and isinstance(blob, dict):
                url = blob.get("url") or blob.get("proxy_url")
                if not url:
                    continue
                if key == "video":
                    media_type = "video"
                elif url.split("?")[0].lower().endswith(".gif"):
                    media_type = "gif"
                else:
                    media_type = "image"
                if allowed_types is not None and media_type not in allowed_types:
                    continue
                filename = normalize_media_filename(url, key)
                if any(entry["url"] == url for entry in entries):
                    continue
                entries.append({
                    "url": url,
                    "filename": filename,
                    "size": 0,
                    "type": media_type,
                })

    for sticker in message.get("stickers", []) or message.get("sticker_items", []):
        if isinstance(sticker, dict):
            url = sticker.get("url") or sticker.get("asset") or sticker.get("proxy_url")
            if url:
                media_type = "sticker"
                if allowed_types is not None and media_type not in allowed_types:
                    continue
                filename = normalize_media_filename(url, "sticker")
                if not any(entry["url"] == url for entry in entries):
                    entries.append({"url": url, "filename": filename, "size": 0, "type": media_type})

    return entries


async def fetch_messages(session, channel_id, before=None, limit=100):
    url = f"https://discord.com/api/v10/channels/{channel_id}/messages?limit={limit}"
    if before is not None:
        url += f"&before={before}"

    async with session.get(url) as resp:
        if resp.status == 429:
            data = await resp.json()
            wait = data.get("retry_after", 5)
            await asyncio.sleep(wait)
            return await fetch_messages(session, channel_id, before=before, limit=limit)
        if resp.status == 403:
            raise PermissionError(
                f"Access denied to channel {channel_id} — "
                "it may be age-restricted (NSFW), private, or this account lacks permission."
            )
        if resp.status == 404:
            raise LookupError(f"Channel {channel_id} not found — check the ID.")
        resp.raise_for_status()
        return await resp.json()


async def check_channel_access(token, channel_id):
    """Return True if this token can read the channel."""
    headers = {
        "Authorization": token,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    try:
        async with aiohttp.ClientSession(headers=headers) as s:
            async with s.get(
                f"https://discord.com/api/v10/channels/{channel_id}/messages?limit=1",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                return r.status == 200
    except Exception:
        return False


async def find_accessible_account(cfg, channel_id):
    """Return (account_name, token) of the first account that can read the channel, or (None, None)."""
    for acct in get_accounts(cfg):
        if await check_channel_access(acct["token"], channel_id):
            return acct["name"], acct["token"]
    return None, None


async def find_accessible_account_for_category(cfg, category_id):
    """Return (account_name, token) of the first account that can read/access the category channel, or (None, None)."""
    for acct in get_accounts(cfg):
        headers = {
            "Authorization": acct["token"],
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        try:
            async with aiohttp.ClientSession(headers=headers) as s:
                async with s.get(
                    f"https://discord.com/api/v10/channels/{category_id}",
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    if r.status == 200:
                        return acct["name"], acct["token"]
        except Exception:
            pass
    return None, None


async def get_channels_in_category(user_token, category_id):
    """Fetch the category details and all text/announcement channels under it using user_token.
    
    Returns (category_name, channels_list, error_message).
    """
    headers = {
        "Authorization": user_token,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    
    try:
        async with aiohttp.ClientSession(headers=headers) as s:
            # 1. Fetch category channel details to verify it exists and get guild_id
            async with s.get(
                f"https://discord.com/api/v10/channels/{category_id}",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status != 200:
                    return None, None, f"Category channel not found or inaccessible (HTTP {r.status})."
                cat_data = await r.json()
                
                # Channel type 4 is a category channel
                if cat_data.get("type") != 4:
                    return None, None, f"Channel ID `{category_id}` is not a category channel (type is {cat_data.get('type')})."
                
                cat_name = cat_data.get("name", "Unknown Category")
                guild_id = cat_data.get("guild_id")
                if not guild_id:
                    return None, None, "Category is not part of a server."
            
            # 2. Fetch all channels in the guild to find children of this category
            async with s.get(
                f"https://discord.com/api/v10/guilds/{guild_id}/channels",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status != 200:
                    return None, None, f"Failed to fetch guild channels (HTTP {r.status})."
                guild_channels = await r.json()
    except Exception as exc:
        return None, None, f"Network error: {exc}"
        
    # 3. Filter channels under the category (parent_id matches category_id) and are text/announcement channels
    # Type 0 = GUILD_TEXT, Type 5 = GUILD_ANNOUNCEMENT
    target_channels = []
    for ch in guild_channels:
        if ch.get("parent_id") == str(category_id) and ch.get("type") in (0, 5):
            target_channels.append({
                "id": ch["id"],
                "name": ch["name"],
                "type": ch["type"],
                "nsfw": ch.get("nsfw", False),
                "position": ch.get("position", 0),
            })
            
    # Sort channels by position
    target_channels.sort(key=lambda x: x["position"])
    
    return cat_name, target_channels, None



def prompt_yes_no(prompt, default=False):
    default_label = "Y/n" if default else "y/N"
    while True:
        answer = input(f"{prompt} ({default_label}): ").strip().lower()
        if answer == "" and default is not None:
            return default
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no"):
            return False
        print("Please answer y or n.")


async def relay_channel(
    config,
    source_channel_id,
    webhook_url,
    include_text,
    clear_cache=False,
    allowed_media_types=None,
    send_text_only=False,
    filter_user_id=None,
    use_original_author=True,
    split_media=True,
    include_forwarded=True,
    on_batch=None,
    on_send=None,
    progress_callback=None,
    logger=print,
    cancel_event=None,
):
    user_token = config["userToken"]
    max_file_size_mb = config.get("maxFileSizeMb", 25)

    def push_progress(percent, message=None):
        if progress_callback:
            progress_callback(max(0, min(100, int(percent))), message)

    duplicates = load_duplicates()
    if clear_cache and source_channel_id in duplicates:
        duplicates[source_channel_id] = []
        save_duplicates(duplicates)
    duplicates.setdefault(source_channel_id, [])

    def ensure_not_cancelled():
        if cancel_event is not None and cancel_event.is_set():
            raise JobCancelled("User requested job cancellation.")

    # Pre-flight: verify the selected account can access the channel.
    # If not, auto-try every other account in the pool.
    push_progress(2, "Checking channel access...")
    if not await check_channel_access(user_token, source_channel_id):
        logger(f"⚠️ Selected account cannot access channel {source_channel_id}. Trying other accounts...")
        found_name, found_token = await find_accessible_account(config, source_channel_id)
        if found_token:
            user_token = found_token
            logger(f"✅ Auto-switched to account '{found_name}' which has access.")
            push_progress(3, f"Using account '{found_name}' (auto-detected).")
        else:
            accounts = get_accounts(config)
            names = ", ".join(a["name"] for a in accounts)
            raise PermissionError(
                f"No account has access to channel {source_channel_id}. "
                f"Tried: {names}. "
                f"The channel may be age-restricted, private, or all tokens lack permission."
            )

    headers = {
        "Authorization": user_token,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    push_progress(5, "Starting relay...")
    before_id = None
    total_scanned = 0
    total_sent = 0
    total_batches_scanned = 0
    total_batches_processed = 0
    scan_done = False
    chunk_num = 0
    _last_pct = 5

    def _advance(label):
        nonlocal _last_pct
        pct = 5 + int(90 * total_batches_processed / max(total_batches_scanned, 1))
        pct = max(_last_pct, min(94, pct))
        _last_pct = pct
        push_progress(pct, label)

    connector = aiohttp.TCPConnector(limit=10)
    timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=60)

    async with aiohttp.ClientSession(headers=headers, connector=connector, timeout=timeout) as session, \
               aiohttp.ClientSession(
                   headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"},
                   connector=aiohttp.TCPConnector(limit=10, ssl=False),
                   timeout=aiohttp.ClientTimeout(total=60)
               ) as download_session:
        webhook = discord.Webhook.from_url(webhook_url, session=session)

        while not scan_done:
            chunk_num += 1
            chunk_markers = []

            # ── Scan chunk: collect up to SCAN_CHUNK_BATCHES batch markers ───
            push_progress(_last_pct, f"Chunk {chunk_num} — scanning messages ({total_scanned:,} scanned so far)...")
            logger(f"🔍 Chunk {chunk_num} — scanning up to {SCAN_CHUNK_BATCHES * 100:,} messages...")

            for _ in range(SCAN_CHUNK_BATCHES):
                ensure_not_cancelled()
                for attempt in range(1, 4):
                    try:
                        peek = await fetch_messages(session, source_channel_id, before=before_id, limit=100)
                        break
                    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                        if attempt == 3:
                            raise
                        wait = 2 ** attempt
                        logger(f"⚠️ Network error during scan (attempt {attempt}/3), retrying in {wait}s: {exc}")
                        await asyncio.sleep(wait)

                if not peek:
                    scan_done = True
                    break

                chunk_markers.append(before_id)
                before_id = peek[-1]["id"]
                total_scanned += len(peek)
                total_batches_scanned += 1

                if on_batch:
                    on_batch(len(peek))

                if len(peek) < 100:
                    scan_done = True
                    break

            if not chunk_markers:
                break

            n_chunk = len(chunk_markers)
            logger(f"📂 Chunk {chunk_num} scanned: {n_chunk} batch(es) (~{total_scanned:,} total msgs). Sending oldest → newest...")

            # ── Send chunk: process batches oldest → newest ───────────────────
            for i, marker in enumerate(reversed(chunk_markers), start=1):
                ensure_not_cancelled()
                _advance(f"Chunk {chunk_num} — sending batch {i}/{n_chunk}  (sent: {total_sent})")

                for attempt in range(1, 4):
                    try:
                        batch = await fetch_messages(session, source_channel_id, before=marker, limit=100)
                        break
                    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                        if attempt == 3:
                            logger(f"⚠️ Skipping batch (chunk {chunk_num}, {i}/{n_chunk}) after 3 failed attempts: {exc}")
                            batch = []
                            break
                        wait = 2 ** attempt
                        logger(f"⚠️ Network error (chunk {chunk_num}, batch {i}/{n_chunk}), retrying in {wait}s: {exc}")
                        await asyncio.sleep(wait)

                batch.sort(key=lambda m: int(m["id"]))

                for message in batch:
                    if filter_user_id:
                        author_id = str(message.get("author", {}).get("id", ""))
                        if author_id != str(filter_user_id):
                            continue
                    media_entries = extract_media_entries(message, allowed_types=allowed_media_types)
                    raw_content = normalize_message_content(message.get("content", ""), message) if (include_text or send_text_only) else ""
                    forwarded_text = extract_forwarded_text(message) if (include_forwarded and (include_text or send_text_only)) else ""
                    text_content = "\n\n".join([part for part in [raw_content, forwarded_text] if part]).strip()
                    files_to_send = []
                    author_name, author_avatar = build_author_data(message.get("author", {}))
                    author_override = {}
                    if use_original_author:
                        author_override = {"username": author_name, "avatar_url": author_avatar}

                    for entry in media_entries:
                        ensure_not_cancelled()
                        skip_reason = None
                        if entry["size"] and entry["size"] > max_file_size_mb * 1_000_000:
                            skip_reason = "oversize"
                        else:
                            try:
                                async with download_session.get(entry["url"]) as file_resp:
                                    if file_resp.status != 200:
                                        skip_reason = f"http_{file_resp.status}"
                                    else:
                                        content = await file_resp.read()
                                        if len(content) > max_file_size_mb * 1_000_000:
                                            skip_reason = "oversize_after_download"
                                        else:
                                            file_hash = calculate_hash(content)
                                            if file_hash in duplicates[source_channel_id]:
                                                skip_reason = "duplicate"
                                            else:
                                                duplicates[source_channel_id].append(file_hash)
                                                file_obj = discord.File(io.BytesIO(content), filename=entry["filename"])
                                                files_to_send.append((file_obj, entry["type"]))
                            except Exception as exc:
                                skip_reason = f"download_error_{type(exc).__name__}"
                                logger(f"    - failed to download {entry['filename']} ({entry['url']}): {exc}")

                        if skip_reason is not None:
                            logger(f"    - skipped {entry['filename']} ({entry['url']}) reason={skip_reason}")

                    if files_to_send:
                        webhook_content = text_content if text_content else None
                        sent_count = 0

                        if split_media:
                            first = True
                            for file_obj, ftype in files_to_send:
                                try:
                                    await webhook.send(
                                        content=webhook_content if first else None,
                                        **author_override,
                                        files=[file_obj],
                                    )
                                    sent_count += 1
                                    total_sent += 1
                                    if on_send:
                                        on_send(1, ftype)
                                    logger(f"✅ Sent 1 {ftype} from message {message['id']} ({file_obj.filename}) by {author_name}")
                                    await asyncio.sleep(1.5)
                                except Exception as exc:
                                    err_text = str(exc)
                                    if "413" in err_text or "Payload Too Large" in err_text or "Request entity too large" in err_text or "40005" in err_text:
                                        logger(f"    - skipped {file_obj.filename} reason=payload_too_large")
                                    else:
                                        logger(f"    - failed to send {file_obj.filename}: {err_text}")
                                finally:
                                    first = False
                        else:
                            CHUNK = 10
                            first_chunk = True
                            for ci in range(0, len(files_to_send), CHUNK):
                                chunk_items = files_to_send[ci:ci + CHUNK]
                                chunk_files = [f for f, _ in chunk_items]
                                try:
                                    await webhook.send(
                                        content=webhook_content if first_chunk else None,
                                        **author_override,
                                        files=chunk_files,
                                    )
                                    sent_count += len(chunk_items)
                                    total_sent += len(chunk_items)
                                    for _, ftype in chunk_items:
                                        if on_send:
                                            on_send(1, ftype)
                                    names = ", ".join(f.filename for f in chunk_files)
                                    logger(f"✅ Sent {len(chunk_items)} file(s) from message {message['id']} ({names}) by {author_name}")
                                    await asyncio.sleep(1.5)
                                except Exception as exc:
                                    err_text = str(exc)
                                    if "413" in err_text or "Payload Too Large" in err_text or "Request entity too large" in err_text or "40005" in err_text:
                                        logger(f"    - skipped chunk of {len(chunk_items)} file(s) reason=payload_too_large")
                                    else:
                                        logger(f"    - failed to send chunk: {err_text}")
                                finally:
                                    first_chunk = False

                        if sent_count == 0:
                            logger(f"⚠️ Found {len(media_entries)} media entries in message {message['id']} by {author_name}, but none were sent.")
                    elif send_text_only and text_content:
                        try:
                            await webhook.send(
                                content=text_content,
                                **author_override,
                            )
                            total_sent += 1
                            if on_send:
                                on_send(1, "text")
                            logger(f"✅ Sent text-only message from {message['id']} by {author_name}")
                        except Exception as exc:
                            logger(f"    - failed to send text message from {message['id']}: {exc}")
                    else:
                        logger(f"⚠️ Skipping message {message['id']} by {author_name}; no media or text to send.")

                total_batches_processed += 1

            save_duplicates(duplicates)
            logger(f"✅ Chunk {chunk_num} complete — {total_sent} items sent so far.")

    logger(f"\n🏁 Relay complete. Total items sent: {total_sent}.")
    push_progress(100, "Relay complete.")
    return total_sent


def run_relay(
    source_channel_id,
    webhook_url,
    include_text=False,
    clear_cache=False,
    media_types=None,
    send_text_only=False,
    filter_user_id=None,
    use_original_author=True,
    split_media=True,
    include_forwarded=True,
    on_batch=None,
    on_send=None,
    config=None,
    logger=print,
    progress_callback=None,
    cancel_event=None,
):
    if config is None:
        config = load_config()

    if "userToken" not in config:
        raise ValueError("Please add userToken to config.json.")

    return asyncio.run(
        relay_channel(
            config,
            source_channel_id,
            webhook_url,
            include_text,
            clear_cache=clear_cache,
            allowed_media_types=media_types,
            send_text_only=send_text_only,
            filter_user_id=filter_user_id,
            use_original_author=use_original_author,
            split_media=split_media,
            include_forwarded=include_forwarded,
            on_batch=on_batch,
            on_send=on_send,
            logger=logger,
            progress_callback=progress_callback,
            cancel_event=cancel_event,
        )
    )


def main():
    config = load_config()

    if "userToken" not in config:
        print("Please add userToken to config.json.")
        sys.exit(1)

    print("\n🚀 Discord Media Scraper Initialized")
    while True:
        source_channel_id = input("\n📂 Enter source channel ID: ").strip()
        webhook_url = input("🔗 Enter destination webhook URL: ").strip()

        if not source_channel_id or not webhook_url:
            print("❌ Channel ID and webhook URL are required.")
            continue

        include_text = prompt_yes_no("Include message text content with media?", default=False)

        duplicates = load_duplicates()
        clear_cache = False
        if source_channel_id in duplicates and duplicates[source_channel_id]:
            clear_cache = prompt_yes_no(
                "Existing duplicate cache found for this channel. Clear cache and resend media?",
                default=False
            )

        try:
            asyncio.run(relay_channel(config, source_channel_id, webhook_url, include_text, clear_cache=clear_cache))
        except Exception as exc:
            print(f"❌ Relay failed: {exc}")

        again = prompt_yes_no("Do you want to process another channel?", default=False)
        if not again:
            break


if __name__ == '__main__':
    main()
