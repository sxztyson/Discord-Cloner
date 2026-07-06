# Discord Media Relay

Scrape and relay media from any Discord channel to a webhook. Comes with a web dashboard and a full Discord bot control panel.

## Features

- Relay images, GIFs, videos, stickers, other files, text messages, and forwarded content
- Multi-account support — add multiple user tokens, auto-detects which account has access to a channel
- Duplicate detection with SHA256 hashing (persists across restarts)
- Live progress bar per job
- Job queue — run multiple jobs simultaneously on different accounts
- Discord bot control panel (`!scraper` or `/scraper`) with step-by-step relay flow
- Web UI at `http://localhost:5000`
- Webhook posts using original sender name & avatar, or the webhook's server default
- Completion notifications via DM or channel mention
- Passcode-protected account removal

---

## Quick Start (Windows)

Double-click **`start.bat`** — it installs dependencies and launches the app.
*(Note: If Python is not installed on your system, `start.bat` will automatically attempt to install Python 3.12 via `winget` or direct installer download, then guide you to restart the script.)*

Then open `http://localhost:5000` in your browser.


---

## Manual Setup

**Requirements:** Python 3.10+

```bash
pip install -r requirements.txt
python web.py
```

---

## Configuration

Copy `config.example.json` to `config.json` and fill in your values:

```json
{
  "botToken": "YOUR_BOT_TOKEN_HERE",
  "accountPasscode": "YOUR_REMOVAL_PASSCODE_HERE",
  "maxFileSizeMb": 25,
  "commandGuildId": "",
  "commandChannelId": ""
}
```

| Field | Required | Description |
|---|---|---|
| `botToken` | Yes | Discord bot token |
| `accountPasscode` | Recommended | Passcode to protect account removal via bot |
| `maxFileSizeMb` | No | Max file size to relay (default: 25) |
| `commandGuildId` | No | Server ID for instant slash command sync |
| `commandChannelId` | No | Restrict bot commands to one channel |

User tokens are added through the bot's **👥 Accounts** panel — never stored in plain text here.

---

## Replit Deployment (24/7)

1. Upload all files to a Replit Python repl
2. Add these Replit Secrets:

| Key | Value |
|---|---|
| `BOT_TOKEN` | Your bot token |
| `ACCOUNT_PASSCODE` | Your removal passcode |
| `COMMAND_GUILD_ID` | Your server ID (optional) |

3. Click **Run**
4. Set up [UptimeRobot](https://uptimerobot.com) to ping `https://your-repl.replit.app/ping` every 5 minutes

---

## Bot Commands

| Command | Description |
|---|---|
| `!scraper` | Drop the control panel in chat |
| `/scraper` | Same via slash command |
| `/help` | Usage guide |
| `/info` | Live stats — jobs, media sent, uptime, accounts |

### Control Panel Buttons

| Button | Description |
|---|---|
| ▶ Start Relay | Step-by-step relay setup in chat |
| 📋 Jobs | View, kill, or remove jobs |
| 📜 Logs | Last 20 log lines from most recent job |
| 💾 Cache | View and clear duplicate hashes |
| 👥 Accounts | Add/update/remove user tokens |
| ⚙️ Settings | Change webhook display name |

---

## Discord Bot Setup

1. Go to [discord.com/developers/applications](https://discord.com/developers/applications)
2. Create a new application → Bot → copy the token
3. Enable **Message Content Intent** under Privileged Gateway Intents
4. Invite the bot with scopes: `bot`, `applications.commands` and permissions: `Send Messages`, `Read Message History`, `Manage Messages`

---

## JustRunMyApp Deployment (24/7)

1. Create an account at [justrunmyapp.com](https://justrunmyapp.com)
2. Create a new app and upload all project files (or connect your GitHub repo)
3. Set the start command to:
   ```
   python web.py
   ```
4. Add these environment variables in the app settings:

| Key | Value |
|---|---|
| `BOT_TOKEN` | Your bot token |
| `ACCOUNT_PASSCODE` | Your removal passcode |
| `COMMAND_GUILD_ID` | Your server ID (optional) |
| `PORT` | `5000` |

5. Click **Deploy** — the web UI will be available at your app's public URL
