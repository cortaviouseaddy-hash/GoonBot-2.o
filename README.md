# GoonBot (Deploy-Ready Minimal)

Minimal Discord.py bot wired for Render deployment with themed Destiny activity presets.
No commands included—just connects and loads presets for your cogs/AI to use.

## Live stream notifications (Twitch + TikTok)
This bot can post a Discord message (optionally pinging **@everyone**) when you go live.

### Twitch (recommended / reliable)
Set these environment variables:
- `TWITCH_USER_LOGIN` (example: `mychannel`)
- `TWITCH_CLIENT_ID`
- `TWITCH_CLIENT_SECRET`

### TikTok (best-effort)
TikTok does not provide a stable public “is live” API. This bot uses **best-effort page scraping**, which may break or miss lives sometimes.

Set:
- `TIKTOK_USERNAME` (example: `mytiktokname`, with or without `@`)

### Where it posts + @everyone ping
- `STREAM_ANNOUNCE_CHANNEL_ID`: channel id to post in (defaults to `GENERAL_CHANNEL_ID` if unset)
- `STREAM_ANNOUNCE_EVERYONE`: `true/false` (default `true`)
- `STREAM_POLL_SECONDS`: how often to check (default `60`, minimum `15`)
- `STREAM_ANNOUNCE_COOLDOWN_SECONDS`: minimum seconds between pings per platform (default `600`)
- `ENABLE_TWITCH_NOTIFY`: `true/false` (default `true`)
- `ENABLE_TIKTOK_NOTIFY`: `true/false` (default `true`)

Note: Discord can still block `@everyone` if the bot/channel doesn’t have permission to mention everyone.

## Files
- main.py — bot entry (no commands)
- activities.json — raids/dungeons/exotics with emojis
- presets_loader.py — loads/validates presets
- env_safety.py — reads DISCORD_TOKEN from env
- requirements.txt — dependencies
- Procfile — start command for Render
- .gitignore — ignores local junk
- .env.example — template for local dev (do not commit real secrets)

## Deploy on Render
1) Push this folder to a new GitHub repo.
2) On Render: New → Web Service → connect the repo.
3) Environment → add secret: DISCORD_TOKEN = <your new token>
4) Deploy.

## Discord Portal Settings
- Enable Message Content Intent if you plan to process messages later.
- Reset your token if it was ever leaked.

## Next Steps
- Add a `cogs/` folder and load extensions from main.py if you want modular features.
- Your code can access presets via the global `ACTIVITIES` dict.