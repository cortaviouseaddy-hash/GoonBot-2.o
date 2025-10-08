# GoonBot (Deploy-Ready Minimal)

Minimal Discord.py bot wired for Render deployment with themed Destiny activity presets.
No commands included—just connects and loads presets for your cogs/AI to use.

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

### Optional environment variables
- `GENERAL_CHANNEL_ID`: fallback/general chat channel
- `WELCOME_CHANNEL_ID`: where welcome embeds go
- `GENERAL_SHERPA_CHANNEL_ID`: announcements for sherpas
- `LFG_CHAT_CHANNEL_ID`: LFG nudges
- `EVENT_SIGNUP_CHANNEL_ID` (or `RAID_DUNGEON_EVENT_SIGNUP_CHANNEL_ID`): main event posts
- `RAID_SIGN_UP_CHANNEL_ID`: sherpa signup posts
- `UPDATE_CHANNEL_ID`: if set, the bot posts a short "Bot updated and online" embed on startup with commit/branch info when available.

## Discord Portal Settings
- Enable Message Content Intent if you plan to process messages later.
- Reset your token if it was ever leaked.

## Next Steps
- Add a `cogs/` folder and load extensions from main.py if you want modular features.
- Your code can access presets via the global `ACTIVITIES` dict.

## Local channel overrides
You can create a `channel_ids.json` (not secret) to override IDs at deploy time without editing env vars:

```
{
  "GENERAL_SHERPA_CHANNEL_ID": 123,
  "RAID_SIGN_UP_CHANNEL_ID": 456,
  "GENERAL_CHANNEL_ID": 789,
  "LFG_CHAT_CHANNEL_ID": 111,
  "EVENT_SIGNUP_CHANNEL_ID": 222,
  "WELCOME_CHANNEL_ID": 333,
  "UPDATE_CHANNEL_ID": 444
}
```