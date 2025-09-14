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

## Discord Portal Settings
- Enable Message Content Intent if you plan to process messages later.
- Reset your token if it was ever leaked.

## Next Steps
- Add a `cogs/` folder and load extensions from main.py if you want modular features.
- Your code can access presets via the global `ACTIVITIES` dict.