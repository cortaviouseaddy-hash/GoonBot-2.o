# AGENTS.md

## Cursor Cloud specific instructions

### Overview

GoonBot is a Destiny 2 Discord bot built with Python and `discord.py`. The application lives primarily in `main.py` (~5600 lines) with helpers `env_safety.py` and `presets_loader.py`. There is no database — persistent state is stored as JSON files under `./data/` (auto-created at startup).

### Running the bot

- **Start command:** `python main.py` (see also `Procfile` and `render.yaml`)
- **Required secret:** `DISCORD_TOKEN` — the process exits immediately if unset or invalid (`env_safety.py`)
- Channel and role IDs come from environment variables, with fallbacks in `channel_ids.json`
- Full E2E testing requires a Discord test server with matching channel/role IDs and the bot invited with **Message Content** and **Server Members** intents enabled

### Dependencies

- Install: `pip install -r requirements.txt` (single dependency: `discord.py`)
- Python 3.9+ (uses stdlib `zoneinfo`; VM ships with 3.12)

### Linting and verification

- No linter or test suite is configured in the repo
- Basic checks: `python3 -m py_compile main.py env_safety.py presets_loader.py`
- Optional: `python3 -m pyflakes main.py` (4 pre-existing warnings in `main.py`)
- Smoke test without Discord: import `presets_loader.load_presets()` and `import main` (module load only; do not call `bot.run()` without a token)

### Key gotchas

- `main.py` is very large; use search or partial reads instead of loading the whole file
- The `data/` directory and JSON state files are created lazily on first write
- The bot uses slash commands (`app_commands`), not prefix commands
- Verify live connectivity with `/ping` in Discord once `DISCORD_TOKEN` is set
- There is no Docker, web server, or local HTTP port — the bot is a long-running worker process connecting outbound to Discord
