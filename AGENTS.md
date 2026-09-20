# AGENTS.md

## Cursor Cloud specific instructions

### Overview

GoonBot is a Destiny 2 Discord bot built with Python and `discord.py`. It is a single-file application (`main.py`, ~5100 lines) with two helper modules (`env_safety.py`, `presets_loader.py`). There is no database — all persistent state is stored as flat JSON files in `./data/`.

### Running the bot

- **Start command:** `python main.py`
- **Required secret:** `DISCORD_TOKEN` environment variable (a valid Discord bot token). The bot will crash immediately without it.
- The bot reads channel/role IDs from environment variables and falls back to `channel_ids.json`.

### Dependencies

- Single pip dependency: `discord.py` (see `requirements.txt`)
- Python 3.9+ required (uses `zoneinfo` from stdlib)
- Install: `pip install -r requirements.txt`

### Linting

- No linter is configured in the repo. Use `python3 -m pyflakes *.py` for basic checks.
- There are 4 pre-existing pyflakes warnings in `main.py` (unused variables, f-string issue, undefined `msg_id`).

### Testing

- No automated test suite exists. Verify changes via `python3 -m py_compile main.py` and full module import test.
- For end-to-end testing, a valid `DISCORD_TOKEN` and a Discord test server are required.

### Key gotchas

- `main.py` is very large (~5100 lines, ~218KB). Reading the full file may exceed tool limits; use offset/limit or search.
- The `data/` directory is auto-created at module load time. JSON state files are created lazily on first write.
- The bot uses `discord.py` slash commands (`app_commands`), not legacy prefix commands.
