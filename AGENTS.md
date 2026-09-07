# AGENTS.md

## Cursor Cloud specific instructions

GoonBot is a single-process **discord.py worker bot** (not an HTTP web service, despite the
`Procfile` saying `web:`). It connects to the Discord gateway and manages raid/dungeon
sign-up queues, sherpa signups, scheduling, reminders, and "Build of the Week" posts.

### Services / how to run

- Dependencies are installed into a virtualenv at `.venv` (the update script creates it and
  installs `requirements.txt`, whose only dependency is `discord.py`).
- Run the bot: `.venv/bin/python main.py` (entry point is `main.py`, boot is under
  `if __name__ == "__main__"`). There is no port to bind; it's a gateway worker.
- Required secret: `DISCORD_TOKEN`. `env_safety.get_token()` rejects a missing token **and**
  any token without a `.` in it, so a placeholder must look like `abc.def.ghi` to get past the
  sanity check (it will then fail at Discord login). A real bot token tied to the target guild
  is required for an actual end-to-end run.
- Optional config: channel IDs are read from env vars first, then fall back to
  `channel_ids.json`; activity presets come from `activities.json` (validated by
  `presets_loader.load_presets`). `FOUNDER_USER_ID` / `SHERPA_ASSISTANT_ROLE_ID` are also env-driven.
- Runtime state is persisted to `$GOONBOT_DATA_DIR` / `$BOT_DATA_DIR`, defaulting to `./data`
  (created automatically on startup).

### Lint / test / build

- No test suite and no linter are configured. Use `.venv/bin/python -m py_compile main.py
  presets_loader.py env_safety.py` as a syntax/compile check, and
  `.venv/bin/python -c "import main"` as an import smoke test (it builds the bot and registers
  ~21 slash commands without connecting).

### Notes / gotchas

- The bot needs the **Members** and **Message Content** privileged intents enabled in the
  Discord developer portal for the matching application/token.
- The committed `core` file is a stray ELF core dump and is unrelated to the bot.
