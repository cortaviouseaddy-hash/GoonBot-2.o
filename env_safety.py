import os
from typing import Optional


def load_dotenv_file(path: Optional[str] = None) -> None:
    """Load KEY=VALUE pairs from a local .env without overriding real env vars."""
    env_path = path or os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.isfile(env_path):
        return
    try:
        with open(env_path, "r", encoding="utf-8") as handle:
            for raw in handle:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[7:].strip()
                if "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip()
                if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
                    val = val[1:-1]
                if key and key not in os.environ:
                    os.environ[key] = val
    except Exception:
        pass


def get_token(env_key: str = "DISCORD_TOKEN") -> str:
    token = os.getenv(env_key)
    if not token:
        raise RuntimeError(f"Missing environment variable: {env_key}. Set it on your host (Render/etc).")
    if "." not in token:
        # quick sanity check (Discord tokens include dots)
        raise RuntimeError("DISCORD_TOKEN looks invalid. Double-check you pasted the full token on your host.")
    return token
