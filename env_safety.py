import os

def get_token(env_key: str = "DISCORD_TOKEN") -> str:
    token = os.getenv(env_key)
    if not token:
        raise RuntimeError(f"Missing environment variable: {env_key}. Set it on your host (Render/etc).")
    if "." not in token:
        # quick sanity check (Discord tokens include dots)
        raise RuntimeError("DISCORD_TOKEN looks invalid. Double-check you pasted the full token on your host.")
    return token