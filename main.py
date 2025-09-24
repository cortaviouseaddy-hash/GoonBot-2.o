
# GoonBot main file with raid image support

import discord
from discord.ext import commands
from discord import app_commands
import os

from presets_loader import load_presets
from env_safety import get_token

ACTIVITIES = load_presets()
TOKEN = get_token("DISCORD_TOKEN")

# Activity images mapping
ACTIVITY_IMAGES = {
    "Crota’s End": "assets/raids/crotas_end.jpg",
    "Deep Stone Crypt": "assets/raids/deep_stone_crypt.jpg",
    "Garden of Salvation": "assets/raids/garden_of_salvation.jpg",
    "King’s Fall": "assets/raids/kings_fall.jpg",
    "Last Wish": "assets/raids/last_wish.jpg",
    "Root of Nightmares": "assets/raids/root_of_nightmares.jpg",
    "Salvation’s Edge": "assets/raids/salvations_edge.jpg",
    "Vault of Glass": "assets/raids/vault_of_glass.jpg",
    "Vow of the Disciple": "assets/raids/vow_of_the_disciple.jpg"
}

# Example snippet showing how images are applied:
def _apply_activity_image(embed: discord.Embed, activity: str):
    p = ACTIVITY_IMAGES.get(activity)
    if not p:
        return embed, None
    if os.path.isfile(p):
        fn = os.path.basename(p)
        f = discord.File(p, filename=fn)
        embed.set_image(url=f"attachment://{fn}")
        return embed, f
    return embed, None

# (Rest of your bot logic remains unchanged)
