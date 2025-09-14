import discord
from discord.ext import commands
from presets_loader import load_presets
from env_safety import get_token

ACTIVITIES = load_presets()
TOKEN = get_token("DISCORD_TOKEN")

intents = discord.Intents.default()
intents.message_content = True  # enable if you plan to parse messages elsewhere

bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    print(f"{bot.user} is online. Presets loaded: "
          f"raids={len(ACTIVITIES['raids'])}, "
          f"dungeons={len(ACTIVITIES['dungeons'])}, "
          f"exotics={len(ACTIVITIES['exotic_activities'])}")

# No commands or handlers here. Add cogs/AI elsewhere to use ACTIVITIES.

bot.run(TOKEN)