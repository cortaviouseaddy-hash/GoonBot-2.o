import discord
from discord.ext import commands
from presets_loader import load_presets
from env_safety import get_token
from discord import app_commands
import os

ACTIVITIES = load_presets()
TOKEN = get_token("DISCORD_TOKEN")
GUILD_ID = os.getenv("GUILD_ID")  # optional: set for fast guild-specific sync

intents = discord.Intents.default()
intents.message_content = True  # enable if you plan to parse messages elsewhere

bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    # Sync app commands (slash commands)
    try:
        if GUILD_ID:
            guild = discord.Object(id=int(GUILD_ID))
            synced = await bot.tree.sync(guild=guild)
            print(f"Synced {len(synced)} commands to guild {GUILD_ID}")
        else:
            synced = await bot.tree.sync()
            print(f"Globally synced {len(synced)} commands")
    except Exception as e:
        print(f"Slash command sync failed: {e}")

    print(f"{bot.user} is online. Presets loaded: "
          f"raids={len(ACTIVITIES['raids'])}, "
          f"dungeons={len(ACTIVITIES['dungeons'])}, "
          f"exotics={len(ACTIVITIES['exotic_activities'])}")

# Example slash command
@bot.tree.command(name="ping", description="Check bot latency")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message(f"Pong! Latency: {round(bot.latency * 1000)} ms")

# No commands or handlers here. Add cogs/AI elsewhere to use ACTIVITIES.

bot.run(TOKEN)