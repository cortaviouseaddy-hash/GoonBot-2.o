import discord
from discord.ext import commands
from presets_loader import load_presets
from env_safety import get_token
from discord import app_commands
import os

ACTIVITIES = load_presets()
TOKEN = get_token("DISCORD_TOKEN")
GUILD_ID = os.getenv("GUILD_ID")  # optional: set for fast guild-specific sync
GENERAL_CHANNEL_ID = os.getenv("GENERAL_CHANNEL_ID")  # Destiny Community -> general
GENERAL_SHERPA_CHANNEL_ID = os.getenv("GENERAL_SHERPA_CHANNEL_ID")  # Sherpa Assistant -> general-sherpa

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

# Helper to send a message to a channel by ID, fetching if needed
async def _send_to_channel_id(channel_id: str | None, content: str) -> bool:
    if not channel_id:
        return False
    try:
        cid = int(channel_id)
        channel = bot.get_channel(cid)
        if channel is None:
            channel = await bot.fetch_channel(cid)
        await channel.send(content)
        return True
    except Exception as e:
        print(f"Failed to post to channel {channel_id}: {e}")
        return False

# Promote command: always posts to the two fixed channels regardless of where it's invoked
@bot.tree.command(name="promote", description="Post a promotion to the designated general channels")
@app_commands.describe(message="The announcement/promotion text to post")
async def promote(interaction: discord.Interaction, message: str):
    await interaction.response.defer(ephemeral=True)

    targets = [GENERAL_CHANNEL_ID, GENERAL_SHERPA_CHANNEL_ID]
    success_count = 0
    for tid in targets:
        ok = await _send_to_channel_id(tid, message)
        if ok:
            success_count += 1

    if success_count == 0:
        await interaction.followup.send(
            "Could not post to any target channels. Please ensure GENERAL_CHANNEL_ID and GENERAL_SHERPA_CHANNEL_ID are set in the environment and that I have permission to send messages.",
            ephemeral=True,
        )
    else:
        await interaction.followup.send(f"Posted to {success_count}/{len(targets)} target channels.", ephemeral=True)

# No commands or handlers here. Add cogs/AI elsewhere to use ACTIVITIES.

bot.run(TOKEN)