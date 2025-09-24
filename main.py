import discord
from discord.ext import commands
from presets_loader import load_presets
from env_safety import get_token
from discord import app_commands
import os
import random
from typing import List

ACTIVITIES = load_presets()
TOKEN = get_token("DISCORD_TOKEN")
GUILD_ID = os.getenv("GUILD_ID")  # optional: set for fast guild-specific sync

# Accept multiple possible env var names so you don't have to rename your existing keys on Render
def _get_first_env(*keys: str) -> str | None:
    for k in keys:
        v = os.getenv(k)
        if v:
            return v
    return None

# Helper: is member a Sherpa Assistant?
def _is_sherpa(member: discord.Member) -> bool:
    role = _get_sherpa_role(member.guild)
    return role is not None and role in member.roles

# Destiny Community -> general
GENERAL_CHANNEL_ID = _get_first_env("GENERAL_CHANNEL_ID", "GENERAL")
# Sherpa Assistant -> general-sherpa
GENERAL_SHERPA_CHANNEL_ID = _get_first_env("GENERAL_SHERPA_CHANNEL_ID", "GENERAL_SHERPA")
RAID_QUEUE_CHANNEL_ID = _get_first_env("RAID_QUEUE_CHANNEL_ID", "RAID_QUEUE")

# Founder/user restriction: allow locking certain commands to a single user or role
FOUNDER_USER_ID = os.getenv("FOUNDER_USER_ID")  # optional: numeric user ID of the founder

def promoter_only():
    async def predicate(interaction: discord.Interaction) -> bool:
        if interaction.guild is None:
            raise app_commands.CheckFailure("This command can only be used in a server.")
        try:
            if FOUNDER_USER_ID and interaction.user.id == int(FOUNDER_USER_ID):
                return True
        except ValueError:
            pass
        if isinstance(interaction.user, discord.Member):
            if any(r.name.lower() == "founder" for r in interaction.user.roles):
                return True
        raise app_commands.CheckFailure("You are not authorized to use this command.")
    return app_commands.check(predicate)

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    try:
        gsynced = await bot.tree.sync()
        print(f"Globally synced {len(gsynced)} commands")
        if GUILD_ID:
            guild = discord.Object(id=int(GUILD_ID))
            gsynced = await bot.tree.sync(guild=guild)
            print(f"Synced {len(gsynced)} commands to guild {GUILD_ID}")
    except Exception as e:
        print(f"Slash command sync failed: {e}")

    print(f"{bot.user} is online. Presets loaded: "
          f"raids={len(ACTIVITIES['raids'])}, "
          f"dungeons={len(ACTIVITIES['dungeons'])}, "
          f"exotics={len(ACTIVITIES['exotic_activities'])}")

@bot.tree.command(name="ping", description="Check bot latency")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message(f"Pong! Latency: {round(bot.latency * 1000)} ms")

# -----------------------------
# Queues: /join, /queue, /remove_from_queue
# -----------------------------

ALL_ACTIVITIES: list[str] = []
CAP_BY_CATEGORY: dict[str, int] = {
    "raids": 6,
    "dungeons": 3,
    "exotic_activities": 3,
}
for cat, items in ACTIVITIES.items():
    if isinstance(items, list):
        ALL_ACTIVITIES.extend(items)

QUEUES: dict[str, list[int]] = {}

def _category_of_activity(name: str) -> str | None:
    for cat, items in ACTIVITIES.items():
        if name in items:
            return cat
    return None

def _cap_for_activity(name: str) -> int:
    cat = _category_of_activity(name)
    return CAP_BY_CATEGORY.get(cat or "", 6)

def _user_current_activities(user_id: int) -> list[str]:
    res: list[str] = []
    for act, lst in QUEUES.items():
        if user_id in lst:
            res.append(act)
    return res

def _ensure_queue(name: str) -> list[int]:
    return QUEUES.setdefault(name, [])

def _activity_choices(prefix: str) -> list[app_commands.Choice[str]]:
    pref = (prefix or "").lower()
    filtered = [a for a in ALL_ACTIVITIES if pref in a.lower()][:25]
    return [app_commands.Choice(name=a, value=a) for a in filtered]

async def _post_queue_board() -> None:
    if not RAID_QUEUE_CHANNEL_ID:
        return
    cap_cache: dict[str, int] = {}
    embed = discord.Embed(title="Activity Queues", color=discord.Color.blurple())
    if not QUEUES:
        embed.description = "No sign-ups yet. Use /join to get started!"
    else:
        for act in ALL_ACTIVITIES:
            if act not in QUEUES:
                continue
            cap = cap_cache.setdefault(act, _cap_for_activity(act))
            main_ids = QUEUES[act][:cap]
            if not main_ids:
                continue
            mentions = [f"<@{uid}>" for uid in main_ids]
            embed.add_field(name=act, value="\n".join(mentions), inline=False)
    await _send_to_channel_id(RAID_QUEUE_CHANNEL_ID, None, embed=embed)

@bot.tree.command(name="join", description="Join an activity queue (raids, dungeons, exotic missions)")
@app_commands.describe(activity="Choose an activity to join")
@app_commands.autocomplete(activity=lambda interaction, current: _activity_choices(current))
async def join_cmd(interaction: discord.Interaction, activity: str):
    if isinstance(interaction.user, discord.Member) and _is_sherpa(interaction.user):
        await interaction.response.send_message("Sherpa Assistants cannot join queues.", ephemeral=True)
        return
    if activity not in ALL_ACTIVITIES:
        await interaction.response.send_message("Unknown activity. Please choose from suggestions.", ephemeral=True)
        return
    uid = interaction.user.id
    joined = _user_current_activities(uid)
    if activity in joined:
        await interaction.response.send_message("You are already signed up for this activity.", ephemeral=True)
        return
    if len(joined) >= 2:
        await interaction.response.send_message("You can only be in 2 different activity queues at once.", ephemeral=True)
        return
    q = _ensure_queue(activity)
    q.append(uid)
    await interaction.response.send_message(f"Joined queue for: {activity}", ephemeral=True)
    await _post_queue_board()

@bot.tree.command(name="queue", description="Show the current queues for all activities")
async def queue_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await _post_queue_board()
    await interaction.followup.send("Queue board updated.", ephemeral=True)

# -------- remove_from_queue (renamed /kick) --------

def _parse_user_ids(text: str, guild: discord.Guild) -> List[int]:
    if not text:
        return []
    parts = [p.strip() for p in text.replace(",", " ").split() if p.strip()]
    ids: List[int] = []
    for p in parts:
        if p.startswith("<@") and p.endswith(">"):
            p = p.strip("<@!>")
        try:
            mid = int(p)
            if guild.get_member(mid):
                ids.append(mid)
            continue
        except ValueError:
            pass
        lower = p.lower()
        m = discord.utils.find(lambda u: lower in u.display_name.lower() or lower in u.name.lower(), guild.members)
        if m:
            ids.append(m.id)
    seen = set()
    out: List[int] = []
    for i in ids:
        if i not in seen:
            seen.add(i)
            out.append(i)
    return out

@bot.tree.command(name="remove_from_queue", description="Remove one or more users from an activity queue (founder only)")
@promoter_only()
@app_commands.describe(
    users="Mentions, IDs, or names separated by spaces/commas (e.g., @A @B 1234567890)",
    activity="Activity queue to remove them from"
)
async def remove_from_queue_cmd(interaction: discord.Interaction, users: str, activity: str):
    if activity not in ALL_ACTIVITIES:
        await interaction.response.send_message("Unknown activity.", ephemeral=True)
        return
    guild = interaction.guild
    if not guild:
        await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
        return
    targets = _parse_user_ids(users, guild)
    if not targets:
        await interaction.response.send_message("No valid users found. Use mentions, IDs, or names.", ephemeral=True)
        return
    q = QUEUES.get(activity)
    if not q:
        await interaction.response.send_message(f"No queue exists yet for **{activity}**.", ephemeral=True)
        return
    removed_names: List[str] = []
    for uid in targets:
        if uid in q:
            member = guild.get_member(uid)
            removed_names.append(member.display_name if member else str(uid))
            q[:] = [x for x in q if x != uid]
    if not q:
        QUEUES.pop(activity, None)
    if not removed_names:
        await interaction.response.send_message(f"No selected users were in the **{activity}** queue.", ephemeral=True)
        return
    await interaction.response.send_message(
        f"Removed from **{activity}**: {', '.join(removed_names)}",
        ephemeral=True
    )
    await _post_queue_board()

# -------- promote command --------

def _get_sherpa_role(guild: discord.Guild) -> discord.Role | None:
    role_id_env = os.getenv("SHERPA_ROLE_ID")
    if role_id_env:
        try:
            r = guild.get_role(int(role_id_env))
            if r:
                return r
        except ValueError:
            pass
    for r in guild.roles:
        if r.name.lower() == "sherpa assistant":
            return r
    return None

@bot.tree.command(name="promote", description="Promote a member to Sherpa Assistant and announce it with flair")
@promoter_only()
@app_commands.describe(user="Member to promote")
async def promote(interaction: discord.Interaction, user: discord.Member):
    await interaction.response.defer(ephemeral=True)
    if interaction.guild is None:
        await interaction.followup.send("This command can only be used in a server.", ephemeral=True)
        return

    guild = interaction.guild
    sherpa_role = _get_sherpa_role(guild)
    if not sherpa_role:
        await interaction.followup.send("Could not find the 'Sherpa Assistant' role.", ephemeral=True)
        return

    assigned = False
    if sherpa_role not in user.roles:
        try:
            await user.add_roles(sherpa_role, reason=f"Promoted by {interaction.user} via /promote")
            assigned = True
        except discord.Forbidden:
            await interaction.followup.send("I don't have permission to manage roles or my role is below 'Sherpa Assistant'.", ephemeral=True)
            return
        except Exception as e:
            await interaction.followup.send(f"Failed to assign role: {e}", ephemeral=True)
            return

    description_lines = [
        f"🎉 **Congratulations, {user.mention}!** 🏆",
        "",
        "You’ve risen above and become a **Sherpa Assistant**. This isn’t just a new role — it’s a mark of trust and respect.",
        "",
        "✨ **What it Means to be a Sherpa**",
        "Sherpas bring **patience, clarity, and positive vibes** to every fireteam.",
        "They’re the **torchbearers of the fireteam**, guiding others through chaos and turning doubt into understanding.",
        "They help newer and returning players **learn mechanics and win together**.",
        "A Sherpa doesn’t just guide — they inspire Guardians to rise higher.",
        "",
        "⚔️ **Expectations**",
        "• Be the calm voice when the fireteam feels the pressure.",
        "• Explain mechanics clearly so **anyone** can master them.",
        "• Turn every wipe into a lesson and every lesson into victory.",
        "• Keep every run welcoming, fun, and unforgettable.",
        "",
        "🌌 **Carry the Light**",
        "Every Guardian you guide becomes part of your story.",
        "Lead with patience, lift others up, and show the community what it truly means to **Carry the Light**.",
    ]

    palette = [discord.Color.blurple(), discord.Color.green(), discord.Color.gold(), discord.Color.purple(), discord.Color.orange()]
    embed = discord.Embed(
        title="Sherpa Promotion 🌟",
        description="\n".join(description_lines),
        color=random.choice(palette),
    )
    embed.set_author(name=user.display_name, icon_url=user.display_avatar.url if user.display_avatar else discord.Embed.Empty)
    embed.set_thumbnail(url=user.display_avatar.url if user.display_avatar else discord.Embed.Empty)
    embed.set_footer(text=f"Promoted by {interaction.user.display_name} • GG!")

    targets = [GENERAL_CHANNEL_ID, GENERAL_SHERPA_CHANNEL_ID]
    success_count = 0
    for tid in targets:
        ok = await _send_to_channel_id(tid, None, embed=embed)
        if ok:
            success_count += 1

    ack = f"Role {'assigned' if assigned else 'already present'}; announcement sent in {success_count}/{len(targets)} channels."
    await interaction.followup.send(ack, ephemeral=True)

# -------- helpers --------

async def _send_to_channel_id(channel_id: str | None, content: str | None = None, *, embed: discord.Embed | None = None) -> bool:
    if not channel_id:
        return False
    try:
        cid = int(channel_id)
        channel = bot.get_channel(cid)
        if channel is None:
            channel = await bot.fetch_channel(cid)
        kwargs = {}
        if content is not None:
            kwargs["content"] = content
        if embed is not None:
            kwargs["embed"] = embed
        await channel.send(**kwargs)
        return True
    except Exception as e:
        print(f"Failed to post to channel {channel_id}: {e}")
        return False

bot.run(TOKEN)