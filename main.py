
# GoonBot main file (robust) — queues, promotions, events + image support with safe fallbacks
import os
import random
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from presets_loader import load_presets
from env_safety import get_token

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

ACTIVITIES = load_presets()
TOKEN = get_token("DISCORD_TOKEN")
GUILD_ID = os.getenv("GUILD_ID")

def _get_first_env(*keys: str) -> Optional[str]:
    for k in keys:
        v = os.getenv(k)
        if v:
            return v
    return None

GENERAL_CHANNEL_ID = _get_first_env("GENERAL_CHANNEL_ID", "GENERAL")
GENERAL_SHERPA_CHANNEL_ID = _get_first_env("GENERAL_SHERPA_CHANNEL_ID", "GENERAL_SHERPA")
RAID_QUEUE_CHANNEL_ID = _get_first_env("RAID_QUEUE_CHANNEL_ID", "RAID_QUEUE")
LFG_CHAT_CHANNEL_ID = _get_first_env("LFG_CHAT_CHANNEL_ID", "LFG_CHAT")

FOUNDER_USER_ID = os.getenv("FOUNDER_USER_ID")
SHERPA_ROLE_ID = os.getenv("SHERPA_ROLE_ID")

# Map activity to local image path. If files are missing, the bot will just skip images.
ACTIVITY_IMAGES: Dict[str, str] = {
    "Crota’s End": "assets/raids/crotas_end.jpg",
    "Deep Stone Crypt": "assets/raids/deep_stone_crypt.jpg",
    "Garden of Salvation": "assets/raids/garden_of_salvation.jpg",
    "King’s Fall": "assets/raids/kings_fall.jpg",
    "Last Wish": "assets/raids/last_wish.jpg",
    "Root of Nightmares": "assets/raids/root_of_nightmares.jpg",
    "Salvation’s Edge": "assets/raids/salvations_edge.jpg",
    "Vault of Glass": "assets/raids/vault_of_glass.jpg",
    "Vow of the Disciple": "assets/raids/vow_of_the_disciple.jpg",
}

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)

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

@bot.event
async def on_ready():
    try:
        if GUILD_ID:
            await bot.tree.sync(guild=discord.Object(id=int(GUILD_ID)))
        await bot.tree.sync()
    except Exception as e:
        print("Slash sync failed:", e)
    print(f"{bot.user} is online.")

def _get_sherpa_role(guild: discord.Guild) -> Optional[discord.Role]:
    if SHERPA_ROLE_ID:
        try:
            r = guild.get_role(int(SHERPA_ROLE_ID))
            if r: return r
        except ValueError:
            pass
    for r in guild.roles:
        if r.name.lower() == "sherpa assistant":
            return r
    return None

def _is_sherpa(member: discord.Member) -> bool:
    role = _get_sherpa_role(member.guild)
    return role is not None and role in member.roles

async def _send_to_channel_id(channel_id: Optional[str], content: Optional[str] = None, *, embed: Optional[discord.Embed] = None, file: Optional[discord.File] = None, allow_everyone: bool = False, allowed_mentions: Optional[discord.AllowedMentions] = None):
    if not channel_id:
        return None
    try:
        ch = bot.get_channel(int(channel_id)) or await bot.fetch_channel(int(channel_id))
        kwargs = {}
        if content is not None: kwargs["content"] = content
        if embed is not None: kwargs["embed"] = embed
        if file is not None: kwargs["file"] = file
        if allow_everyone:
            am = allowed_mentions or discord.AllowedMentions()
            am.everyone = True
            kwargs["allowed_mentions"] = am
        elif allowed_mentions is not None:
            kwargs["allowed_mentions"] = allowed_mentions
        return await ch.send(**kwargs)
    except Exception as e:
        print("Send failed:", e)
        return None

_PALETTE = [discord.Color.blurple(), discord.Color.purple(), discord.Color.gold(), discord.Color.orange(), discord.Color.green(), discord.Color.teal(), discord.Color.red(), discord.Color.blue()]
def _activity_color(activity: str) -> discord.Color:
    return _PALETTE[sum(ord(c) for c in activity) % len(_PALETTE)]

def _apply_activity_image(embed: discord.Embed, activity: str) -> Tuple[discord.Embed, Optional[discord.File]]:
    p = ACTIVITY_IMAGES.get(activity)
    if not p:
        return embed, None
    try:
        if p.startswith("http://") or p.startswith("https://"):
            embed.set_image(url=p)
            return embed, None
        if os.path.isfile(p):
            fn = os.path.basename(p)
            f = discord.File(p, filename=fn)
            embed.set_image(url=f"attachment://{fn}")
            return embed, f
    except Exception as e:
        print("Image attach error:", e)
    return embed, None

ALL_ACTIVITIES: List[str] = []
CAP_BY_CATEGORY: Dict[str, int] = {"raids": 6, "dungeons": 3, "exotic_activities": 3}
for cat, items in ACTIVITIES.items():
    if isinstance(items, list):
        ALL_ACTIVITIES.extend(items)
QUEUES: Dict[str, List[int]] = {}

def _category_of_activity(name: str) -> Optional[str]:
    for cat, items in ACTIVITIES.items():
        if name in items:
            return cat
    return None

def _cap_for_activity(name: str) -> int:
    return CAP_BY_CATEGORY.get(_category_of_activity(name) or "", 6)

def _user_current_activities(uid: int) -> List[str]:
    return [a for a, lst in QUEUES.items() if uid in lst]

def _ensure_queue(name: str) -> List[int]:
    return QUEUES.setdefault(name, [])

def _category_label(cat: Optional[str]) -> str:
    return {"raids": "Raid", "dungeons": "Dungeon", "exotic_activities": "Exotic"}.get(cat or "", "Activity")

def _activity_choices(prefix: str) -> List[app_commands.Choice[str]]:
    pref = (prefix or "").lower()
    out: List[app_commands.Choice[str]] = []
    for cat, items in ACTIVITIES.items():
        if not isinstance(items, list):
            continue
        label = _category_label(cat)
        for a in sorted(items, key=lambda x: x.lower()):
            if pref and pref not in a.lower():
                continue
            out.append(app_commands.Choice(name=f"{label}: {a}", value=a))
            if len(out) >= 25:
                return out
    if not out:
        flat = [a for sub in ACTIVITIES.values() if isinstance(sub, list) for a in sub]
        for a in sorted(flat, key=lambda x: x.lower())[:25]:
            out.append(app_commands.Choice(name=f"{_category_label(_category_of_activity(a))}: {a}", value=a))
    return out

async def activity_autocomplete(inter: discord.Interaction, current: str):
    return _activity_choices(current)

@bot.tree.command(name="ping", description="Check bot latency")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message(f"Pong! {round(bot.latency*1000)} ms")

@bot.tree.command(name="join", description="Join an activity queue")
@app_commands.describe(activity="Choose an activity to join")
@app_commands.autocomplete(activity=activity_autocomplete)
async def join_cmd(interaction: discord.Interaction, activity: str):
    if isinstance(interaction.user, discord.Member) and _is_sherpa(interaction.user):
        await interaction.response.send_message("Sherpa Assistants cannot join queues.", ephemeral=True); return
    if activity not in ALL_ACTIVITIES:
        await interaction.response.send_message("Unknown activity.", ephemeral=True); return
    uid = interaction.user.id
    if activity in _user_current_activities(uid):
        await interaction.response.send_message("You are already signed up for this activity.", ephemeral=True); return
    if len(_user_current_activities(uid)) >= 2:
        await interaction.response.send_message("You can only be in 2 different activity queues at once.", ephemeral=True); return
    _ensure_queue(activity).append(uid)
    await interaction.response.send_message(f"Joined queue for: {activity}", ephemeral=True)
    await _post_activity_board(activity)

@bot.tree.command(name="queue", description="Post the current queues (one embed per activity, all names)")
async def queue_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await _post_all_activity_boards()
    await interaction.followup.send("Queue boards posted.", ephemeral=True)

def _parse_user_ids(text: str, guild: discord.Guild) -> List[int]:
    if not text: return []
    parts = [p.strip() for p in text.replace(",", " ").split() if p.strip()]
    ids: List[int] = []
    for p in parts:
        if p.startswith("<@") and p.endswith(">"): p = p.strip("<@!>")
        try:
            mid = int(p)
            if guild.get_member(mid): ids.append(mid); continue
        except ValueError:
            pass
        lower = p.lower()
        m = discord.utils.find(lambda u: lower in u.display_name.lower() or lower in u.name.lower(), guild.members)
        if m: ids.append(m.id)
    out, seen = [], set()
    for i in ids:
        if i not in seen: seen.add(i); out.append(i)
    return out

@bot.tree.command(name="remove_from_queue", description="Remove one or more users from an activity queue (founder only)")
@promoter_only()
@app_commands.describe(users="Mentions/IDs/names separated by spaces/commas", activity="Activity queue to remove them from")
@app_commands.autocomplete(activity=activity_autocomplete)
async def remove_from_queue_cmd(interaction: discord.Interaction, users: str, activity: str):
    if activity not in ALL_ACTIVITIES:
        await interaction.response.send_message("Unknown activity.", ephemeral=True); return
    guild = interaction.guild
    if not guild:
        await interaction.response.send_message("This command must be used in a server.", ephemeral=True); return
    targets = _parse_user_ids(users, guild)
    if not targets:
        await interaction.response.send_message("No valid users found.", ephemeral=True); return
    q = QUEUES.get(activity)
    if not q:
        await interaction.response.send_message(f"No queue exists yet for **{activity}**.", ephemeral=True); return
    removed = []
    for uid in targets:
        if uid in q:
            member = guild.get_member(uid)
            removed.append(member.display_name if member else str(uid))
            q[:] = [x for x in q if x != uid]
    if not q: QUEUES.pop(activity, None)
    if not removed:
        await interaction.response.send_message(f"No selected users were in the **{activity}** queue.", ephemeral=True); return
    await interaction.response.send_message(f"Removed from **{activity}**: {', '.join(removed)}", ephemeral=True)
    await _post_activity_board(activity)

@bot.tree.command(name="promote", description="Promote a member to Sherpa Assistant and announce it")
@promoter_only()
@app_commands.describe(user="Member to promote")
async def promote(interaction: discord.Interaction, user: discord.Member):
    await interaction.response.defer(ephemeral=True)
    if interaction.guild is None:
        await interaction.followup.send("This command can only be used in a server.", ephemeral=True); return
    role = _get_sherpa_role(interaction.guild)
    if not role:
        await interaction.followup.send("Could not find the 'Sherpa Assistant' role. Set SHERPA_ROLE_ID or create one.", ephemeral=True); return
    assigned = False
    if role not in user.roles:
        try:
            await user.add_roles(role, reason=f"Promoted by {interaction.user} via /promote"); assigned = True
        except discord.Forbidden:
            await interaction.followup.send("I need 'Manage Roles' and my role must be above 'Sherpa Assistant'.", ephemeral=True); return
    lines = [
        f"🎉 **Congratulations, {user.mention}!** 🏆",
        "",
        "You’re now a **Sherpa Assistant** — patience, clarity, and positive vibes.",
        "Help newer/returning players learn mechanics and win together.",
        "",
        "⚔️ **Expectations**",
        "• Be calm under pressure",
        "• Explain mechanics clearly",
        "• Turn wipes into lessons",
        "• Keep runs welcoming and fun",
        "",
        "🌌 **Carry the Light**",
    ]
    embed = discord.Embed(title="Sherpa Promotion 🌟", description="\n".join(lines),
                          color=random.choice([discord.Color.blurple(), discord.Color.green(), discord.Color.gold(), discord.Color.purple(), discord.Color.orange()]))
    icon = getattr(user.display_avatar, "url", None)
    if icon:
        embed.set_author(name=user.display_name, icon_url=icon)
        embed.set_thumbnail(url=icon)
    embed.set_footer(text=f"Promoted by {interaction.user.display_name}")
    sent = 0
    for ch in (GENERAL_CHANNEL_ID, GENERAL_SHERPA_CHANNEL_ID):
        if await _send_to_channel_id(ch, None, embed=embed): sent += 1
    await interaction.followup.send(f"Role {'assigned' if assigned else 'already present'}; announcement sent in {sent} channel(s).", ephemeral=True)

async def _post_activity_board(activity: str) -> None:
    if not RAID_QUEUE_CHANNEL_ID or activity not in QUEUES:
        return
    q = QUEUES.get(activity, [])
    cap = _cap_for_activity(activity)
    embed = discord.Embed(title=f"Queue — {activity}", color=_activity_color(activity))
    embed.add_field(name="Capacity", value=str(cap), inline=True)
    embed.add_field(name="Signed Up", value=str(len(q)), inline=True)
    if q:
        embed.add_field(name="Players (in order)", value="\n".join(f"<@{u}>" for u in q), inline=False)
    else:
        embed.description = "No sign-ups yet. Use `/join` to get started."
    embed, attachment = _apply_activity_image(embed, activity)
    await _send_to_channel_id(RAID_QUEUE_CHANNEL_ID, None, embed=embed, file=attachment)

async def _post_all_activity_boards() -> None:
    if not RAID_QUEUE_CHANNEL_ID:
        return
    posted = False
    for cat, items in ACTIVITIES.items():
        if not isinstance(items, list): continue
        for a in sorted(items, key=lambda x: x.lower()):
            if a in QUEUES and QUEUES[a]:
                await _post_activity_board(a); posted = True
    if not posted:
        await _send_to_channel_id(RAID_QUEUE_CHANNEL_ID, "No active queues yet. Use `/join` to sign up.")

EVENT_REACTIONS: Dict[int, Dict[str, object]] = {}

def _parse_mmdd_time_tz(mmdd: str, hhmm: str, timezone: str):
    if ZoneInfo is None: return None
    try:
        month, day = [int(x) for x in mmdd.split("-")]
        hour, minute = [int(x) for x in hhmm.split(":")]
        tz = ZoneInfo(timezone)
    except Exception:
        return None
    now = datetime.now(tz); year = now.year
    dt = datetime(year, month, day, hour, minute, tzinfo=tz)
    if dt < now: dt = datetime(year + 1, month, day, hour, minute, tzinfo=tz)
    return int(dt.timestamp()), tz.key

def _names_from_ids(guild: discord.Guild, ids: Set[int]) -> str:
    if not ids: return "—"
    out = []
    for uid in ids:
        m = guild.get_member(uid)
        out.append(m.display_name if m else f"<@{uid}>")
    return "\n".join(out)

async def _render_event_embed(guild: discord.Guild, activity: str, ts: int, note: Optional[str], data: Dict[str, object]) -> Tuple[discord.Embed, Optional[discord.File]]:
    embed = discord.Embed(title=f"📣 Event: {activity}", description=(note or "Be ready and bring good vibes!"), color=_activity_color(activity))
    embed.add_field(name="When", value=f"<t:{ts}:F> (<t:{ts}:R>)", inline=False)
    embed.add_field(name="Category", value=_category_label(_category_of_activity(activity)), inline=True)
    embed.add_field(name="Sherpa Requests (🧭)", value=_names_from_ids(guild, data.get("sherpa", set())), inline=False)
    embed.add_field(name="Backups (✅)", value=_names_from_ids(guild, data.get("backup", set())), inline=False)
    embed.add_field(name="Backed Out (❌)", value=_names_from_ids(guild, data.get("backout", set())), inline=False)
    embed.set_footer(text="React: 🧭 Sherpa • ✅ Backup • ❌ Back out")
    embed, attachment = _apply_activity_image(embed, activity)
    return embed, attachment

@bot.tree.command(name="event", description="Create and announce an event (@everyone) with reactions")
@promoter_only()
@app_commands.describe(activity="Pick an activity", date="MM-DD (no year)", time="HH:MM 24h", timezone="IANA tz (e.g., America/New_York)", note="Optional details")
@app_commands.autocomplete(activity=activity_autocomplete)
async def event_cmd(interaction: discord.Interaction, activity: str, date: str, time: str, timezone: str, note: Optional[str] = None):
    await interaction.response.defer(ephemeral=True)
    if activity not in ALL_ACTIVITIES:
        await interaction.followup.send("Unknown activity.", ephemeral=True); return
    parsed = _parse_mmdd_time_tz(date, time, timezone)
    if not parsed:
        await interaction.followup.send("Invalid date/time/timezone. Use MM-DD, HH:MM, and a valid IANA zone.", ephemeral=True); return
    ts, _ = parsed
    if not LFG_CHAT_CHANNEL_ID:
        await interaction.followup.send("Set LFG_CHAT_CHANNEL_ID.", ephemeral=True); return
    data = {"activity": activity, "when": ts, "note": note, "sherpa": set(), "backup": set(), "backout": set()}
    embed, attachment = await _render_event_embed(interaction.guild, activity, ts, note, data)
    msg = await _send_to_channel_id(LFG_CHAT_CHANNEL_ID, "@everyone New event posted!", embed=embed, file=attachment, allow_everyone=True)
    if not msg:
        await interaction.followup.send("Failed to post in LFG.", ephemeral=True); return
    EVENT_REACTIONS[msg.id] = data
    try:
        await msg.add_reaction("🧭")
        await msg.add_reaction("✅")
        await msg.add_reaction("❌")
    except Exception as e:
        print("Add reactions failed:", e)
    await interaction.followup.send("Event announced.", ephemeral=True)

async def _update_event_message(payload: discord.RawReactionActionEvent, add: bool):
    mid = payload.message_id
    if mid not in EVENT_REACTIONS: return
    guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
    if guild is None: return
    data = EVENT_REACTIONS[mid]
    emoji = str(payload.emoji); uid = payload.user_id
    if uid == bot.user.id: return

    member = guild.get_member(uid) or await guild.fetch_member(uid)
    key = None
    if emoji == "🧭":
        if not _is_sherpa(member):
            try:
                ch = guild.get_channel(payload.channel_id) or await guild.fetch_channel(payload.channel_id)
                msg = await ch.fetch_message(mid)
                await msg.remove_reaction("🧭", member)
            except Exception:
                pass
            return
        key = "sherpa"
    elif emoji == "✅":
        key = "backup"
    elif emoji == "❌":
        key = "backout"
    else:
        return

    for k in ("sherpa","backup","backout"):
        if k not in data: data[k] = set()

    was = uid in data[key]
    if add:
        data[key].add(uid)
        if key == "backout":
            data["sherpa"].discard(uid); data["backup"].discard(uid)
    else:
        data[key].discard(uid)

    if add and key == "sherpa" and not was and GENERAL_SHERPA_CHANNEL_ID:
        role = _get_sherpa_role(guild)
        mention = role.mention if role else "Sherpas"
        content = f"{mention} • {member.mention} is requesting Sherpa help for **{data['activity']}** at <t:{data['when']}:F> (<t:{data['when']}:R>)."
        await _send_to_channel_id(GENERAL_SHERPA_CHANNEL_ID, content, allowed_mentions=discord.AllowedMentions(roles=True, users=True, everyone=False))

    try:
        ch = guild.get_channel(payload.channel_id) or await guild.fetch_channel(payload.channel_id)
        msg = await ch.fetch_message(mid)
        new_embed, _ = await _render_event_embed(guild, data["activity"], data["when"], data.get("note"), data)
        await msg.edit(embed=new_embed)
    except Exception as e:
        print("Edit failed:", e)

@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    await _update_event_message(payload, True)

@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    await _update_event_message(payload, False)

async def _post_all_activity_boards() -> None:
    if not RAID_QUEUE_CHANNEL_ID:
        return
    posted = False
    for cat, items in ACTIVITIES.items():
        if not isinstance(items, list): continue
        for a in sorted(items, key=lambda x: x.lower()):
            if a in QUEUES and QUEUES[a]:
                await _post_activity_board(a); posted = True
    if not posted:
        await _send_to_channel_id(RAID_QUEUE_CHANNEL_ID, "No active queues yet. Use `/join` to sign up.")

bot.run(TOKEN)
