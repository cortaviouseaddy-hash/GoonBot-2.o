# GoonBot main file — unlimited queues, founder check-in, schedule with DM confirmations & reminders
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from presets_loader import load_presets
from env_safety import get_token

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # py3.8 fallback

# ---------------
# Config & presets
# ---------------
ACTIVITIES = load_presets()  # {'raids': [...], 'dungeons': [...], 'exotic_activities': [...]}

# env helper with fallback to name
def _get_first_env(key: str, fallback_name: str) -> Optional[str]:
    v = os.getenv(key)
    if v:
        return v
    # allow using channel name directly in dev
    return os.getenv(f"{fallback_name}_NAME")

GENERAL_CHANNEL_ID = _get_first_env("GENERAL_CHANNEL_ID", "GENERAL")
GENERAL_SHERPA_CHANNEL_ID = _get_first_env("GENERAL_SHERPA_CHANNEL_ID", "GENERAL_SHERPA")
RAID_QUEUE_CHANNEL_ID = _get_first_env("RAID_QUEUE_CHANNEL_ID", "RAID_QUEUE")
LFG_CHAT_CHANNEL_ID = _get_first_env("LFG_CHAT_CHANNEL_ID", "LFG_CHAT")

FOUNDER_USER_ID = os.getenv("FOUNDER_USER_ID")
SHERPA_ROLE_ID = os.getenv("SHERPA_ROLE_ID")

# Map activity to local image path. If files are missing, the bot will just skip images.
ACTIVITY_IMAGES: Dict[str, str] = {
    "Crota's End": "assets/raids/crotas_end.jpg",
    "Deep Stone Crypt": "assets/raids/deep_stone_crypt.jpg",
    "Garden of Salvation": "assets/raids/garden_of_salvation.jpg",
    "King's Fall": "assets/raids/kings_fall.jpg",
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

# ----------------
# Permissions
# ----------------

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


def founder_only():
    async def predicate(interaction: discord.Interaction) -> bool:
        if interaction.guild is None:
            raise app_commands.CheckFailure("Use this in a server.")
        try:
            return FOUNDER_USER_ID and interaction.user.id == int(FOUNDER_USER_ID)
        except Exception:
            return False
    return app_commands.check(predicate)


@bot.event
async def on_ready():
    try:
        await bot.tree.sync()
    except Exception as e:
        print("Slash sync failed:", e)
    # Start scheduler loop once
    if not getattr(bot, "_sched_task", None):
        bot._sched_task = bot.loop.create_task(_scheduler_loop())
    print(f"Ready as {bot.user}")

# ----------------
# Utilities
# ----------------

def _get_sherpa_role(guild: discord.Guild) -> Optional[discord.Role]:
    # Prefer explicit env id
    if SHERPA_ROLE_ID:
        r = guild.get_role(int(SHERPA_ROLE_ID))
        if r:
            return r
    # Fallback by name
    for r in guild.roles:
        if r.name.lower() in {"sherpa assistant", "sherpa", "sherpa-assistant"}:
            return r
    return None


def _is_sherpa(member: discord.Member) -> bool:
    role = _get_sherpa_role(member.guild)
    return role is not None and role in member.roles


async def _send_to_channel_id(channel_id: Optional[str], content: Optional[str] = None, *,
                              embed: Optional[discord.Embed] = None, file: Optional[discord.File] = None,
                              allow_everyone: bool = False, allowed_mentions: Optional[discord.AllowedMentions] = None):
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

# A tiny consistent palette
_PALETTE = [discord.Color.blurple(), discord.Color.purple(), discord.Color.gold(), discord.Color.orange(),
            discord.Color.green(), discord.Color.teal(), discord.Color.red(), discord.Color.blue()]

def _activity_color(activity: str) -> discord.Color:
    return _PALETTE[sum(ord(c) for c in activity) % len(_PALETTE)]


def _apply_activity_image(embed: discord.Embed, activity: str) -> Tuple[discord.Embed, Optional[discord.File]]:
    p = ACTIVITY_IMAGES.get(activity)
    if not p:
        return embed, None
    try:
        f = discord.File(p, filename=os.path.basename(p))
        embed.set_image(url=f"attachment://{os.path.basename(p)}")
        return embed, f
    except Exception:
        return embed, None

# --------------
# Queues & boards
# --------------
ALL_ACTIVITIES: List[str] = []
CAP_BY_CATEGORY: Dict[str, int] = {"raids": 6, "dungeons": 3, "exotic_activities": 3}
for cat, items in ACTIVITIES.items():
    if isinstance(items, list):
        ALL_ACTIVITIES.extend(items)
QUEUES: Dict[str, List[int]] = {}
CHECKED: Dict[str, Set[int]] = {}


def _category_of_activity(name: str) -> Optional[str]:
    for cat, items in ACTIVITIES.items():
        if name in items:
            return cat
    return None


def _cap_for_activity(name: str) -> int:
    return CAP_BY_CATEGORY.get(_category_of_activity(name) or "", 6)


def _ensure_queue(name: str) -> List[int]:
    return QUEUES.setdefault(name, [])


def _ensure_checked(name: str) -> Set[int]:
    return CHECKED.setdefault(name, set())


async def _post_all_activity_boards():
    if not RAID_QUEUE_CHANNEL_ID:
        return
    for act in list(QUEUES.keys()):
        await _post_activity_board(act)


def _activity_autocomplete(_: discord.Interaction, current: str):
    return [app_commands.Choice(name=a, value=a) for a in ALL_ACTIVITIES if current.lower() in a.lower()][:25]


async def _post_activity_board(activity: str) -> None:
    if not RAID_QUEUE_CHANNEL_ID or activity not in QUEUES:
        return
    q = QUEUES.get(activity, [])
    checked = _ensure_checked(activity)

    embed = discord.Embed(title=f"Queue — {activity}", color=_activity_color(activity))
    # No capacity field (unlimited display)
    embed.add_field(name="Signed Up", value=str(len(q)), inline=True)

    if q:
        lines = []
        for uid in q:
            mark = " ✅" if uid in checked else ""
            lines.append(f"<@{uid}>{mark}")
        embed.add_field(name="Players (in order)", value="
".join(lines), inline=False)
    else:
        embed.description = "No sign-ups yet. Use `/join` to get started."

    embed, attachment = _apply_activity_image(embed, activity)
    await _send_to_channel_id(RAID_QUEUE_CHANNEL_ID, None, embed=embed, file=attachment)

# --------------
# Slash: ping / join / queue / add / remove / check-in
# --------------

@bot.tree.command(name="ping", description="Check bot latency")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message(f"Pong! {round(bot.latency*1000)} ms")


@bot.tree.command(name="join", description="Join an activity queue")
@app_commands.describe(activity="Choose an activity to join")
@app_commands.autocomplete(activity=_activity_autocomplete)
async def join_cmd(interaction: discord.Interaction, activity: str):
    # Sherpa Assistants cannot join queues
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if member and _is_sherpa(member):
        await interaction.response.send_message("Sherpa Assistants cannot join queues.", ephemeral=True); return
    if activity not in ALL_ACTIVITIES:
        await interaction.response.send_message("Unknown activity.", ephemeral=True); return
    uid = interaction.user.id
    # Allow up to two different activities
    in_any = [a for a, lst in QUEUES.items() if uid in lst]
    if activity in in_any:
        await interaction.response.send_message("You're already in that queue.", ephemeral=True); return
    if len(in_any) >= 2:
        await interaction.response.send_message("You can be in at most 2 different activity queues.", ephemeral=True); return
    _ensure_queue(activity).append(uid)
    await interaction.response.send_message(f"Joined queue for: {activity}", ephemeral=True)
    await _post_activity_board(activity)


@bot.tree.command(name="queue", description="Post the current queues (one embed per activity, all names)")
async def queue_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await _post_all_activity_boards()
    await interaction.followup.send("Queue boards posted.", ephemeral=True)


# Helper to parse mentions/IDs/names

def _parse_user_ids(text: str, guild: discord.Guild) -> List[int]:
    if not text: return []
    parts = [p.strip() for p in text.replace(",", " ").split() if p.strip()]
    out: List[int] = []
    for p in parts:
        if p.isdigit():
            out.append(int(p)); continue
        if p.startswith("<@") and p.endswith(">"):
            num = ''.join(ch for ch in p if ch.isdigit())
            if num: out.append(int(num)); continue
        # name fallback
        m = discord.utils.find(lambda m: m.display_name.lower() == p.lower() or m.name.lower() == p.lower(), guild.members)
        if m: out.append(m.id)
    # dedupe, preserve order
    seen = set(); uniq: List[int] = []
    for uid in out:
        if uid not in seen:
            uniq.append(uid); seen.add(uid)
    return uniq


@bot.tree.command(name="add_to_queue", description="(Founder) Add users to an activity queue")
@promoter_only()
@app_commands.describe(users="Mentions/IDs/names separated by spaces/commas", activity="Activity name")
@app_commands.autocomplete(activity=_activity_autocomplete)
async def add_to_queue_cmd(interaction: discord.Interaction, users: str, activity: str):
    if activity not in ALL_ACTIVITIES:
        await interaction.response.send_message("Unknown activity.", ephemeral=True); return
    guild = interaction.guild
    if not guild:
        await interaction.response.send_message("This command must be used in a server.", ephemeral=True); return
    targets = _parse_user_ids(users, guild)
    if not targets:
        await interaction.response.send_message("No valid users found.", ephemeral=True); return
    q = _ensure_queue(activity)
    added = []
    for uid in targets:
        if uid not in q:
            q.append(uid)
            m = guild.get_member(uid)
            added.append(m.display_name if m else str(uid))
    if not added:
        await interaction.response.send_message("Everyone you listed is already in the queue.", ephemeral=True); return
    await interaction.response.send_message(f"Added to **{activity}**: {', '.join(added)}", ephemeral=True)
    await _post_activity_board(activity)


@bot.tree.command(name="remove_from_queue", description="(Founder) Remove users from an activity queue")
@promoter_only()
@app_commands.describe(users="Mentions/IDs/names separated by spaces/commas", activity="Activity name")
@app_commands.autocomplete(activity=_activity_autocomplete)
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
            _ensure_checked(activity).discard(uid)
    if not q: QUEUES.pop(activity, None)
    if not removed:
        await interaction.response.send_message(f"No selected users were in the **{activity}** queue.", ephemeral=True); return
    await interaction.response.send_message(f"Removed from **{activity}**: {', '.join(removed)}", ephemeral=True)
    await _post_activity_board(activity)


# Founder-only green-check toggles
@bot.tree.command(name="check_in", description="Add a ✅ next to one or more users in an activity queue (founder only)")
@promoter_only()
@app_commands.describe(users="Mentions/IDs/names separated by spaces/commas",
                       activity="Activity whose queue to mark")
@app_commands.autocomplete(activity=_activity_autocomplete)
async def check_in_cmd(interaction: discord.Interaction, users: str, activity: str):
    if activity not in ALL_ACTIVITIES:
        await interaction.response.send_message("Unknown activity.", ephemeral=True); return
    guild = interaction.guild
    if not guild:
        await interaction.response.send_message("Use this in a server.", ephemeral=True); return

    targets = _parse_user_ids(users, guild)
    if not targets:
        await interaction.response.send_message("No valid users found.", ephemeral=True); return
    q = QUEUES.get(activity, [])
    if not q:
        await interaction.response.send_message(f"No queue exists yet for **{activity}**.", ephemeral=True); return

    checked = _ensure_checked(activity)
    added = []
    for uid in targets:
        if uid in q:
            checked.add(uid)
            m = guild.get_member(uid)
            added.append(m.display_name if m else str(uid))

    if not added:
        await interaction.response.send_message("No selected users are in that queue.", ephemeral=True); return

    await _post_activity_board(activity)
    await interaction.response.send_message(f"Checked ✅ in **{activity}**: {', '.join(added)}", ephemeral=True)


@bot.tree.command(name="uncheck_in", description="Remove the ✅ for one or more users in an activity queue (founder only)")
@promoter_only()
@app_commands.describe(users="Mentions/IDs/names separated by spaces/commas",
                       activity="Activity whose queue to unmark")
@app_commands.autocomplete(activity=_activity_autocomplete)
async def uncheck_in_cmd(interaction: discord.Interaction, users: str, activity: str):
    if activity not in ALL_ACTIVITIES:
        await interaction.response.send_message("Unknown activity.", ephemeral=True); return
    guild = interaction.guild
    if not guild:
        await interaction.response.send_message("Use this in a server.", ephemeral=True); return

    targets = _parse_user_ids(users, guild)
    if not targets:
        await interaction.response.send_message("No valid users found.", ephemeral=True); return
    q = QUEUES.get(activity, [])
    if not q:
        await interaction.response.send_message(f"No queue exists yet for **{activity}**.", ephemeral=True); return

    checked = _ensure_checked(activity)
    removed = []
    for uid in targets:
        if uid in checked:
            checked.discard(uid)
            m = guild.get_member(uid)
            removed.append(m.display_name if m else str(uid))

    if not removed:
        await interaction.response.send_message("None of the selected users were checked.", ephemeral=True); return

    await _post_activity_board(activity)
    await interaction.response.send_message(f"Unchecked in **{activity}**: {', '.join(removed)}", ephemeral=True)

# --------------
# Promote (Torchbearer style embed)
# --------------

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
    if role not in user.roles:
        try:
            await user.add_roles(role, reason=f"Promoted by {interaction.user} via /promote")
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
    embed = discord.Embed(title="Sherpa Promotion 🌟", description="
".join(lines), color=discord.Color.purple())
    embed.set_footer(text="Carry the torch. Lead the way.")

    # Post in General + General Sherpa if configured
    sent = 0
    for cid in (GENERAL_CHANNEL_ID, GENERAL_SHERPA_CHANNEL_ID):
        if cid:
            if await _send_to_channel_id(cid, embed=embed, allow_everyone=False):
                sent += 1
    await interaction.followup.send(f"Promoted {'and announced' if sent else ''}.", ephemeral=True)

# --------------
# Event embed helper
# --------------

def _category_label(cat: Optional[str]) -> str:
    return {
        "raids": "Raid",
        "dungeons": "Dungeon",
        "exotic_activities": "Exotic Mission",
        None: "Unknown",
    }.get(cat, "Unknown")


def _names_from_ids(guild: discord.Guild, ids: Set[int]) -> str:
    if not ids: return "—"
    out = []
    for uid in ids:
        m = guild.get_member(uid)
        out.append(m.display_name if m else f"<@{uid}>")
    return "
".join(out)


async def _render_event_embed(guild: discord.Guild, activity: str, data: Dict[str, object]) -> Tuple[discord.Embed, Optional[discord.File]]:
    when_text = data.get("when_text", "TBD")
    embed = discord.Embed(title=f"📣 Event: {activity}", description=data.get("desc", "Be ready and bring good vibes!"), color=_activity_color(activity))
    embed.add_field(name="When", value=str(when_text), inline=False)
    embed.add_field(name="Category", value=_category_label(_category_of_activity(activity)), inline=True)

    cap = data.get("capacity", _cap_for_activity(activity)) or _cap_for_activity(activity)
    embed.add_field(name="Capacity", value=str(cap), inline=True)

    # Sherpas
    reserved = int(data.get("reserved_sherpas", 0))
    sherpas = data.get("sherpas", set()) or set()
    sherpa_backup = data.get("sherpa_backup", set()) or set()
    embed.add_field(name=f"Sherpas ({len(sherpas)}/{reserved})", value=_names_from_ids(guild, sherpas) or "—", inline=False)
    if sherpa_backup:
        embed.add_field(name=f"Sherpa Backup ({len(sherpa_backup)})", value=_names_from_ids(guild, sherpa_backup), inline=False)

    # Participants
    participants: List[int] = data.get("players", []) or []  # confirmed players
    if participants:
        lines = [f"<@{uid}>" for uid in participants]
        embed.add_field(name=f"Participants ({len(participants)}/{cap})", value="
".join(lines), inline=False)

    backups: List[int] = data.get("backups", []) or []
    if backups:
        embed.add_field(name=f"Backups ({len(backups)})", value="
".join(f"<@{u}>" for u in backups), inline=False)

    embed, f = _apply_activity_image(embed, activity)
    embed.set_footer(text="Use 📝 on this post to join Backups. LFG reactions do not count.")
    return embed, f

# --------------
# Schedule system (Founder-only, DM everyone in queue; self-backup; auto-open; reminders; survey)
# --------------
SCHEDULES: Dict[int, Dict[str, object]] = {}  # event_message_id -> schedule data

class ConfirmView(discord.ui.View):
    def __init__(self, mid: int, uid: int):
        super().__init__(timeout=None)
        self.mid = mid
        self.uid = uid

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success, custom_id="confirm_yes")
    async def yes(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.uid:
            await interaction.response.send_message("This DM button isn't for you.", ephemeral=True)
            return
        data = SCHEDULES.get(self.mid)
        if not data:
            await interaction.response.send_message("Event no longer exists.", ephemeral=True)
            return
        participants: List[int] = data.get("players", [])  # type: ignore
        backups: List[int] = data.get("backups", [])  # type: ignore
        cap = int(data.get("capacity", 0))
        reserved = int(data.get("reserved_sherpas", 0))
        player_slots = max(0, cap - reserved)
        if self.uid in participants:
            await interaction.response.send_message("You're already locked in.", ephemeral=True)
            return
        if len(participants) < player_slots:
            participants.append(self.uid)
            await interaction.response.send_message("Locked in. See you there! ✅", ephemeral=True)
        else:
            if self.uid not in backups:
                backups.append(self.uid)
            await interaction.response.send_message("Roster is full — you've been added as **Backup**.", ephemeral=True)
        guild = interaction.client.get_guild(data.get("guild_id"))  # type: ignore
        if guild:
            await _update_schedule_message(guild, self.mid)

    @discord.ui.button(label="Can't make it", style=discord.ButtonStyle.secondary, custom_id="confirm_no")
    async def no(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.uid:
            await interaction.response.send_message("This DM button isn't for you.", ephemeral=True)
            return
        data = SCHEDULES.get(self.mid)
        if data:
            participants: List[int] = data.get("players", [])  # type: ignore
            if self.uid in participants:
                participants[:] = [x for x in participants if x != self.uid]
                _autofill_from_backups(data)
            guild = interaction.client.get_guild(data.get("guild_id"))  # type: ignore
            if guild:
                await _update_schedule_message(guild, self.mid)
        await interaction.response.send_message("All good. Thanks for letting us know.", ephemeral=True)


def _autofill_from_backups(data: Dict[str, object]):
    cap = int(data.get("capacity", 0))
    reserved = int(data.get("reserved_sherpas", 0))
    player_slots = max(0, cap - reserved)
    participants: List[int] = data.get("players", [])  # type: ignore
    backups: List[int] = data.get("backups", [])  # type: ignore
    while len(participants) < player_slots and backups:
        nxt = backups.pop(0)
        if nxt not in participants:
            participants.append(nxt)


async def _update_schedule_message(guild: discord.Guild, message_id: int):
    data = SCHEDULES.get(message_id)
    if not data: return
    channel_id = data.get("channel_id")
    if not channel_id: return
    try:
        ch = bot.get_channel(int(channel_id)) or await bot.fetch_channel(int(channel_id))
        msg = await ch.fetch_message(int(message_id))
        embed, _ = await _render_event_embed(guild, data["activity"], data)
        await msg.edit(embed=embed)
    except Exception as e:
        print("Failed to update schedule msg:", e)


def _parse_date_time_to_epoch(date_str: Optional[str], time_str: Optional[str], tz_name: str = "America/New_York") -> Optional[int]:
    if not date_str or not time_str:
        return None
    try:
        y, m, d = map(int, date_str.split("-"))
        hh, mm = map(int, time_str.split(":"))
        tz = ZoneInfo(tz_name) if ZoneInfo else None
        dt = datetime(y, m, d, hh, mm, tzinfo=tz)
        return int(dt.timestamp())
    except Exception:
        return None


async def _scheduler_loop():
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            now = int(datetime.now(ZoneInfo("UTC") if ZoneInfo else None).timestamp())
            for mid, data in list(SCHEDULES.items()):
                start_ts = data.get("start_ts")
                if not start_ts:
                    continue
                cap = int(data.get("capacity", 0))
                reserved = int(data.get("reserved_sherpas", 0))
                player_slots = max(0, cap - reserved)
                participants: List[int] = data.get("players", [])  # type: ignore

                # 2h auto-open (to everyone) if short on participants; announcement in LFG
                if not data.get("signups_open") and now >= start_ts - 2*60*60 and len(participants) < player_slots:
                    data["signups_open"] = True
                    _autofill_from_backups(data)
                    await _send_to_channel_id(
                        LFG_CHAT_CHANNEL_ID or GENERAL_CHANNEL_ID,
                        content=(
                            f"📣 **{data['activity']}** starts soon. We still need players.
"
                            f"👉 Go to the **event signup post** and react there to join. (Reactions **here** won't count.)"
                        ),
                    )

                # Reminders to participants and sherpas
                for label, delta, key in (("2h", 2*60*60, "r_2h"), ("30m", 30*60, "r_30m"), ("start", 0, "r_0m")):
                    if not data.get(key) and now >= start_ts - delta:
                        await _send_reminders(data, label)
                        data[key] = True
        except Exception as e:
            print("scheduler error:", e)
        finally:
            # tick roughly once per minute
            await discord.utils.sleep_until(datetime.utcnow().replace(second=0, microsecond=0) + timedelta(minutes=1))


async def _send_reminders(data: Dict[str, object], label: str):
    guild = bot.get_guild(data.get("guild_id"))  # type: ignore
    if not guild:
        return
    activity = data.get("activity", "Event")
    when_text = data.get("when_text", "soon")

    participants: List[int] = data.get("players", [])  # type: ignore
    sherpas: Set[int] = data.get("sherpas", set())  # type: ignore

    msg = {
        "2h": f"Reminder: **{activity}** in ~2 hours ({when_text}).",
        "30m": f"Reminder: **{activity}** in ~30 minutes ({when_text}).",
        "start": f"It's time: **{activity}** ({when_text}).",
    }.get(label, f"Reminder: **{activity}** ({when_text}).")

    async def dm(uid: int):
        try:
            member = guild.get_member(uid)
            if not member:
                return
            d = await member.create_dm()
            await d.send(msg)
        except Exception:
            pass

    for uid in participants:
        await dm(uid)
    for uid in sherpas:
        await dm(uid)

    # Post-event survey DM (3h after start)
    if label == "start":
        async def survey_task():
            try:
                await discord.utils.sleep_until(datetime.utcnow() + timedelta(hours=3))
                g = bot.get_guild(data.get("guild_id"))  # type: ignore
                if not g:
                    return
                survey_msg = (
                    f"Thanks for running **{activity}**! We'd love your feedback.
"
                    f"Please fill out the survey in **#survey-and-suggestions**."
                )
                for uid in participants:
                    try:
                        member = g.get_member(uid)
                        if member:
                            d = await member.create_dm()
                            await d.send(survey_msg)
                    except Exception:
                        pass
            except Exception:
                pass
        bot.loop.create_task(survey_task())


@bot.tree.command(name="schedule", description="(Founder) Schedule a run: DM everyone in queue; backups once full; reminders; LFG announce")
@founder_only()
@app_commands.describe(activity="Activity name",
                       when_text="Shown in the embed (e.g., 'Today 5pm ET')",
                       date="YYYY-MM-DD (for reminders/auto-open)",
                       time="HH:MM 24h in America/New_York (for reminders/auto-open)",
                       reserved_sherpas="Number of Sherpa slots to reserve (default 2)")
@app_commands.autocomplete(activity=_activity_autocomplete)
async def schedule_cmd(interaction: discord.Interaction, activity: str, when_text: str,
                       date: Optional[str] = None, time: Optional[str] = None, reserved_sherpas: Optional[int] = 2):
    if activity not in ALL_ACTIVITIES:
        await interaction.response.send_message("Unknown activity.", ephemeral=True); return

    cap = _cap_for_activity(activity)
    reserved = max(0, min(int(reserved_sherpas or 0), cap))

    q = QUEUES.get(activity, [])
    candidates = list(q)  # DM **everyone** in the queue

    start_ts = _parse_date_time_to_epoch(date, time, tz_name="America/New_York")

    data = {
        "guild_id": interaction.guild.id,
        "activity": activity,
        "desc": f"Scheduled by {interaction.user.mention}. Check your DMs to confirm.",
        "when_text": when_text,
        "capacity": cap,
        "reserved_sherpas": reserved,
        "sherpas": set(),
        "sherpa_backup": set(),
        "candidates": candidates,
        "players": [],           # participants (confirmed)
        "backups": [],           # stays empty until roster is full
        "signups_open": False,
        "channel_id": GENERAL_CHANNEL_ID or RAID_QUEUE_CHANNEL_ID,
        "start_ts": start_ts,
        "r_2h": False, "r_30m": False, "r_0m": False,
    }

    # Post event embed
    embed, f = await _render_event_embed(interaction.guild, activity, data)
    await interaction.response.defer(ephemeral=True)
    ev_msg = await _send_to_channel_id(data["channel_id"], embed=embed, file=f)
    if not ev_msg:
        await interaction.followup.send("Failed to post event.", ephemeral=True); return

    mid = ev_msg.id
    SCHEDULES[mid] = data

    # DM **all** queue members with Confirm button
    sent = 0
    for uid in candidates:
        try:
            m = interaction.guild.get_member(uid)
            if not m: continue
            dm = await m.create_dm()
            await dm.send(
                content=(
                    f"You've been selected for **{activity}** at **{when_text}** in {interaction.guild.name}.
"
                    f"Tap **Confirm** to lock your spot."
                ),
                view=ConfirmView(mid=mid, uid=uid)
            )
            sent += 1
        except Exception:
            pass

    # Sherpa alert (first R get slots; extras go to backup)
    if GENERAL_SHERPA_CHANNEL_ID and reserved > 0:
        alert = await _send_to_channel_id(
            GENERAL_SHERPA_CHANNEL_ID,
            content=(
                f"🧭 **Sherpa Alert:** {activity} at **{when_text}**. "
                f"{reserved} reserved Sherpa slot(s). React ✅ to claim."
            )
        )
        if alert:
            SCHEDULES[mid]["sherpa_alert_channel_id"] = str(alert.channel.id)
            SCHEDULES[mid]["sherpa_alert_message_id"] = str(alert.id)
            try:
                await alert.add_reaction("✅")
            except Exception:
                pass

    await interaction.followup.send(f"Scheduled **{activity}**. DMed {sent} queue member(s).", ephemeral=True)


@bot.tree.command(name="open_signups", description="(Founder) Open event to everyone (react on event only; LFG is announcement-only)")
@founder_only()
@app_commands.describe(event_message_id="Right-click Copy ID of the event message")
async def open_signups_cmd(interaction: discord.Interaction, event_message_id: str):
    try:
        mid = int(event_message_id)
    except ValueError:
        await interaction.response.send_message("Invalid message ID.", ephemeral=True); return
    data = SCHEDULES.get(mid)
    if not data:
        await interaction.response.send_message("I can't find that scheduled event.", ephemeral=True); return
    data["signups_open"] = True
    # Promote backups into participants until full
    _autofill_from_backups(data)
    await _update_schedule_message(interaction.guild, mid)
    # LFG announcement that directs users to the event post
    await _send_to_channel_id(
        LFG_CHAT_CHANNEL_ID or GENERAL_CHANNEL_ID,
        content=(
            f"📣 **{data['activity']}** signups are now open.
"
            f"👉 Go to the **event signup post** and react there to join. (Reactions **here** won't count.)"
        ),
    )
    await interaction.response.send_message("Signups opened and LFG announcement posted.", ephemeral=True)


# Reaction handlers: Sherpa claims; backup self-sign; public joins when open
@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if bot.user and payload.user_id == bot.user.id:
        return
    # Sherpa alert claim
    for mid, data in list(SCHEDULES.items()):
        alert_id = int(data.get("sherpa_alert_message_id")) if data.get("sherpa_alert_message_id") else None
        if alert_id and payload.message_id == alert_id and str(payload.emoji) == "✅":
            guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
            if not guild: return
            member = guild.get_member(payload.user_id)
            if not member or not _is_sherpa(member):
                return
            reserved = int(data.get("reserved_sherpas", 0))
            sherpas: Set[int] = data.get("sherpas")  # type: ignore
            backup: Set[int] = data.get("sherpa_backup")  # type: ignore
            if len(sherpas) < reserved and member.id not in sherpas:
                sherpas.add(member.id)
            else:
                backup.add(member.id)
            await _update_schedule_message(guild, mid)
            return

    # Backup self-sign on the EVENT message using 📝 anytime
    if str(payload.emoji) == "📝":
        data = SCHEDULES.get(payload.message_id)
        if not data:
            return
        guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
        if not guild:
            return
        participants: List[int] = data.get("players", [])  # type: ignore
        backups: List[int] = data.get("backups", [])  # type: ignore
        if payload.user_id not in participants and payload.user_id not in backups:
            backups.append(payload.user_id)
            await _update_schedule_message(guild, payload.message_id)
        return

    # Event public joins only when signups_open via ✅
    if str(payload.emoji) == "✅":
        data = SCHEDULES.get(payload.message_id)
        if not data or not data.get("signups_open"):
            return
        guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
        if not guild:
            return
        cap = int(data.get("capacity", 0))
        reserved = int(data.get("reserved_sherpas", 0))
        player_slots = max(0, cap - reserved)
        participants: List[int] = data.get("players", [])  # type: ignore
        backups: List[int] = data.get("backups", [])  # type: ignore
        if len(participants) < player_slots and payload.user_id not in participants:
            participants.append(payload.user_id)
        else:
            if payload.user_id not in backups:
                backups.append(payload.user_id)
        await _update_schedule_message(guild, payload.message_id)


@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    # If a participant removes ✅ from the event, treat as backing out: free slot and auto-fill from backups
    data = SCHEDULES.get(payload.message_id)
    if not data or str(payload.emoji) != "✅":
        return
    guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
    if not guild:
        return
    participants: List[int] = data.get("players", [])  # type: ignore
    if payload.user_id in participants:
        participants[:] = [x for x in participants if x != payload.user_id]
        _autofill_from_backups(data)
        await _update_schedule_message(guild, payload.message_id)


# --------------
# Boot
# --------------
if __name__ == "__main__":
    token = get_token("DISCORD_TOKEN")
    bot.run(token)
