# GoonBot main.py — full version with queues, check-in, promotions, scheduling,
# DM confirmations (DM everyone in queue), self-backups (📝), auto-open to everyone 2h prior,
# LFG announcement (read-only), reminders (2h/30m/start), and post-event survey (3h after start).
# Uses "\n" joins in f-strings to avoid unterminated string literal issues.

import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

import discord
import random
from discord import app_commands
from discord.ext import commands

# Project helpers expected:
# - presets_loader.load_presets() -> Dict[str, List[str]]
# - env_safety.get_token("DISCORD_TOKEN") -> str
from presets_loader import load_presets
from env_safety import get_token

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None  # If not available, continue without tz awareness

# Minimal runtime scaffolding and helpers (restores names used in the file).
# These are intentionally small, easy-to-replace implementations so the bot can run
# and the `/schedule` command works. You can replace with your fuller versions later.
INTENTS = discord.Intents.default()
INTENTS.members = True
INTENTS.message_content = True

# Bot instance
bot = commands.Bot(command_prefix="!", intents=INTENTS)

# Persistent in-memory structures
SCHEDULES: Dict[int, Dict[str, object]] = {}
QUEUES: Dict[str, List[int]] = {}

# Load presets of activities (presets_loader returns a dict of lists)
try:
    PRESETS = load_presets()
except Exception:
    PRESETS = {}

# Flatten presets into ALL_ACTIVITIES
ALL_ACTIVITIES: List[str] = []
for v in PRESETS.values():
    if isinstance(v, list):
        ALL_ACTIVITIES.extend(v)

def _cap_for_activity(activity: str) -> int:
    # Basic heuristic: raids 6, dungeons 3, default 6
    a = activity.lower()
    if any(k in a for k in ("raid", "vault", "deep", "wish", "garden")):
        return 6
    if any(k in a for k in ("dungeon", "lost", "crypt", "deep")):
        return 3
    return 6

async def _activity_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    choices: List[app_commands.Choice[str]] = []
    cur = (current or "").lower()
    for act in ALL_ACTIVITIES:
        if not cur or cur in act.lower():
            choices.append(app_commands.Choice(name=act, value=act))
            if len(choices) >= 25:
                break
    return choices

def _is_sherpa(member: discord.Member) -> bool:
    # Simple role-name heuristic; replace with role ID check if desired
    try:
        return any(r.name.lower().startswith("sherpa") for r in member.roles)
    except Exception:
        return False


def _ensure_queue(activity: str) -> List[int]:
    if activity not in QUEUES:
        QUEUES[activity] = []
    return QUEUES[activity]


async def _post_activity_board(activity: str):
    # Post a simple embed listing current queue members
    q = QUEUES.get(activity, [])
    embed = discord.Embed(title=f"Queue — {activity}", color=0x2F3136)
    embed.add_field(name="Signed Up", value=str(len(q)), inline=True)
    if q:
        lines = [f"<@{uid}>" for uid in q]
        embed.add_field(name="Players (in order)", value="\n".join(lines), inline=False)
    else:
        embed.description = "No sign-ups yet. Use `/join` to get started."
    await _send_to_channel_id(RAID_QUEUE_CHANNEL_ID, embed=embed)


async def _post_all_activity_boards():
    for act in ALL_ACTIVITIES:
        await _post_activity_board(act)


def founder_only():
    # If FOUNDER_USER_ID env var set, restrict command to that user; otherwise allow.
    fid = os.getenv("FOUNDER_USER_ID")
    def _check(interaction: discord.Interaction) -> bool:
        try:
            if not fid:
                return True
            return int(fid) == int(interaction.user.id)
        except Exception:
            return False
    return app_commands.check(_check)

def _parse_date_time_to_epoch(date_iso: str, time_part: str, tz_name: Optional[str] = None) -> Optional[int]:
    # date_iso expected as YYYY-MM-DD
    try:
        dt = datetime.strptime(f"{date_iso} {time_part}", "%Y-%m-%d %H:%M")
        if tz_name and ZoneInfo:
            try:
                tz = ZoneInfo(tz_name)
                dt = dt.replace(tzinfo=tz)
            except Exception:
                pass
        # Convert to UTC epoch
        if dt.tzinfo:
            epoch = int(dt.timestamp())
        else:
            # assume UTC if no tzinfo
            epoch = int(dt.replace(tzinfo=ZoneInfo("UTC") if ZoneInfo else None).timestamp())
        return epoch
    except Exception:
        return None

async def _send_to_channel_id(channel_id: Optional[int], content: Optional[str] = None, *, embed: Optional[discord.Embed] = None, file: Optional[discord.File] = None):
    try:
        if not channel_id:
            return None
        ch = bot.get_channel(int(channel_id)) or await bot.fetch_channel(int(channel_id))
        if not ch:
            return None
        if file and embed:
            return await ch.send(content=content, embed=embed, file=file)
        if embed:
            return await ch.send(content=content, embed=embed)
        return await ch.send(content=content)
    except Exception as e:
        print("_send_to_channel_id error:", e)
        return None

async def _render_event_embed(guild: Optional[discord.Guild], activity: str, data: Dict[str, object]):
    # Minimal embed renderer; you can expand to include images/colors
    title = f"{activity} — Event"
    desc = data.get("desc", "")
    embed = discord.Embed(title=title, description=desc, color=0x2F3136)
    when = data.get("when_text")
    embed.add_field(name="When", value=when or "TBD", inline=False)
    cap = data.get("capacity")
    embed.add_field(name="Capacity", value=str(cap), inline=True)
    players = data.get("players", [])
    if players:
        embed.add_field(name="Players", value="\n".join(f"<@{p}" for p in players), inline=False)
    return embed, None

# Channel IDs from env if provided
GENERAL_CHANNEL_ID = int(os.getenv("GENERAL_CHANNEL_ID")) if os.getenv("GENERAL_CHANNEL_ID") else None
RAID_QUEUE_CHANNEL_ID = int(os.getenv("RAID_QUEUE_CHANNEL_ID")) if os.getenv("RAID_QUEUE_CHANNEL_ID") else None
GENERAL_SHERPA_CHANNEL_ID = int(os.getenv("GENERAL_SHERPA_CHANNEL_ID")) if os.getenv("GENERAL_SHERPA_CHANNEL_ID") else None
LFG_CHAT_CHANNEL_ID = int(os.getenv("LFG_CHAT_CHANNEL_ID")) if os.getenv("LFG_CHAT_CHANNEL_ID" ) else None

# Ensure bot startup syncs commands and starts scheduler
@bot.event
async def on_ready():
    try:
        await bot.tree.sync()
    except Exception:
        pass
    try:
        bot.loop.create_task(_scheduler_loop())
    except Exception:
        pass
    ZoneInfo = None  # If not available, continue without tz awareness


# ---------------------------
# Slash Commands: ping / join / queue / add/remove / check-in
# ---------------------------

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
        await interaction.response.send_message("Sherpa Assistants cannot join queues.", ephemeral=True)
        return
    if activity not in ALL_ACTIVITIES:
        await interaction.response.send_message("Unknown activity.", ephemeral=True)
        return
    uid = interaction.user.id
    # Allow up to two different activities
    in_any = [a for a, lst in QUEUES.items() if uid in lst]
    if activity in in_any:
        await interaction.response.send_message("You're already in that queue.", ephemeral=True)
        return
    if len(in_any) >= 2:
        await interaction.response.send_message("You can be in at most 2 different activity queues.", ephemeral=True)
        return
    _ensure_queue(activity).append(uid)
    await interaction.response.send_message(f"Joined queue for: {activity}", ephemeral=True)
    await _post_activity_board(activity)


@bot.tree.command(name="queue", description="Post the current queues (one embed per activity, all names)")
async def queue_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await _post_all_activity_boards()
    await interaction.followup.send("Queue boards posted.", ephemeral=True)


# helper to parse mentions/IDs/names
def _parse_user_ids(text: str, guild: discord.Guild) -> List[int]:
    if not text:
        return []
    parts = [p.strip() for p in text.replace(",", " ").split() if p.strip()]
    out: List[int] = []
    for p in parts:
        if p.isdigit():
            out.append(int(p))
            continue
        if p.startswith("<@") and p.endswith(">"):
            num = "".join(ch for ch in p if ch.isdigit())
            if num:
                out.append(int(num))
                continue
        # name fallback
        m = discord.utils.find(lambda m: m.display_name.lower() == p.lower() or m.name.lower() == p.lower(), guild.members)
        if m:
            out.append(m.id)
    # dedupe preserve order
    seen = set(); uniq: List[int] = []
    for uid in out:
        if uid not in seen:
            uniq.append(uid); seen.add(uid)
    return uniq


            # ---------------------------
            # Configuration & Presets
# Founder-only, DM everyone in queue; self-backup; auto-open; reminders; survey
# ---------------------------

class ConfirmView(discord.ui.View):
    def __init__(self, mid: int, uid: int):
        super().__init__(timeout=None)
        self.mid = mid
        self.uid = uid

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success, custom_id="confirm_yes")
    async def yes(self, interaction: discord.Interaction, button: discord.ui.Button):  # type: ignore[override]
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
    async def no(self, interaction: discord.Interaction, button: discord.ui.Button):  # type: ignore[override]
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
    if not data:
        return
    channel_id = data.get("channel_id")
    if not channel_id:
        return
    try:
        ch = bot.get_channel(int(channel_id)) or await bot.fetch_channel(int(channel_id))
        msg = await ch.fetch_message(int(message_id))
        embed, _ = await _render_event_embed(guild, data["activity"], data)  # type: ignore
        await msg.edit(embed=embed)
    except Exception as e:
        print("Failed to update schedule msg:", e)


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
                            f"📣 **{data['activity']}** starts soon. We still need players.\n"
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
            # Wait roughly one minute between checks
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
                    f"Thanks for running **{activity}**! We'd love your feedback.\n"
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


@bot.tree.command(name="schedule", description="(Founder) Create event: DM everyone in queue; backups once full; reminders; LFG announce")
@founder_only()
@app_commands.describe(
    activity="Activity name",
    datetime_str="Date and time (MM-DD HH:MM, 24h)",
    timezone="Timezone (dropdown)",
    reserved_sherpas="Number of Sherpa slots to reserve (default 2)",
    sherpas="User(s) to pre-slot as Sherpa (optional)",
    participants="User(s) to pre-slot as Participant (optional)",
)
@app_commands.autocomplete(activity=_activity_autocomplete)
@app_commands.choices(
    timezone=[
        app_commands.Choice(name="US Eastern", value="America/New_York"),
        app_commands.Choice(name="US Central", value="America/Chicago"),
        app_commands.Choice(name="US Mountain", value="America/Denver"),
        app_commands.Choice(name="US Pacific", value="America/Los_Angeles"),
        app_commands.Choice(name="UTC", value="UTC"),
        app_commands.Choice(name="Europe/London", value="Europe/London"),
        app_commands.Choice(name="Europe/Paris", value="Europe/Paris"),
        app_commands.Choice(name="Asia/Tokyo", value="Asia/Tokyo"),
    ]
)
async def schedule_cmd(
    interaction: discord.Interaction,
    activity: str,
    datetime_str: str,
    timezone: str = "America/New_York",
    reserved_sherpas: Optional[int] = 2,
    sherpas: Optional[str] = None,
    participants: Optional[str] = None,
):
    # Defer early to avoid interaction timeout while we do DMs and posting
    try:
        await interaction.response.defer(ephemeral=True)
    except Exception:
        # If already acknowledged, ignore
        pass

    try:
        if activity not in ALL_ACTIVITIES:
            await interaction.followup.send("Unknown activity.", ephemeral=True)
            return

        cap = _cap_for_activity(activity)
        reserved = max(0, min(int(reserved_sherpas or 0), cap))

        q = QUEUES.get(activity, [])
        candidates = list(q)  # DM **everyone** in the queue

        # Parse datetime_str and timezone (MM-DD HH:MM)
        try:
            date_part, time_part = datetime_str.strip().split()
            # Use current year
            now = datetime.now()
            year = now.year
            date_full = f"{year}-{date_part}"
        except Exception:
            await interaction.followup.send("Invalid datetime format. Use MM-DD HH:MM.", ephemeral=True)
            return
        start_ts = _parse_date_time_to_epoch(date_full, time_part, tz_name=timezone)

        # Parse pre-slotted users using helper (handles mentions and names)
        guild = interaction.guild
        sherpa_ids = set(_parse_user_ids(sherpas or "", guild)) if sherpas else set()
        participant_ids = _parse_user_ids(participants or "", guild) if participants else []

        # Ensure the scheduling user takes one participant slot
        promoter_id = interaction.user.id
        if promoter_id not in participant_ids:
            participant_ids.insert(0, promoter_id)

        # Compose when_text for embed
        when_text = f"<t:{start_ts}:F> ({timezone})" if start_ts else "TBD"

        # Split participants into actual players (up to available player slots) and backups
        player_slots = max(0, cap - reserved)
        # Dedupe while preserving order
        seen = set(); uniq_participants: List[int] = []
        for uid in participant_ids:
            if uid not in seen:
                uniq_participants.append(uid); seen.add(uid)
        players_final = uniq_participants[:player_slots]
        backups_final = uniq_participants[player_slots:]

        # Determine the channel to post the event in. Prefer configured env vars,
        # otherwise fall back to the channel where the command was invoked.
        channel_id = GENERAL_CHANNEL_ID or RAID_QUEUE_CHANNEL_ID
        if not channel_id and interaction.channel:
            try:
                channel_id = int(interaction.channel.id)
            except Exception:
                channel_id = None

        data = {
            "guild_id": guild.id if guild else None,
            "activity": activity,
            "desc": f"Scheduled by {interaction.user.mention}. Check your DMs to confirm.",
            "when_text": when_text,
            "capacity": cap,
            "reserved_sherpas": reserved,
            "sherpas": sherpa_ids,
            "sherpa_backup": set(),
            "candidates": candidates,
            "players": players_final,           # participants (confirmed)
            "backups": backups_final,           # pre-slotted extras become backups
            "signups_open": False,
            "channel_id": channel_id,
            "start_ts": start_ts,
            "r_2h": False, "r_30m": False, "r_0m": False,
        }

        # Post event embed
        embed, f = await _render_event_embed(guild, activity, data)
        ev_msg = await _send_to_channel_id(data["channel_id"], embed=embed, file=f)
        if not ev_msg:
            attempted = data.get("channel_id")
            await interaction.followup.send(
                f"Failed to post event — no channel available (attempted: {attempted}). Set GENERAL_CHANNEL_ID or RAID_QUEUE_CHANNEL_ID, or run this command in a channel.",
                ephemeral=True,
            )
            return

        mid = ev_msg.id
        SCHEDULES[mid] = data

        # DM **all** queue members with Confirm button
        sent = 0
        for uid in candidates:
            try:
                m = guild.get_member(uid) if guild else None
                if not m:
                    continue
                dm = await m.create_dm()
                await dm.send(
                    content=(
                        f"You've been selected for **{activity}** at **{when_text}** in {guild.name if guild else 'server'}.\n"
                        f"Tap **Confirm** to lock your spot."
                    ),
                    view=ConfirmView(mid=mid, uid=uid),
                )
                sent += 1
            except Exception as e:
                print("DM failed:", e)

        # DM pre-slotted participants immediately (if any) with confirmation
        pre_dmed = set(candidates)  # already attempted DMs to queue candidates
        p_sent = 0
        for uid in data.get("players", []) or []:
            try:
                if uid in pre_dmed:
                    continue
                m = guild.get_member(uid) if guild else None
                if not m:
                    continue
                dm = await m.create_dm()
                # include jump link to the event post if available
                event_link = ev_msg.jump_url if ev_msg else None
                content = (
                    f"You're confirmed for **{activity}** at **{when_text}** in {guild.name if guild else 'server'}."
                    + (f"\nView the event: {event_link}" if event_link else "")
                )
                await dm.send(content=content)
                p_sent += 1
            except Exception as e:
                print("Pre-slot DM failed:", e)

        # Sherpa alert (first R get slots; extras go to backup)
        if GENERAL_SHERPA_CHANNEL_ID and reserved > 0:
            alert = await _send_to_channel_id(
                GENERAL_SHERPA_CHANNEL_ID,
                content=(
                    f"🧭 **Sherpa Alert:** {activity} at **{when_text}**. "
                    f"{reserved} reserved Sherpa slot(s). React ✅ to claim."
                ),
            )
            if alert:
                SCHEDULES[mid]["sherpa_alert_channel_id"] = str(alert.channel.id)
                SCHEDULES[mid]["sherpa_alert_message_id"] = str(alert.id)
                try:
                    await alert.add_reaction("✅")
                except Exception:
                    pass

        # Final acknowledge to the command invoker
        await interaction.followup.send(
            f"Scheduled **{activity}**. DMed {sent} queue member(s), notified {p_sent} pre-slotted participant(s).",
            ephemeral=True,
        )

    except Exception as e:
        print("/schedule command error:", e)
        try:
            # Prefer followup since we deferred earlier
            await interaction.followup.send("An error occurred while scheduling the event. Check the bot logs.", ephemeral=True)
        except Exception:
            try:
                await interaction.response.send_message("An error occurred while scheduling the event. Check the bot logs.", ephemeral=True)
            except Exception:
                pass
        return


# (open_signups command removed — schedule now auto-opens via scheduler loop 2h before start if slots remain)


# ---------------------------
# Reactions
# ---------------------------

@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if bot.user and payload.user_id == bot.user.id:
        return
    # Sherpa alert claim
    for mid, data in list(SCHEDULES.items()):
        alert_id = int(data.get("sherpa_alert_message_id")) if data.get("sherpa_alert_message_id") else None
        if alert_id and payload.message_id == alert_id and str(payload.emoji) == "✅":
            guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
            if not guild:
                return
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


# ---------------------------
# Boot
# ---------------------------

if __name__ == "__main__":
    token = get_token("DISCORD_TOKEN")
    bot.run(token)
