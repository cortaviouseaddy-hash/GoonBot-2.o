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

# Explicit activity -> image map (lowercase keys). Add more mappings as needed.
# Example: map "desert perpetual" to assets/raids/desert_perpetual.jpg
ACTIVITY_IMAGE_MAP: Dict[str, str] = {
    # If you have a real desert_perpetual.jpg, replace the path below.
    # Temporarily map to an available raid image so the embed shows something.
    "desert perpetual": os.path.join(os.path.dirname(__file__), "assets", "raids", "salvations_edge.jpg"),
}

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


def _activity_color(activity: str) -> int:
    """Pick an embed color based on activity category or name tokens."""
    a = (activity or "").lower()
    # Category-based colors
    try:
        # If activity is in presets categories, pick category color
        for key, items in PRESETS.items():
            if activity in items:
                if key == "raids":
                    return 0xE6B500  # gold
                if key == "dungeons":
                    return 0x8A2BE2  # purple
                if key == "exotic_activities":
                    return 0x00CED1  # dark turquoise
    except Exception:
        pass
    # Keyword heuristics
    if any(k in a for k in ("raid", "vault", "wish", "garden", "crota")):
        return 0xE6B500
    if any(k in a for k in ("dungeon", "pit", "crypt", "deep", "spire")):
        return 0x8A2BE2
    # Default neutral color
    return 0x2F3136

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
    # Enhanced embed renderer: adds image (if available) and lists promoter, sherpas, players, backups
    title = f"{activity} — Event"
    desc = data.get("desc", "")
    embed = discord.Embed(title=title, description=desc, color=_activity_color(activity))
    when = data.get("when_text")
    embed.add_field(name="When", value=when or "TBD", inline=False)
    cap = data.get("capacity")
    embed.add_field(name="Capacity", value=str(cap), inline=True)

    # Promoter
    promoter_id = data.get("promoter_id")
    if promoter_id:
        embed.add_field(name="Scheduled by", value=f"<@{promoter_id}>", inline=True)
        # Set promoter avatar as thumbnail if possible
        try:
            member = guild.get_member(int(promoter_id)) if guild and promoter_id else None
            if member and member.avatar:
                embed.set_thumbnail(url=member.avatar.url)
        except Exception:
            pass

    # Sherpas
    sherpas = data.get("sherpas") or set()
    if sherpas:
        try:
            s_list = [f"<@{int(x)}>" for x in (list(sherpas)[:10])]
            embed.add_field(name="Sherpas", value=", ".join(s_list), inline=False)
        except Exception:
            pass
    # Sherpa backups
    s_backups = data.get("sherpa_backup") or set()
    if s_backups:
        try:
            sb_list = [f"<@{int(x)}>" for x in (list(s_backups)[:10])]
            embed.add_field(name=f"Sherpa Backups ({len(s_backups)})", value="\n".join(sb_list), inline=False)
        except Exception:
            pass

    # Players and backups
    players = data.get("players", []) or []
    backups = data.get("backups", []) or []
    if players:
        p_lines = [f"<@{p}>" for p in players]
        embed.add_field(name=f"Players ({len(players)})", value="\n".join(p_lines), inline=False)
    if backups:
        b_lines = [f"<@{b}>" for b in backups]
        embed.add_field(name=f"Backups ({len(backups)})", value="\n".join(b_lines), inline=False)

    # If there's an explicit map, prefer that image; otherwise attempt fuzzy search
    mapped = ACTIVITY_IMAGE_MAP.get(activity.lower()) if activity else None
    img = mapped or _find_activity_image(activity)
    file = None
    if img:
        try:
            filename = os.path.basename(img)
            file = discord.File(img, filename=filename)
            embed.set_image(url=f"attachment://{filename}")
        except Exception:
            file = None

    return embed, file


def _find_activity_image(activity: str) -> Optional[str]:
    # Explicit mapping for Desert Perpetual and fallback to fuzzy search
    ACTIVITY_IMAGE_MAP = {
        "Desert Perpetual": "assets/raids/desert_perpetual.jpg",
        # Add more mappings as needed
    }
    aset = os.path.join(os.path.dirname(__file__), "assets")
    # Direct mapping first
    img_path = ACTIVITY_IMAGE_MAP.get(activity)
    if img_path:
        abs_path = os.path.join(os.path.dirname(__file__), img_path) if not os.path.isabs(img_path) else img_path
        if os.path.isfile(abs_path):
            return abs_path
    # Fallback: fuzzy search
    if not os.path.isdir(aset):
        return None
    activity_key = ''.join(ch.lower() for ch in activity if ch.isalnum() or ch.isspace()).strip()
    tokens = [t for t in activity_key.split() if t]
    best = None
    best_score = 0
    for root, _, files in os.walk(aset):
        for fn in files:
            name = os.path.splitext(fn)[0].lower()
            score = sum(1 for t in tokens if t in name)
            if score > best_score:
                best_score = score
                best = os.path.join(root, fn)
    return best if best_score > 0 else None

# Helper to read integer env vars with multiple fallbacks
def _env_int(*names) -> Optional[int]:
    for n in names:
        v = os.getenv(n)
        if v:
            try:
                return int(v)
            except Exception:
                try:
                    # sometimes values are stored as quoted strings
                    return int(v.strip())
                except Exception:
                    return None
    return None

# Channel IDs from env (accept several possible env names used in hosting dashboards)
GENERAL_CHANNEL_ID = _env_int("GENERAL_CHANNEL_ID", "GENERAL", "general")
RAID_QUEUE_CHANNEL_ID = _env_int("RAID_QUEUE_CHANNEL_ID", "RAID_QUEUE", "raid_queue", "RAID_QUEUE")
GENERAL_SHERPA_CHANNEL_ID = _env_int("GENERAL_SHERPA_CHANNEL_ID", "GENERAL_SHERPA", "general_sherpa", "GENERAL_SHERPA_CHANNEL")
# LFG/announce channel — try a few common names the dashboard might use
LFG_CHAT_CHANNEL_ID = _env_int("LFG_CHAT_CHANNEL_ID", "RAID_SIGN_UP", "RAID_DUNGEON_EVENT_SIGNUP", "RAID_SIGNUP", "raid_sign_up")

# Separate channel for sherpa claims (the "raid-sign-up" channel in your layout)
RAID_SIGN_UP_CHANNEL_ID = _env_int("RAID_SIGN_UP_CHANNEL", "RAID_SIGN_UP", "RAID_SIGNUP_CHANNEL", "RAID_SIGN_UP")

# Optional role ID to ping when posting sherpa alerts
SHERPA_ROLE_ID = os.getenv("SHERPA_ROLE_ID") or os.getenv("SHERPA_ROLE")

# Optional guild id
GUILD_ID = _env_int("GUILD_ID", "GUILD")

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


class SherpaConfirmView(discord.ui.View):
    def __init__(self, mid: int, uid: int):
        super().__init__(timeout=None)
        self.mid = mid
        self.uid = uid

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success, custom_id="sherpa_confirm_yes")
    async def yes(self, interaction: discord.Interaction, button: discord.ui.Button):  # type: ignore[override]
        if interaction.user.id != self.uid:
            await interaction.response.send_message("This DM button isn't for you.", ephemeral=True)
            return
        data = SCHEDULES.get(self.mid)
        if not data:
            await interaction.response.send_message("Event no longer exists.", ephemeral=True)
            return
        sherpas: Set[int] = data.get("sherpas") or set()  # type: ignore
        sbackup: Set[int] = data.get("sherpa_backup") or set()  # type: ignore
        reserved = int(data.get("reserved_sherpas", 0))
        if self.uid in sherpas:
            await interaction.response.send_message("You're already locked in as a Sherpa.", ephemeral=True)
            return
        if len(sherpas) < reserved:
            sherpas.add(self.uid)
            data["sherpas"] = sherpas
            await interaction.response.send_message("Locked in as Sherpa. Thank you! ✅", ephemeral=True)
        else:
            if self.uid not in sbackup:
                sbackup.add(self.uid)
                data["sherpa_backup"] = sbackup
            await interaction.response.send_message("All Sherpa slots are full — you've been added as Sherpa Backup.", ephemeral=True)
        guild = interaction.client.get_guild(data.get("guild_id"))  # type: ignore
        if guild:
            await _update_schedule_message(guild, self.mid)

    @discord.ui.button(label="Can't make it", style=discord.ButtonStyle.secondary, custom_id="sherpa_confirm_no")
    async def no(self, interaction: discord.Interaction, button: discord.ui.Button):  # type: ignore[override]
        if interaction.user.id != self.uid:
            await interaction.response.send_message("This DM button isn't for you.", ephemeral=True)
            return
        data = SCHEDULES.get(self.mid)
        if data:
            sherpas: Set[int] = data.get("sherpas") or set()  # type: ignore
            sbackup: Set[int] = data.get("sherpa_backup") or set()  # type: ignore
            if self.uid in sherpas:
                sherpas.discard(self.uid)
            if self.uid in sbackup:
                sbackup.discard(self.uid)
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
    moved: List[int] = []
    while len(participants) < player_slots and backups:
        nxt = backups.pop(0)
        if nxt not in participants:
            participants.append(nxt)
            moved.append(nxt)
    return moved


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
                    moved = _autofill_from_backups(data)
                    await _send_to_channel_id(
                        LFG_CHAT_CHANNEL_ID or GENERAL_CHANNEL_ID,
                        content=(
                            f"📣 **{data['activity']}** starts soon. We still need players.\n"
                            f"👉 Go to the **event signup post** and react there to join. (Reactions **here** won't count.)"
                        ),
                    )
                    # Try to add a public ✅ reaction to the event message so people can react to join
                    try:
                        ch = None
                        if data.get("channel_id"):
                            try:
                                ch = bot.get_channel(int(data.get("channel_id"))) or await bot.fetch_channel(int(data.get("channel_id")))
                            except Exception:
                                ch = None
                        if ch:
                            try:
                                msg = await ch.fetch_message(int(mid))
                                # Add join, backup, and leave reactions so members can interact
                                for emoji in ("✅", "📝", "❌"):
                                    try:
                                        await msg.add_reaction(emoji)
                                    except Exception:
                                        pass
                            except Exception:
                                # Could be missing permissions or message deleted; ignore
                                pass
                    except Exception:
                        pass
                    # DM any backups that were pulled up to players
                    try:
                        guild = bot.get_guild(data.get("guild_id")) if data.get("guild_id") else None
                        if guild and moved:
                            for uid in moved:
                                try:
                                    member = guild.get_member(uid)
                                    if member:
                                        d = await member.create_dm()
                                        await d.send(f"You've been promoted from Backup to Player for **{data.get('activity')}** at {data.get('when_text')}. See you there! ✅")
                                except Exception:
                                    pass
                        if guild:
                            await _update_schedule_message(guild, mid)
                    except Exception:
                        pass

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

        # Sherpas visually separate but should count toward player slots.
        # Merge sherpas into the participant order (preserve provided participant order,
        # then append any sherpas not already listed) so they consume player slots.
        merged_participants = list(participant_ids)
        for sid in list(sherpa_ids):
            if sid not in merged_participants:
                merged_participants.append(sid)

        # Compose when_text for embed
        when_text = f"<t:{start_ts}:F> ({timezone})" if start_ts else "TBD"

        # Split participants into actual players (up to available player slots) and backups
        player_slots = max(0, cap - reserved)
        # Dedupe merged participants while preserving order
        seen = set(); uniq_participants: List[int] = []
        for uid in merged_participants:
            if uid not in seen:
                uniq_participants.append(uid); seen.add(uid)
        players_final = uniq_participants[:player_slots]
        backups_final = uniq_participants[player_slots:]

        # Determine the channel to post the main event embed in.
        # Prefer the raid/dungeon event signup (LFG) channel, then the raid queue channel,
        # then the general channel; finally fall back to the channel where the command was invoked.
        channel_id = LFG_CHAT_CHANNEL_ID or RAID_QUEUE_CHANNEL_ID or GENERAL_CHANNEL_ID
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
            "promoter_id": promoter_id,
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

        # Add immediate reactions for Backup and Leave only. Join (✅) is added later when signups_open = True
        try:
            try:
                await ev_msg.add_reaction("📝")
            except Exception:
                pass
            try:
                await ev_msg.add_reaction("❌")
            except Exception:
                pass
        except Exception:
            pass

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

        # Sherpa alert (first R get slots; extras go to backup) — post as an embed so sherpas can sign up
        # Post the sherpa alert in the raid-sign-up channel if available, otherwise fallback to the general sherpa channel
        sherpa_post_channel = RAID_SIGN_UP_CHANNEL_ID or GENERAL_SHERPA_CHANNEL_ID
        if sherpa_post_channel and reserved > 0:
            try:
                alert_embed = discord.Embed(
                    title=f"🧭 Sherpa Alert — {activity}",
                    description=(f"{reserved} reserved Sherpa slot(s). React ✅ to claim your slot."),
                    color=_activity_color(activity),
                )
                alert_embed.add_field(name="When", value=when_text, inline=True)
                alert_embed.add_field(name="Reserved Sherpas", value=str(reserved), inline=True)
                # If we have the event message, include a jump link
                try:
                    if ev_msg:
                        alert_embed.add_field(name="Event", value=f"[Jump to event]({ev_msg.jump_url})", inline=False)
                except Exception:
                    pass

                # Attach activity image to sherpa embed if available
                img_path = _find_activity_image(activity)
                alert_file = None
                if img_path:
                    try:
                        filename = os.path.basename(img_path)
                        alert_file = discord.File(img_path, filename=filename)
                        alert_embed.set_image(url=f"attachment://{filename}")
                    except Exception:
                        alert_file = None

                # Prepend a role ping if configured
                ping_text = f"<@&{SHERPA_ROLE_ID}>\n" if SHERPA_ROLE_ID else None
                alert = await _send_to_channel_id(sherpa_post_channel, content=ping_text, embed=alert_embed, file=alert_file)
                if alert:
                    SCHEDULES[mid]["sherpa_alert_channel_id"] = str(alert.channel.id)
                    SCHEDULES[mid]["sherpa_alert_message_id"] = str(alert.id)
                    try:
                        await alert.add_reaction("✅")
                    except Exception:
                        pass
            except Exception:
                # If embed send fails, fall back to the plain text alert
                try:
                    ping_text = f"<@&{SHERPA_ROLE_ID}>\n" if SHERPA_ROLE_ID else None
                    alert = await _send_to_channel_id(
                        sherpa_post_channel,
                        content=(ping_text or "") + (
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
        alert_ch = int(data.get("sherpa_alert_channel_id")) if data.get("sherpa_alert_channel_id") else None
        # Only accept sherpa claims when the reaction is on the stored sherpa alert message in the alert channel
        if alert_id and payload.message_id == alert_id and str(payload.emoji) == "✅" and (alert_ch is None or payload.channel_id == alert_ch):
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
            # DM the claiming sherpa with a confirmation view so they can lock in or decline.
            try:
                dm = await member.create_dm()
                event_link = None
                try:
                    ch_id = int(data.get("channel_id")) if data.get("channel_id") else None
                    if ch_id:
                        ch = bot.get_channel(ch_id) or await bot.fetch_channel(ch_id)
                        msg = await ch.fetch_message(int(mid)) if ch else None
                        event_link = msg.jump_url if msg else None
                except Exception:
                    event_link = None
                when_text = data.get("when_text")
                activity = data.get("activity")
                content = (
                    f"You've claimed a Sherpa slot for **{activity}** at **{when_text}**.\n"
                    + (f"View event: {event_link}\n" if event_link else "")
                    + "Tap **Confirm** to lock your Sherpa slot."
                )
                await dm.send(content=content, view=ConfirmView(mid=mid, uid=member.id))
            except Exception:
                pass
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

    # ✅ behavior: before signups open, treat as Backup (user intends to backup). After signups_open=True, ✅ is the join action.
    if str(payload.emoji) == "✅":
        data = SCHEDULES.get(payload.message_id)
        if not data:
            return
        guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
        if not guild:
            return
        participants: List[int] = data.get("players", [])  # type: ignore
        backups: List[int] = data.get("backups", [])  # type: ignore
        # If signups aren't open yet, treat ✅ as a backup signup
        if not data.get("signups_open"):
            if payload.user_id not in participants and payload.user_id not in backups:
                backups.append(payload.user_id)
            await _update_schedule_message(guild, payload.message_id)
            return

        # Signups are open: treat ✅ as join (player if space, otherwise backup)
        cap = int(data.get("capacity", 0))
        reserved = int(data.get("reserved_sherpas", 0))
        player_slots = max(0, cap - reserved)
        if payload.user_id in participants or payload.user_id in backups:
            # already signed up
            await _update_schedule_message(guild, payload.message_id)
            return
        if len(participants) < player_slots:
            participants.append(payload.user_id)
        else:
            backups.append(payload.user_id)
        await _update_schedule_message(guild, payload.message_id)
        return

    # Leave reaction: ❌ removes the user from players/backups
    if str(payload.emoji) == "❌":
        data = SCHEDULES.get(payload.message_id)
        if not data:
            return
        guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
        if not guild:
            return
        participants: List[int] = data.get("players", [])  # type: ignore
        backups: List[int] = data.get("backups", [])  # type: ignore
        removed = False
        if payload.user_id in participants:
            participants[:] = [x for x in participants if x != payload.user_id]
            removed = True
            _autofill_from_backups(data)
        if payload.user_id in backups:
            backups[:] = [x for x in backups if x != payload.user_id]
            removed = True
        if removed:
            await _update_schedule_message(guild, payload.message_id)
        return


@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    # Removing reactions: handle ✅ differently depending on whether signups are open.
    data = SCHEDULES.get(payload.message_id)
    if not data:
        return
    guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
    if not guild:
        return

    # ✅ removed
    if str(payload.emoji) == "✅":
        # If signups are open, removing ✅ frees a player slot and triggers autofill
        if data.get("signups_open"):
            participants: List[int] = data.get("players", [])  # type: ignore
            if payload.user_id in participants:
                participants[:] = [x for x in participants if x != payload.user_id]
                _autofill_from_backups(data)
                await _update_schedule_message(guild, payload.message_id)
        else:
            # Before signups open, ✅ was treated as a backup — removing it removes from backups
            backups: List[int] = data.get("backups", [])  # type: ignore
            if payload.user_id in backups:
                backups[:] = [x for x in backups if x != payload.user_id]
                await _update_schedule_message(guild, payload.message_id)
        return

    # For other emojis we don't need to special-case here (📝 handled on add; ❌ handled on add)
    return


# ---------------------------
# Boot
# ---------------------------

if __name__ == "__main__":
    token = get_token("DISCORD_TOKEN")
    bot.run(token)
