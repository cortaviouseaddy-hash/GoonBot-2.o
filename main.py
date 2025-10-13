# GoonBot main.py — queues, check-in, promotions, scheduling
# Exact behavior:
# - Main Event Embed -> EVENT_SIGNUP_CHANNEL_ID (aka RAID_DUNGEON_EVENT_SIGNUP_CHANNEL_ID)
# - Sherpa Signup Embed -> RAID_SIGN_UP_CHANNEL_ID (✅ to claim Sherpa; overflow -> Sherpa Backup)
# - Sherpa Announcement -> GENERAL_SHERPA_CHANNEL_ID (pings SHERPA_ROLE_ID if set; points to Sherpa signup post)
# - T-2h before start (if player slots remain): add ✅ to main embed + single LFG nudge in LFG_CHAT_CHANNEL_ID
# - DM the entire queue with Confirm buttons; confirming joins as participant; no response = nothing
# - Colors based on category; optional activity images from ./assets/** by fuzzy filename match
# - Reminders at T-2h, T-30m, and start; survey DM 3h after start

import os
import asyncio
import json
from datetime import datetime, timedelta
import datetime as datetime_module
from typing import Dict, List, Optional, Set, Tuple

import discord
from discord import app_commands
from discord.ext import commands

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None

# ---------------------------
# Config & Environment
# ---------------------------

def _env_int(*names) -> Optional[int]:
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip() != "":
            try:
                return int(str(v).strip())
            except Exception:
                return None
    return None

def _env_bool(name: str, default: bool = True) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "on")

GENERAL_CHANNEL_ID            = _env_int("GENERAL_CHANNEL_ID")
WELCOME_CHANNEL_ID            = _env_int("WELCOME_CHANNEL_ID")
GENERAL_SHERPA_CHANNEL_ID     = _env_int("GENERAL_SHERPA_CHANNEL_ID")
LFG_CHAT_CHANNEL_ID           = _env_int("LFG_CHAT_CHANNEL_ID")
RAID_QUEUE_CHANNEL_ID         = _env_int("RAID_QUEUE_CHANNEL_ID")
RAID_SIGN_UP_CHANNEL_ID       = _env_int("RAID_SIGN_UP_CHANNEL_ID")  # Sherpa signup channel
SHERPA_ASSISTANT_ROLE_ID      = _env_int("SHERPA_ASSISTANT_ROLE_ID")
SHERPA_ROLE_ID                = _env_int("SHERPA_ROLE_ID")
EVENT_SIGNUP_CHANNEL_ID       = _env_int("RAID_DUNGEON_EVENT_SIGNUP_CHANNEL_ID", "EVENT_SIGNUP_CHANNEL_ID")  # Main event embed
EVENT_HOST_AUTOJOIN           = _env_bool("EVENT_HOST_AUTOJOIN", True)

# Optional local overrides via channel_ids.json (non-secret, deploy-time config)
def _load_channel_overrides() -> None:
    try:
        cfg_path = os.path.join(os.path.dirname(__file__), "channel_ids.json")
        if not os.path.isfile(cfg_path):
            return
        with open(cfg_path, "r") as f:
            data = json.load(f)
        def _to_int(v):
            try:
                return int(str(v).strip())
            except Exception:
                return None
        global GENERAL_SHERPA_CHANNEL_ID, RAID_SIGN_UP_CHANNEL_ID, GENERAL_CHANNEL_ID, LFG_CHAT_CHANNEL_ID, RAID_QUEUE_CHANNEL_ID, EVENT_SIGNUP_CHANNEL_ID, WELCOME_CHANNEL_ID
        gs = _to_int(data.get("GENERAL_SHERPA_CHANNEL_ID"))
        rs = _to_int(data.get("RAID_SIGN_UP_CHANNEL_ID"))
        gc = _to_int(data.get("GENERAL_CHANNEL_ID"))
        lf = _to_int(data.get("LFG_CHAT_CHANNEL_ID"))
        rq = _to_int(data.get("RAID_QUEUE_CHANNEL_ID"))
        ev = _to_int(data.get("EVENT_SIGNUP_CHANNEL_ID")) or _to_int(data.get("RAID_DUNGEON_EVENT_SIGNUP_CHANNEL_ID"))
        wc = _to_int(data.get("WELCOME_CHANNEL_ID"))
        if gs and not GENERAL_SHERPA_CHANNEL_ID:
            GENERAL_SHERPA_CHANNEL_ID = gs
        if rs and not RAID_SIGN_UP_CHANNEL_ID:
            RAID_SIGN_UP_CHANNEL_ID = rs
        if gc and not GENERAL_CHANNEL_ID:
            GENERAL_CHANNEL_ID = gc
        if lf and not LFG_CHAT_CHANNEL_ID:
            LFG_CHAT_CHANNEL_ID = lf
        if rq and not RAID_QUEUE_CHANNEL_ID:
            RAID_QUEUE_CHANNEL_ID = rq
        if ev and not EVENT_SIGNUP_CHANNEL_ID:
            EVENT_SIGNUP_CHANNEL_ID = ev
        if wc and not WELCOME_CHANNEL_ID:
            WELCOME_CHANNEL_ID = wc
    except Exception:
        pass

_load_channel_overrides()

# ---------------------------
# Data directory (durable storage)
# ---------------------------
def _ensure_dir(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass

# Prefer explicit env var; fall back to ./data alongside this file
DATA_DIR = (
    os.getenv("GOONBOT_DATA_DIR")
    or os.getenv("BOT_DATA_DIR")
    or os.path.join(os.path.dirname(__file__), "data")
)
_ensure_dir(DATA_DIR)

FOUNDER_USER_ID               = os.getenv("FOUNDER_USER_ID")  # str
ALLOW_ASSISTANTS_TO_HOST      = os.getenv("ALLOW_ASSISTANTS_TO_HOST", "1").strip() not in ("0", "false", "no")

# ---------------------------
# Intents & Bot
# ---------------------------

INTENTS = discord.Intents.default()
INTENTS.members = True
INTENTS.message_content = True

bot = commands.Bot(command_prefix="!", intents=INTENTS)

# ---------------------------
# Data Stores
# ---------------------------

SCHEDULES: Dict[int, Dict[str, object]] = {}
QUEUES: Dict[str, List[int]] = {}
CHECKED: Dict[str, Set[int]] = {}

# ---------------------------
# External Helpers (project)
# ---------------------------
from presets_loader import load_presets
from env_safety import get_token

try:
    PRESETS = load_presets() or {}
except Exception:
    PRESETS = {}

ALL_ACTIVITIES: List[str] = []
for v in PRESETS.values():
    if isinstance(v, list):
        ALL_ACTIVITIES.extend(v)

# ---------------------------
# Utilities
# ---------------------------

def _normalize_activity_text(text: Optional[str]) -> str:
    base = ''.join((ch.lower() if (ch.isalnum() or ch.isspace()) else ' ') for ch in (text or ""))
    return ' '.join(base.split())

def _resolve_activity(user_input: Optional[str], pool: Optional[List[str]] = None) -> Tuple[Optional[str], List[str]]:
    if not user_input:
        return None, []
    candidates = pool or ALL_ACTIVITIES
    # Exact match first
    if user_input in candidates:
        return user_input, []
    norm_in = _normalize_activity_text(user_input)
    normalized_map: List[Tuple[str, str]] = [(act, _normalize_activity_text(act)) for act in candidates]

    # Exact normalized match
    exact_norm = [act for act, norm in normalized_map if norm == norm_in]
    if len(exact_norm) == 1:
        return exact_norm[0], []

    # Unique substring on normalized text
    subs_norm = [act for act, norm in normalized_map if norm_in and norm_in in norm]
    if len(subs_norm) == 1:
        return subs_norm[0], []

    # Unique substring on raw, case-insensitive
    low_in = (user_input or "").lower()
    subs_raw = [act for act in candidates if low_in and low_in in act.lower()]
    if len(subs_raw) == 1:
        return subs_raw[0], []

    # Suggestions (top up to 5 from best candidate list)
    suggestions = subs_norm[:5] if subs_norm else subs_raw[:5]
    return None, suggestions

def _ensure_queue(activity: str) -> List[int]:
    return QUEUES.setdefault(activity, [])

def _ensure_checked(activity: str) -> Set[int]:
    return CHECKED.setdefault(activity, set())

def _cap_for_activity(activity: str) -> int:
    a = (activity or "").lower()
    if any(k in a for k in ("raid", "vault", "wish", "garden", "crota", "salvation")): return 6
    if any(k in a for k in ("dungeon", "pit", "crypt", "deep", "spire")): return 3
    return 6

def _is_sherpa(member: discord.Member) -> bool:
    try:
        return any(r.name.lower().startswith("sherpa") for r in member.roles)
    except Exception:
        return False

def _is_sherpa_assistant(member: discord.Member) -> bool:
    try:
        if SHERPA_ASSISTANT_ROLE_ID:
            return any(r.id == int(SHERPA_ASSISTANT_ROLE_ID) for r in member.roles)
        return any(r.name.lower() == "sherpa assistant" for r in member.roles)
    except Exception:
        return False

def sherpa_host_only():
    async def predicate(interaction: discord.Interaction) -> bool:
        if interaction.guild is None:
            raise app_commands.CheckFailure("Use this in a server.")
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not member:
            raise app_commands.CheckFailure("Member context required.")
        if _is_sherpa(member):
            return True
        if ALLOW_ASSISTANTS_TO_HOST and _is_sherpa_assistant(member):
            return True
        raise app_commands.CheckFailure("Only Sherpas can use this command." + (" Assistants are not allowed." if not ALLOW_ASSISTANTS_TO_HOST else ""))
    return app_commands.check(predicate)

async def _activity_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    cur = (current or "").lower()
    out: List[app_commands.Choice[str]] = []
    for act in ALL_ACTIVITIES:
        if not cur or cur in act.lower():
            out.append(app_commands.Choice(name=act, value=act))
            if len(out) >= 25:
                break
    return out

def _activity_color(activity: str) -> int:
    a = (activity or "").lower()
    try:
        for key, items in PRESETS.items():
            if activity in items:
                if key == "raids": return 0xE6B500  # gold
                if key == "dungeons": return 0x8A2BE2  # purple
                if key == "exotic_activities": return 0x00CED1  # teal
    except Exception:
        pass
    if any(k in a for k in ("raid", "vault", "wish", "garden", "crota", "salvation")): return 0xE6B500
    if any(k in a for k in ("dungeon", "pit", "crypt", "deep", "spire")): return 0x8A2BE2
    return 0x2F3136  # neutral

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
        try: print("_send_to_channel_id error:", channel_id, e)
        except Exception: pass
        return None

def _can_send_in_channel(guild: Optional[discord.Guild], channel: object) -> bool:
    try:
        if not guild or not channel:
            return False
        me = guild.me
        if not me:
            return False
        # Some channel types (e.g., categories) will not have permissions_for/send
        perms = getattr(channel, "permissions_for", None)
        if not callable(perms):
            return False
        p = channel.permissions_for(me)
        return bool(getattr(p, "send_messages", False))
    except Exception:
        return False

def _resolve_welcome_channel_id(guild: Optional[discord.Guild]) -> Optional[int]:
    """
    Resolve a safe channel id to post the welcome embed:
    1) Use configured WELCOME_CHANNEL_ID or GENERAL_CHANNEL_ID if sendable
    2) Use guild.system_channel if sendable
    3) Prefer common names: welcome, general, introductions, start-here, lounge, chat
    4) Fallback to the first text channel the bot can send in
    """
    try:
        # 1) Configured ids first
        for cid in (WELCOME_CHANNEL_ID, GENERAL_CHANNEL_ID):
            if cid:
                ch = bot.get_channel(int(cid))
                if ch and _can_send_in_channel(guild, ch):
                    return int(cid)
        # 2) System channel
        if guild and guild.system_channel and _can_send_in_channel(guild, guild.system_channel):
            return int(guild.system_channel.id)
        # 3) Preferred names
        preferred_names = (
            "welcome", "welcome-and-rules", "welcome-rules", "rules", "start-here", "get-started",
            "general", "general-chat", "lounge", "chat", "introductions", "introduce-yourself"
        )
        if guild:
            try:
                for name in preferred_names:
                    ch = discord.utils.find(
                        lambda c: isinstance(c, discord.TextChannel) and c.name.lower() == name,
                        getattr(guild, "text_channels", []),
                    )
                    if ch and _can_send_in_channel(guild, ch):
                        return int(ch.id)
            except Exception:
                pass
            # 4) First sendable text channel
            for ch in getattr(guild, "text_channels", []):
                if _can_send_in_channel(guild, ch):
                    return int(ch.id)
    except Exception as e:
        try: print("resolve_welcome_channel error:", e)
        except Exception: pass
    return None

def _find_activity_image(activity: str) -> Optional[str]:
    aset = os.path.join(os.path.dirname(__file__), "assets")
    if not os.path.isdir(aset):
        return None
    activity_key = ''.join(ch.lower() for ch in (activity or "") if ch.isalnum() or ch.isspace()).strip()
    if not activity_key:
        return None
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

def _apply_activity_image(embed: discord.Embed, activity: str) -> Tuple[discord.Embed, Optional[discord.File]]:
    # Known fallbacks for newer activities that may not exist in assets yet
    # Map canonicalized activity names -> local asset path (temporary placeholder)
    FALLBACK_LOCAL_IMAGES = {
        "desert perpetual": os.path.join(os.path.dirname(__file__), "assets", "raids", "Desert_Perpetual.jpeg"),
    }

    img = _find_activity_image(activity)
    file = None
    if not img:
        # Try a simple alias-based fallback (temporary until a proper asset is added)
        key = ''.join(ch.lower() for ch in (activity or "") if ch.isalnum() or ch.isspace()).strip()
        img = FALLBACK_LOCAL_IMAGES.get(key)
        if img and not os.path.isfile(img):
            img = None

    if img:
        try:
            filename = os.path.basename(img)
            file = discord.File(img, filename=filename)
            embed.set_image(url=f"attachment://{filename}")
        except Exception:
            file = None
    return embed, file

# ---------------------------
# Event list + logging helpers
# ---------------------------

# Append-only JSONL file for lightweight debug logs
CONFIRM_LOG_FILE = os.path.join(os.path.dirname(__file__), "confirmations.jsonl")

def _user_in_any_event_list(data: Dict[str, object], uid: int) -> Optional[str]:
    try:
        if uid in (data.get("players", []) or []):
            return "players"
        if uid in (data.get("backups", []) or []):
            return "backups"
        # sherpas and sherpa_backup may be list or set depending on flow
        sherpas = data.get("sherpas") or set()
        if uid in set(sherpas):
            return "sherpas"
        sbackup = data.get("sherpa_backup") or []
        if uid in set(sbackup) or uid in list(sbackup):
            return "sherpa_backup"
        return None
    except Exception:
        return None

def _remove_user_from_list(data: Dict[str, object], uid: int, key: str) -> bool:
    try:
        if key == "sherpas":
            cur = data.get("sherpas") or set()
            before = len(cur)
            try:
                cur.discard(uid)
            except Exception:
                cur = set([x for x in list(cur) if int(x) != int(uid)])
            data["sherpas"] = cur
            return len(cur) != before
        lst = data.get(key) or []
        if isinstance(lst, list):
            new_lst = [x for x in lst if int(x) != int(uid)]
            changed = len(new_lst) != len(lst)
            data[key] = new_lst
            return changed
        else:
            # treat as set
            s = set(lst)
            before = len(s)
            s.discard(uid)
            data[key] = s
            return len(s) != before
    except Exception:
        return False

def _remove_from_all_event_lists(data: Dict[str, object], uid: int) -> None:
    for key in ("players", "backups", "sherpas", "sherpa_backup"):
        _remove_user_from_list(data, uid, key)

def _append_unique_to(data: Dict[str, object], key: str, uid: int) -> Tuple[bool, Optional[str]]:
    """Try to append uid to the given list/set key if uid is not present
    in ANY event list. Returns (added, skip_reason)."""
    exists = _user_in_any_event_list(data, uid)
    if exists and exists != key:
        return False, f"already in {exists}"
    try:
        if key == "sherpas":
            cur = data.get("sherpas") or set()
            if uid in set(cur):
                return False, "already in sherpas"
            cur = set(cur)
            cur.add(uid)
            data["sherpas"] = cur
            return True, None
        cur = data.get(key)
        if isinstance(cur, list):
            if uid in cur:
                return False, f"already in {key}"
            cur.append(uid)
            data[key] = cur
            return True, None
        else:
            s = set(cur or [])
            if uid in s:
                return False, f"already in {key}"
            s.add(uid)
            data[key] = s
            return True, None
    except Exception as e:
        return False, f"error: {e.__class__.__name__}"

def _log_confirmation(mid: int, uid: int, action: str, result: str, reason: Optional[str] = None) -> None:
    record = {
        "mid": int(mid),
        "uid": int(uid),
        "action": action,
        "result": result,
        "reason": reason,
        "ts": int(datetime.now().timestamp()),
    }
    try:
        print("confirm-log:", record)
    except Exception:
        pass
    try:
        with open(CONFIRM_LOG_FILE, "a") as f:
            f.write(json.dumps(record) + "\n")
    except Exception:
        # best-effort; ignore fs errors
        pass

def _parse_date_time_to_epoch(date_iso: str, time_part: str, tz_name: Optional[str] = None) -> Optional[int]:
    try:
        dt = datetime.strptime(f"{date_iso} {time_part}", "%Y-%m-%d %H:%M")
        if tz_name and ZoneInfo:
            try:
                tz = ZoneInfo(tz_name)
                dt = dt.replace(tzinfo=tz)
            except Exception:
                pass
        if dt.tzinfo:
            return int(dt.timestamp())
        if ZoneInfo:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        return int(dt.timestamp())
    except Exception:
        return None

# ---------------------------
# Counter Utilities
# ---------------------------

COUNT_FILE = os.path.join(DATA_DIR, "counts.json")
COUNTER_LOCK = asyncio.Lock()

# Persistent storage for activity queues
QUEUES_FILE = os.path.join(DATA_DIR, "queues.json")
QUEUES_LOCK = asyncio.Lock()

def _read_counter() -> int:
    try:
        with open(COUNT_FILE, "r") as f:
            data = json.load(f)
        value = int(data.get("count", 0))
        return value if value >= 0 else 0
    except Exception:
        return 0

def _write_counter(value: int) -> None:
    try:
        with open(COUNT_FILE, "w") as f:
            json.dump({"count": int(value)}, f)
    except Exception:
        pass

async def _increment_counter() -> int:
    async with COUNTER_LOCK:
        current = _read_counter()
        new_value = current + 1
        _write_counter(new_value)
        return new_value

# ---------------
# Queue persistence
# ---------------
def _read_queues_from_disk() -> Dict[str, List[int]]:
    try:
        # Prefer new data dir path; fall back to legacy file near this module
        path = QUEUES_FILE
        if not os.path.isfile(path):
            legacy = os.path.join(os.path.dirname(__file__), "queues.json")
            if os.path.isfile(legacy):
                path = legacy
            else:
                return {}
        with open(path, "r") as f:
            raw = json.load(f)
        out: Dict[str, List[int]] = {}
        for k, v in (raw or {}).items():
            try:
                name = str(k)
                ids = [int(x) for x in (v or [])]
                out[name] = ids
            except Exception:
                continue
        return out
    except Exception:
        return {}

def _write_queues_to_disk(state: Dict[str, List[int]]) -> None:
    try:
        tmp_path = f"{QUEUES_FILE}.tmp"
        serializable = {str(k): [int(x) for x in (v or [])] for k, v in state.items()}
        # Write atomically and fsync to reduce data loss on crashes
        with open(tmp_path, "w") as f:
            json.dump(serializable, f)
            try:
                f.flush(); os.fsync(f.fileno())
            except Exception:
                pass
        os.replace(tmp_path, QUEUES_FILE)
        # Best-effort fsync the directory entry
        try:
            dir_fd = os.open(os.path.dirname(QUEUES_FILE) or ".", os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except Exception:
            pass
    except Exception as e:
        try:
            print("Queue write failed:", e)
        except Exception:
            pass

async def persist_queues() -> None:
    async with QUEUES_LOCK:
        _write_queues_to_disk(QUEUES)

async def load_queues() -> None:
    async with QUEUES_LOCK:
        loaded = _read_queues_from_disk()
        if loaded:
            # Merge into current to preserve references
            for k, v in loaded.items():
                QUEUES[k] = list(v)


# ---------------------------
# Permissions
# ---------------------------

def founder_only():
    async def predicate(interaction: discord.Interaction) -> bool:
        if interaction.guild is None:
            raise app_commands.CheckFailure("Use this in a server.")
        if not FOUNDER_USER_ID:
            return True
        try:
            if FOUNDER_USER_ID and interaction.user.id == int(FOUNDER_USER_ID):
                return True
        except Exception:
            pass
        if isinstance(interaction.user, discord.Member):
            # fallback by role name in case founder id not set
            if any(r.name.lower() == "founder" for r in interaction.user.roles):
                return True
        raise app_commands.CheckFailure("You are not authorized to use this command.")
    return app_commands.check(predicate)

def _is_promoter_or_founder(interaction: discord.Interaction, data: Optional[Dict[str, object]] = None) -> bool:
    try:
        uid = int(interaction.user.id)
        if FOUNDER_USER_ID and uid == int(FOUNDER_USER_ID):
            return True
        if data and "promoter_id" in data and int(data["promoter_id"]) == uid:
            return True
    except Exception:
        pass
    return False

# ---------------------------
# Embeds
# ---------------------------

async def _render_event_embed(guild: Optional[discord.Guild], activity: str, data: Dict[str, object]) -> Tuple[discord.Embed, Optional[discord.File]]:
    is_user_event = bool(data.get("format") == "user_event")
    desc = str(data.get("desc", "") or "")
    when = data.get("when_text")
    cap = int(data.get("capacity", 0))

    if is_user_event:
        title = f"🗓️ {activity} — {when or 'TBD'}"
    else:
        title = f"{activity} — Event"

    embed = discord.Embed(title=title, description=desc, color=_activity_color(activity))

    if not is_user_event:
        embed.add_field(name="When", value=when or "TBD", inline=False)
        embed.add_field(name="Capacity", value=str(cap), inline=True)

    promoter_id = data.get("promoter_id")
    if promoter_id:
        host_label = "Host" if is_user_event else "Scheduled by"
        embed.add_field(name=host_label, value=f"<@{promoter_id}>", inline=True)
        try:
            member = guild.get_member(int(promoter_id)) if guild and promoter_id else None
            if member and member.avatar:
                embed.set_thumbnail(url=member.avatar.url)
        except Exception:
            pass

    if is_user_event:
        req = int(data.get("requested_sherpas", 0))
        voice_name = data.get("voice_name")
        embed.add_field(name="Capacity", value=str(cap), inline=True)
        embed.add_field(name="Requested Sherpas", value=str(req), inline=True)
        if voice_name:
            embed.add_field(name="Voice", value=str(voice_name), inline=True)

    sherpas: Set[int] = data.get("sherpas") or set()  # type: ignore
    s_backups: Set[int] = data.get("sherpa_backup") or set()  # type: ignore
    players: List[int] = data.get("players", []) or []  # type: ignore
    backups: List[int] = data.get("backups", []) or []  # type: ignore

    if not is_user_event:
        if sherpas:
            embed.add_field(name="Sherpas", value=", ".join(f"<@{int(x)}>" for x in list(sherpas)[:10]), inline=False)
        if s_backups:
            embed.add_field(name=f"Sherpa Backups ({len(s_backups)})", value="\n".join(f"<@{int(x)}>" for x in list(s_backups)[:10]), inline=False)

    if players:
        if is_user_event:
            lines = [f"{i+1}. <@{uid}>" for i, uid in enumerate(players)]
            embed.add_field(name=f"Participants ({len(players)}/{cap})", value="\n".join(lines), inline=False)
        else:
            embed.add_field(name=f"Players ({len(players)})", value="\n".join(f"<@{p}>" for p in players), inline=False)
    if backups:
        if is_user_event:
            embed.add_field(name=f"Backup ({len(backups)})", value="\n".join(f"– <@{b}>" for b in backups), inline=False)
        else:
            embed.add_field(name=f"Backups ({len(backups)})", value="\n".join(f"<@{b}>" for b in backups), inline=False)

    if is_user_event and desc:
        embed.add_field(name="Notes", value=desc, inline=False)

    # Preserve previously uploaded image if known
    try:
        img_url = data.get("image_url")
        if img_url:
            embed.set_image(url=str(img_url))
            return embed, None
    except Exception:
        pass

    # Prefer encounter/preset for image search if provided
    search_text = str(data.get("encounter") or activity)
    embed_with_img, attachment = _apply_activity_image(embed, search_text)
    # If we produced a local file attachment, prefer to not send it as an external upload.
    # We'll set the image via attachment first, then immediately capture Discord's CDN URL and
    # re-render without an attachment (handled by callers).
    return embed_with_img, attachment

def _format_title_when(ts: Optional[int], tz_name: Optional[str]) -> str:
    try:
        if not ts:
            return "TBD"
        dt = datetime.fromtimestamp(int(ts), ZoneInfo(tz_name) if (tz_name and ZoneInfo) else None)
        # Example: Sat Oct 5 @ 7:00 PM (EST)
        day = dt.strftime("%a %b %-d") if os.name != "nt" else dt.strftime("%a %b %#d")
        time_part = dt.strftime("%-I:%M %p") if os.name != "nt" else dt.strftime("%#I:%M %p")
        tz_abbr = dt.tzname() or (tz_name or "UTC")
        return f"{day} @ {time_part} ({tz_abbr})"
    except Exception:
        return "TBD"

async def _render_sherpa_only_embed(guild: Optional[discord.Guild], activity: str, data: Dict[str, object]) -> Tuple[discord.Embed, Optional[discord.File]]:
    title_when = _format_title_when(data.get("start_ts"), data.get("timezone"))
    title = f"🗓️ Sherpa Run — {activity} — {title_when}"
    desc = str(data.get("notes", "") or "")
    embed = discord.Embed(title=title, description=(f"Notes: {desc}" if desc else None), color=_activity_color(activity))
    host_id = data.get("host_id")
    if host_id:
        embed.add_field(name="Host", value=f"<@{int(host_id)}>", inline=True)
    cap = int(data.get("capacity", 0))
    sherpas: Set[int] = data.get("sherpas") or set()  # type: ignore
    embed.add_field(name="Slots", value=f"{len(sherpas)} of {cap} (Sherpa-only)", inline=True)
    # Voice info: prefer explicit voice_name; otherwise try to mention by id; fallback to empty
    voice_name = data.get("voice_name")
    voice_channel_id = data.get("voice_channel_id")
    voice_value = None
    try:
        if voice_name:
            voice_value = str(voice_name)
        elif voice_channel_id:
            voice_value = f"<#{int(voice_channel_id)}>"
    except Exception:
        voice_value = None
    if voice_value:
        embed.add_field(name="Voice", value=voice_value, inline=True)

    # Participants and backup lists
    if sherpas:
        names = [f"<@{int(x)}>" + (" (Host)" if int(x) == int(host_id or 0) else "") for x in sherpas]
        embed.add_field(name=f"Participants ({len(sherpas)}/{cap})", value="\n".join(names), inline=False)
    s_backups: List[int] = list(data.get("sherpa_backup") or [])  # type: ignore
    if s_backups:
        embed.add_field(name=f"Backup ({len(s_backups)})", value="\n".join(f"<@{int(x)}>" for x in s_backups), inline=False)

    # Preserve previously uploaded image if known
    try:
        img_url = data.get("image_url")
        if img_url:
            embed.set_image(url=str(img_url))
            return embed, None
    except Exception:
        pass
    embed_with_img, attachment = _apply_activity_image(embed, activity)
    # Same behavior as event embed regarding avoiding duplicate uploads (handled by callers).
    return embed_with_img, attachment

# ---------------------------
# Lifecycle
# ---------------------------

@bot.event
async def on_ready():
    try:
        await bot.tree.sync()
    except Exception as e:
        print("Slash sync failed:", e)
    # Load queues from disk once
    if not getattr(bot, "_queues_loaded", False):  # type: ignore[attr-defined]
        try:
            await load_queues()
            bot._queues_loaded = True  # type: ignore[attr-defined]
            print("Queues loaded from disk")
        except Exception as e:
            print("Queue load failed:", e)
    if not getattr(bot, "_sched_task", None):
        bot._sched_task = bot.loop.create_task(_scheduler_loop())  # type: ignore[attr-defined]
    if not getattr(bot, "_autosave_task", None):
        bot._autosave_task = bot.loop.create_task(_autosave_loop())  # type: ignore[attr-defined]
    print(f"Ready as {bot.user}")

# ---------------------------
# Welcome Flow (member join)
# ---------------------------

@bot.event
async def on_member_join(member: discord.Member):
    try:
        guild = member.guild
        target_channel_id = _resolve_welcome_channel_id(guild)
        if target_channel_id:
            try:
                title = f"Welcome, {member.display_name}!"
                desc = (
                    f"{member.mention} just joined {guild.name} — glad to have you here!\n\n"
                    "Take a moment to say hi and check out current activities."
                )
                emb = discord.Embed(title=title, description=desc, color=0x00BFFF)
                try:
                    if member.avatar:
                        emb.set_thumbnail(url=member.avatar.url)
                except Exception:
                    pass
                emb.add_field(name="Getting Started", value="Say hi in chat and browse upcoming events.", inline=False)
                emb.add_field(
                    name="Commands",
                    value=(
                        "• /join — choose an activity to enter its queue (max 2)\n"
                        "• /queue — view current queues or a specific activity\n"
                        "• /schedule — founder-only: creates the event post you can react to"
                    ),
                    inline=False,
                )
                emb.add_field(
                    name="What to look for",
                    value=(
                        "• Event posts with reactions: 📝 to note interest, ✅ to join when open, ❌ to leave\n"
                        "• DMs for confirmations and reminders (2h/30m/start)"
                    ),
                    inline=False,
                )
                try: print(f"welcome: posting in <#{int(target_channel_id)}>")
                except Exception: pass
                await _send_to_channel_id(int(target_channel_id), content=None, embed=emb)
            except Exception as e:
                try: print("welcome channel send failed:", e)
                except Exception: pass
        else:
            try: print("welcome: no sendable channel found; set WELCOME_CHANNEL_ID or GENERAL_CHANNEL_ID")
            except Exception: pass

        try:
            dm = await member.create_dm()
            dm_msg = (
                f"Welcome to {guild.name}!\n\n"
                "Getting started:\n"
                "• Say hi and meet the group\n"
                "• Check the event signup channel for upcoming runs\n\n"
                "Commands:\n"
                "• /join — choose an activity to enter its queue (max 2)\n"
                "• /queue — view current queues or a specific activity\n"
                "• /schedule — founder-only: creates an event post you can react to\n\n"
                "What to look for:\n"
                "• Event posts: 📝 adds you as backup; ✅ tries to join when signups open; ❌ leaves\n"
                "• DMs for confirmations and reminders (2h/30m/start); you can reply here with questions"
            )
            await dm.send(content=dm_msg)
        except Exception as e:
            try: print("welcome DM failed:", member.id, e)
            except Exception: pass
    except Exception:
        pass

# ---------------------------
# Queue Boards (optional utility)
# ---------------------------

async def _post_activity_board(activity: str, fallback_channel_id: Optional[int] = None) -> None:
    # Choose target channel: configured RAID_QUEUE_CHANNEL_ID or provided fallback
    target_channel_id = RAID_QUEUE_CHANNEL_ID or fallback_channel_id
    if not target_channel_id:
        return
    # Always ensure a queue exists so we can render empty boards as well
    q = _ensure_queue(activity)
    checked = _ensure_checked(activity)
    embed = discord.Embed(title=f"Queue — {activity}", color=_activity_color(activity))
    embed.add_field(name="Signed Up", value=str(len(q)), inline=True)
    if q:
        lines = [f"<@{uid}>{' ✅' if uid in checked else ''}" for uid in q]
        embed.add_field(name="Players (in order)", value="\n".join(lines), inline=False)
    else:
        embed.description = "No sign-ups yet. Use `/join` to get started."
    embed, attachment = _apply_activity_image(embed, activity)
    await _send_to_channel_id(int(target_channel_id), None, embed=embed, file=attachment)

async def _post_all_activity_boards(fallback_channel_id: Optional[int] = None):
    # If nothing configured, use the provided fallback channel (e.g., the invoking channel)
    target_channel_id = RAID_QUEUE_CHANNEL_ID or fallback_channel_id
    if not target_channel_id:
        return
    for act in list(QUEUES.keys()):
        await _post_activity_board(act, target_channel_id)

# ---------------------------
# Slash Commands
# ---------------------------

@bot.tree.command(name="join", description="Join an activity queue")
@app_commands.describe(activity="Choose an activity to join")
@app_commands.autocomplete(activity=_activity_autocomplete)
async def join_cmd(interaction: discord.Interaction, activity: str):
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if member and _is_sherpa(member):
        await interaction.response.send_message("Sherpa Assistants cannot join queues.", ephemeral=True)
        return
    act, sug = _resolve_activity(activity)
    if not act:
        hint = (" Try: " + ", ".join(sug)) if sug else ""
        await interaction.response.send_message(f"Unknown activity.{hint}", ephemeral=True)
        return
    uid = interaction.user.id
    in_any = [a for a, lst in QUEUES.items() if uid in lst]
    if act in in_any:
        await interaction.response.send_message("You're already in that queue.", ephemeral=True)
        return
    if len(in_any) >= 2:
        await interaction.response.send_message("You can be in at most 2 different activity queues.", ephemeral=True)
        return
    _ensure_queue(act).append(uid)
    await persist_queues()
    await interaction.response.send_message(f"Joined queue for: {act}", ephemeral=True)
    await _post_activity_board(act)

@bot.tree.command(name="leave", description="Leave an activity queue or an event by message ID")
@app_commands.describe(activity="(Optional) activity name to leave", message_id="(Optional) event message ID to leave")
async def leave_cmd(interaction: discord.Interaction, activity: Optional[str] = None, message_id: Optional[int] = None):
    uid = interaction.user.id
    changed = False
    if message_id:
        data = SCHEDULES.get(message_id)
        if not data:
            await interaction.response.send_message("No event found with that message ID.", ephemeral=True)
            return
        participants: List[int] = data.get("players", [])  # type: ignore
        backups: List[int] = data.get("backups", [])  # type: ignore
        if uid in participants:
            participants[:] = [x for x in participants if x != uid]
            moved = _autofill_from_backups(data)
            changed = True
            guild = interaction.client.get_guild(int(data.get("guild_id"))) if data.get("guild_id") else None  # type: ignore
            await _dm_promoted_users(guild, moved, data)
        if uid in backups:
            backups[:] = [x for x in backups if x != uid]
            changed = True
        if changed:
            guild = interaction.client.get_guild(int(data.get("guild_id"))) if data.get("guild_id") else None  # type: ignore
            if guild:
                await _update_schedule_message(guild, message_id)
            await interaction.response.send_message("Left the event.", ephemeral=True)
            return
    if activity:
        act, _ = _resolve_activity(activity, list(ALL_ACTIVITIES) + list(QUEUES.keys()))
        if not act:
            await interaction.response.send_message("Unknown activity.", ephemeral=True)
            return
        q = QUEUES.get(act, [])
        if uid in q:
            q[:] = [x for x in q if x != uid]
            await persist_queues()
            await interaction.response.send_message(f"Left queue: {act}", ephemeral=True)
            await _post_activity_board(act)
            return
        else:
            await interaction.response.send_message("You are not in that queue.", ephemeral=True)
            return
    await interaction.response.send_message("Specify an activity or a message_id to leave.", ephemeral=True)

@bot.tree.command(name="promote", description="Assign Sherpa Assistant role to a chosen user and announce it")
@app_commands.describe(user="User to promote to Sherpa Assistant")
async def promote_cmd(interaction: discord.Interaction, user: discord.User):
    # Acknowledge early to avoid interaction timeouts while we work
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)
    except Exception:
        # If defer fails, continue; we'll try to send a follow-up later
        pass
    guild = interaction.guild

    # Try to auto-detect the relevant event when none is specified
    selected_mid: Optional[int] = None
    data: Optional[Dict[str, object]] = None
    try:
        invoker_uid = int(interaction.user.id)
        channel_id = int(interaction.channel.id) if interaction.channel else None  # type: ignore

        # Prefer events in the current channel where the invoker is the promoter (or founder)
        if channel_id is not None:
            channel_candidates: List[Tuple[int, Dict[str, object]]] = []
            for mid, d in list(SCHEDULES.items()):
                try:
                    ch_id = int(d.get("channel_id")) if d.get("channel_id") else None  # type: ignore
                except Exception:
                    ch_id = None
                if ch_id == channel_id:
                    channel_candidates.append((int(mid), d))

            authorized_in_channel: List[Tuple[int, Dict[str, object]]] = []
            for mid, d in channel_candidates:
                try:
                    pid = int(d.get("promoter_id")) if d.get("promoter_id") else None  # type: ignore
                except Exception:
                    pid = None
                if pid == invoker_uid or (FOUNDER_USER_ID and invoker_uid == int(FOUNDER_USER_ID)):
                    authorized_in_channel.append((mid, d))

            if authorized_in_channel:
                selected_mid, data = max(authorized_in_channel, key=lambda x: x[0])

        # Fallback: latest event where the invoker is the promoter
        if data is None:
            owned: List[Tuple[int, Dict[str, object]]] = []
            for mid, d in list(SCHEDULES.items()):
                try:
                    pid = int(d.get("promoter_id")) if d.get("promoter_id") else None  # type: ignore
                except Exception:
                    pid = None
                if pid == invoker_uid:
                    owned.append((int(mid), d))
            if owned:
                selected_mid, data = max(owned, key=lambda x: x[0])
    except Exception:
        # If auto-detection fails, continue without event context
        data = None
        selected_mid = None

    # If we found an event, enforce promoter/founder permission for that event
    if data and not _is_promoter_or_founder(interaction, data):
        try:
            if interaction.response.is_done():
                await interaction.followup.send("Only the event promoter or the founder can promote for this event.", ephemeral=True)
            else:
                await interaction.response.send_message("Only the event promoter or the founder can promote for this event.", ephemeral=True)
        except Exception:
            pass
        return
    # If no event context, allow the command to run without founder restriction
    # This enables promoting users even when not tied to a specific event.

    promoted_uid = int(user.id)
    promoted_member: Optional[discord.Member] = None
    if guild:
        try:
            promoted_member = guild.get_member(promoted_uid)
            if promoted_member is None:
                # Fallback to API fetch if not cached
                promoted_member = await guild.fetch_member(promoted_uid)
        except Exception:
            promoted_member = None

    assigned = False
    assign_error: Optional[str] = None
    if SHERPA_ASSISTANT_ROLE_ID and guild:
        try:
            role = guild.get_role(int(SHERPA_ASSISTANT_ROLE_ID))
        except Exception:
            role = None
        if promoted_member and role:
            try:
                bot_member = guild.me
                if not bot_member or not getattr(bot_member.guild_permissions, "manage_roles", False):
                    assign_error = "Bot lacks Manage Roles permission."
                elif role.position >= (bot_member.top_role.position if bot_member.top_role else 0):
                    assign_error = "Bot role must be above target role."
                else:
                    await promoted_member.add_roles(role, reason="Assigned Sherpa Assistant via /promote")
                    assigned = True
            except Exception as e:
                assign_error = f"Failed to assign role: {e.__class__.__name__}"
        elif not role and SHERPA_ASSISTANT_ROLE_ID:
            assign_error = "Configured Sherpa Assistant role not found in this guild."
        elif not promoted_member:
            assign_error = "User is not a member of this server."
    else:
        if not SHERPA_ASSISTANT_ROLE_ID:
            assign_error = "SHERPA_ASSISTANT_ROLE_ID not configured."

    # If we have event context, update event's sherpa lists and refresh the message
    if data is not None:
        try:
            sherpas: Set[int] = data.get("sherpas") or set()  # type: ignore
            sbackup: Set[int] = data.get("sherpa_backup") or set()  # type: ignore
            if promoted_uid in sbackup:
                sbackup.discard(promoted_uid)
                data["sherpa_backup"] = sbackup
            if promoted_uid not in sherpas:
                sherpas.add(promoted_uid)
                data["sherpas"] = sherpas
            if guild and selected_mid is not None:
                await _update_schedule_message(guild, selected_mid)
        except Exception:
            pass

    # Build announcement embed (embed titles don't render mentions, so use a display name)
    promoted_display = (
        promoted_member.display_name
        if promoted_member is not None
        else (getattr(user, "global_name", None) or user.name)
    )
    title = f"🎉 Congratulations, {promoted_display}! 🎉"
    desc = (
        "✨ What it Means to be a Sherpa Assistant\n"
        "You are now part of an elite group dedicated to helping Guardians conquer Destiny’s toughest challenges.\n"
        "Sherpas bring patience, clarity, and positive vibes to every fireteam.\n"
        "You’re the torchbearers — guiding others through chaos and turning doubt into understanding.\n\n"
        "❤️ Why We Do This\n"
        "Every Guardian deserves the chance to experience the best of Destiny.\n"
        "By serving as a Sherpa Assistant, you’re building a stronger, more inclusive community where knowledge is shared.\n\n"
        "⚔️ Expectations\n"
        "• Be the calm voice when the fireteam feels the pressure\n"
        "• Explain mechanics clearly so anyone can succeed\n"
        "• Turn wipes into lessons, and lessons into victory\n"
        "• Keep every run welcoming, fun, and unforgettable\n\n"
        "🧭 Carry the Light\n"
        "Lead with patience, lift others up, and show what it truly means to Carry the Light."
    )
    emb = discord.Embed(title=title, description=desc, color=0xFFD700)
    try:
        # Prefer the member's display avatar; fall back to the user's if needed
        avatar_url = (
            promoted_member.display_avatar.url
            if promoted_member is not None
            else user.display_avatar.url
        )
        if avatar_url:
            emb.set_thumbnail(url=avatar_url)
    except Exception:
        pass
    if data is not None:
        try:
            emb.add_field(name="Event", value=str(data.get("activity", "event")), inline=True)
            emb.add_field(name="When", value=str(data.get("when_text", "TBD")), inline=True)
            # Include a link to the sign-up post if we know it
            guild_id = int(data.get("guild_id")) if data.get("guild_id") else (guild.id if guild else None)  # type: ignore
            ch_id = int(data.get("channel_id")) if data.get("channel_id") else None  # type: ignore
            if guild_id and ch_id and selected_mid:
                link = f"https://discord.com/channels/{guild_id}/{ch_id}/{selected_mid}"
                emb.add_field(name="Sign-up Post", value=f"[Open]({link})", inline=False)
        except Exception:
            pass
    emb.set_footer(text=f"Assigned by {interaction.user.display_name}")

    posted = 0
    for ch_id in (GENERAL_CHANNEL_ID, GENERAL_SHERPA_CHANNEL_ID):
        try:
            if ch_id:
                msg = await _send_to_channel_id(ch_id, embed=emb)  # type: ignore[arg-type]
                if msg:
                    posted += 1
                    try:
                        await msg.add_reaction("🎉")
                    except Exception:
                        pass
        except Exception:
            pass

    # DM the promoted member
    try:
        if promoted_member and assigned:
            d = await promoted_member.create_dm()
            activity_name = str(data.get("activity")) if data else None
            suffix = f" for {activity_name}" if activity_name else ""
            await d.send(f"You've been assigned the Sherpa Assistant role{suffix}.")
    except Exception:
        pass

    # Final ephemeral follow-up
    try:
        msg = f"Promotion applied. Role assigned: {assigned}. Announced in {posted} channel(s)."
        if not assigned and assign_error:
            msg += f"\nNote: {assign_error}"
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)
    except Exception:
        pass

@bot.tree.command(name="add", description="Add a user to a queue or event (promoter/founder for events)")
@app_commands.describe(activity="(Optional) activity to add to", message_id="(Optional) event message ID to add to", user="User mention or ID to add")
async def add_cmd(interaction: discord.Interaction, user: str, activity: Optional[str] = None, message_id: Optional[int] = None):
    guild = interaction.guild
    uid_list = _parse_user_ids(user, guild) if guild else []
    if not uid_list:
        await interaction.response.send_message("Couldn't resolve that user.", ephemeral=True)
        return
    uid = uid_list[0]
    if message_id:
        data = SCHEDULES.get(message_id)
        if not data:
            await interaction.response.send_message("No event found with that message ID.", ephemeral=True)
            return
        if not _is_promoter_or_founder(interaction, data):
            await interaction.response.send_message("Only the promoter or founder can add users to this event.", ephemeral=True)
            return
        participants: List[int] = data.get("players", [])  # type: ignore
        backups: List[int] = data.get("backups", [])  # type: ignore
        cap = int(data.get("capacity", 0))
        reserved = int(data.get("reserved_sherpas", 0))
        player_slots = max(0, cap - reserved)
        where = _user_in_any_event_list(data, uid)
        if where is not None:
            await interaction.response.send_message(f"User already in event ({where}).", ephemeral=True)
            return
        if len(participants) < player_slots:
            participants.append(uid); status = "Player"
        else:
            backups.append(uid); status = "Backup"
        if guild: await _update_schedule_message(guild, message_id)  # type: ignore
        await interaction.response.send_message(f"Added user as {status}.", ephemeral=True)
        return

    if activity:
        act, sug = _resolve_activity(activity)
        if not act:
            hint = (" Try: " + ", ".join(sug)) if sug else ""
            await interaction.response.send_message(f"Unknown activity.{hint}", ephemeral=True)
            return
        q = _ensure_queue(act)
        if uid in q:
            await interaction.response.send_message("User already in queue.", ephemeral=True)
            return
        q.append(uid)
        await persist_queues()
        await interaction.response.send_message(f"Added user to queue: {act}", ephemeral=True)
        await _post_activity_board(act)
        return

    await interaction.response.send_message("Specify an activity or message_id to add the user to.", ephemeral=True)

@bot.tree.command(name="remove", description="Remove a user from a queue or event (founder only)")
@founder_only()
@app_commands.describe(activity="(Optional) activity to remove from", message_id="(Optional) event message ID", user="User mention or ID to remove")
async def remove_cmd(interaction: discord.Interaction, user: str, activity: Optional[str] = None, message_id: Optional[int] = None):
    guild = interaction.guild
    uid_list = _parse_user_ids(user, guild) if guild else []
    if not uid_list:
        await interaction.response.send_message("Couldn't resolve that user.", ephemeral=True)
        return
    uid = uid_list[0]
    if message_id:
        data = SCHEDULES.get(message_id)
        if not data:
            await interaction.response.send_message("No event found with that message ID.", ephemeral=True)
            return
        if not _is_promoter_or_founder(interaction, data):
            await interaction.response.send_message("Only the promoter or founder can remove users from this event.", ephemeral=True)
            return
        participants: List[int] = data.get("players", [])  # type: ignore
        backups: List[int] = data.get("backups", [])  # type: ignore
        removed = False
        if uid in participants:
            participants[:] = [x for x in participants if x != uid]
            _autofill_from_backups(data); removed = True
        if uid in backups:
            backups[:] = [x for x in backups if x != uid]
            removed = True
        if removed and guild:
            await _update_schedule_message(guild, message_id)  # type: ignore
        await interaction.response.send_message("Removed user from event." if removed else "User not in that event.", ephemeral=True)
        return

    if activity:
        act, _ = _resolve_activity(activity, list(ALL_ACTIVITIES) + list(QUEUES.keys()))
        if not act:
            await interaction.response.send_message("Unknown activity.", ephemeral=True)
            return
        q = QUEUES.get(act, [])
        if uid in q:
            q[:] = [x for x in q if x != uid]
            await persist_queues()
            await interaction.response.send_message("Removed user from queue.", ephemeral=True)
            await _post_activity_board(act)
            return
        await interaction.response.send_message("User not in that queue.", ephemeral=True)
        return

    await interaction.response.send_message("Specify an activity or message_id to remove the user from.", ephemeral=True)

@bot.tree.command(name="queue", description="Post the current queues (one embed per activity, or pick a specific activity)")
@app_commands.describe(activity="(Optional) Choose an activity to show its queue only")
@app_commands.autocomplete(activity=_activity_autocomplete)
async def queue_cmd(interaction: discord.Interaction, activity: Optional[str] = None):
    await interaction.response.defer(ephemeral=True)
    if activity:
        act, sug = _resolve_activity(activity)
        if not act:
            hint = (" Try: " + ", ".join(sug)) if sug else ""
            await interaction.followup.send(f"Unknown activity.{hint}", ephemeral=True)
            return
        await _post_activity_board(act, interaction.channel_id)
        await interaction.followup.send(f"Queue board posted for: {act}", ephemeral=True)
    else:
        await _post_all_activity_boards(interaction.channel_id)
        await interaction.followup.send("Queue boards posted.", ephemeral=True)

@bot.tree.command(name="count", description="Increment a persistent counter and show the value")
async def count_cmd(interaction: discord.Interaction):
    new_value = await _increment_counter()
    await interaction.response.send_message(f"Count: {new_value}")

# Simple health check
@bot.tree.command(name="ping", description="Health check: bot latency")
async def ping_cmd(interaction: discord.Interaction):
    try:
        latency_ms = int((bot.latency or 0.0) * 1000)
    except Exception:
        latency_ms = 0
    await interaction.response.send_message(f"Pong! {latency_ms} ms")

# ---------------------------
# Parser
# ---------------------------

def _parse_user_ids(text: str, guild: Optional[discord.Guild]) -> List[int]:
    if not text or not guild:
        return []
    parts = [p.strip() for p in text.replace(",", " ").split() if p.strip()]
    out: List[int] = []
    for p in parts:
        if p.isdigit():
            out.append(int(p)); continue
        if p.startswith("<@") and p.endswith(">"):
            num = "".join(ch for ch in p if ch.isdigit())
            if num: out.append(int(num)); continue
        m = discord.utils.find(lambda m: m.display_name.lower() == p.lower() or m.name.lower() == p.lower(), guild.members)
        if m: out.append(m.id)
    seen = set(); uniq: List[int] = []
    for uid in out:
        if uid not in seen:
            uniq.append(uid); seen.add(uid)
    return uniq

# ---------------------------
# DM Confirm Views
# ---------------------------

class ConfirmView(discord.ui.View):
    def __init__(self, mid: int, uid: int):
        super().__init__(timeout=None); self.mid = mid; self.uid = uid

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success, custom_id="confirm_yes")
    async def yes(self, interaction: discord.Interaction, button: discord.ui.Button):  # type: ignore[override]
        if interaction.user.id != self.uid:
            await interaction.response.send_message("This DM button isn't for you.", ephemeral=True); return
        data = SCHEDULES.get(self.mid)
        if not data:
            await interaction.response.send_message("Event no longer exists.", ephemeral=True); return
        participants: List[int] = data.get("players", [])  # type: ignore
        cap = int(data.get("capacity", 0)); reserved = int(data.get("reserved_sherpas", 0))
        player_slots = max(0, cap - reserved)
        # Try to add to players if there is space; otherwise backups
        if len(participants) < player_slots:
            added, reason = _append_unique_to(data, "players", self.uid)
            if added:
                await interaction.response.send_message("Locked in. See you there! ✅", ephemeral=True)
                _log_confirmation(self.mid, self.uid, "confirm", "added_players")
            else:
                await interaction.response.send_message("You're already accounted for.", ephemeral=True)
                _log_confirmation(self.mid, self.uid, "confirm", "skipped", reason)
        else:
            added, reason = _append_unique_to(data, "backups", self.uid)
            if added:
                await interaction.response.send_message("Roster is full — added as **Backup**.", ephemeral=True)
                _log_confirmation(self.mid, self.uid, "confirm", "added_backups")
            else:
                await interaction.response.send_message("You're already accounted for.", ephemeral=True)
                _log_confirmation(self.mid, self.uid, "confirm", "skipped", reason)
        guild = interaction.client.get_guild(int(data.get("guild_id"))) if data.get("guild_id") else None  # type: ignore
        if guild: await _update_schedule_message(guild, self.mid)

    @discord.ui.button(label="Can't make it", style=discord.ButtonStyle.secondary, custom_id="confirm_no")
    async def no(self, interaction: discord.Interaction, button: discord.ui.Button):  # type: ignore[override]
        if interaction.user.id != self.uid:
            await interaction.response.send_message("This DM button isn't for you.", ephemeral=True); return
        data = SCHEDULES.get(self.mid)
        if data:
            participants: List[int] = data.get("players", [])  # type: ignore
            if self.uid in participants:
                participants[:] = [x for x in participants if x != self.uid]
                _autofill_from_backups(data)
            guild = interaction.client.get_guild(int(data.get("guild_id"))) if data.get("guild_id") else None  # type: ignore
            if guild: await _update_schedule_message(guild, self.mid)
        await interaction.response.send_message("All good. Thanks for letting us know.", ephemeral=True)
        _log_confirmation(self.mid, self.uid, "decline", "ok")

class SherpaConfirmView(discord.ui.View):
    def __init__(self, mid: int, uid: int):
        super().__init__(timeout=None); self.mid = mid; self.uid = uid

    @discord.ui.button(label="Confirm Sherpa", style=discord.ButtonStyle.success, custom_id="sherpa_confirm_yes")
    async def yes(self, interaction: discord.Interaction, button: discord.ui.Button):  # type: ignore[override]
        if interaction.user.id != self.uid:
            await interaction.response.send_message("This DM button isn't for you.", ephemeral=True); return
        data = SCHEDULES.get(self.mid)
        if not data:
            await interaction.response.send_message("Event no longer exists.", ephemeral=True); return
        sherpas: Set[int] = data.get("sherpas") or set()  # type: ignore
        reserved = int(data.get("reserved_sherpas", 0))
        if self.uid in sherpas:
            await interaction.response.send_message("You're already locked in as a Sherpa.", ephemeral=True); return
        if len(sherpas) < reserved:
            added, reason = _append_unique_to(data, "sherpas", self.uid)
            if added:
                await interaction.response.send_message("Locked in as Sherpa. Thank you! ✅", ephemeral=True)
                _log_confirmation(self.mid, self.uid, "sherpa_confirm", "added_sherpas")
            else:
                await interaction.response.send_message("You're already accounted for.", ephemeral=True)
                _log_confirmation(self.mid, self.uid, "sherpa_confirm", "skipped", reason)
        else:
            added, reason = _append_unique_to(data, "sherpa_backup", self.uid)
            if added:
                await interaction.response.send_message("All Sherpa slots are full — added as Sherpa Backup.", ephemeral=True)
                _log_confirmation(self.mid, self.uid, "sherpa_confirm", "added_sbackup")
            else:
                await interaction.response.send_message("You're already accounted for.", ephemeral=True)
                _log_confirmation(self.mid, self.uid, "sherpa_confirm", "skipped", reason)
        guild = interaction.client.get_guild(int(data.get("guild_id"))) if data.get("guild_id") else None  # type: ignore
        if guild: await _update_schedule_message(guild, self.mid)

    @discord.ui.button(label="Can't make it", style=discord.ButtonStyle.secondary, custom_id="sherpa_confirm_no")
    async def no(self, interaction: discord.Interaction, button: discord.ui.Button):  # type: ignore[override]
        if interaction.user.id != self.uid:
            await interaction.response.send_message("This DM button isn't for you.", ephemeral=True); return
        data = SCHEDULES.get(self.mid)
        if data:
            sherpas: Set[int] = data.get("sherpas") or set()  # type: ignore
            sbackup: Set[int] = data.get("sherpa_backup") or set()  # type: ignore
            if self.uid in sherpas: sherpas.discard(self.uid)
            if self.uid in sbackup: sbackup.discard(self.uid)
            guild = interaction.client.get_guild(int(data.get("guild_id"))) if data.get("guild_id") else None  # type: ignore
            if guild: await _update_schedule_message(guild, self.mid)
        await interaction.response.send_message("All good. Thanks for letting us know.", ephemeral=True)

# ---------------------------
# Schedules & Reminders
# ---------------------------

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
            participants.append(nxt); moved.append(nxt)
    return moved

async def _dm_promoted_users(guild: Optional[discord.Guild], moved: List[int], data: Dict[str, object]):
    if not guild or not moved:
        return
    activity = data.get("activity", "Event")
    when_text = data.get("when_text", "soon")
    for uid in moved:
        try:
            member = guild.get_member(uid)
            if not member:
                continue
            d = await member.create_dm()
            await d.send(f"You have been pulled from Backup into the roster for **{activity}** ({when_text}).")
        except Exception:
            pass

async def _update_schedule_message(guild: discord.Guild, message_id: int):
    data = SCHEDULES.get(message_id)
    if not data: return
    ch_id = int(data.get("channel_id")) if data.get("channel_id") else None  # type: ignore
    if not ch_id: return
    try:
        ch = bot.get_channel(ch_id) or await bot.fetch_channel(ch_id)
        msg = await ch.fetch_message(int(message_id))
        # If we have not yet persisted a CDN image URL, try to capture it
        # from the existing message (either the embed's image URL if it's already
        # a CDN, or from an image attachment on the message).
        if not data.get("image_url"):
            try:
                existing_cdn: Optional[str] = None
                # Prefer the embed image URL if it is already a CDN link
                if msg.embeds and msg.embeds[0].image and msg.embeds[0].image.url:
                    url = str(msg.embeds[0].image.url)
                    if not url.startswith("attachment://"):
                        existing_cdn = url
                # Otherwise, fall back to an image attachment URL if present
                if not existing_cdn:
                    for att in (msg.attachments or []):
                        try:
                            ctype = (att.content_type or "").lower()
                        except Exception:
                            ctype = ""
                        if ctype.startswith("image"):
                            existing_cdn = att.url
                            break
                if existing_cdn:
                    data["image_url"] = existing_cdn
            except Exception:
                pass
        if str(data.get("type")) == "sherpa_only":
            embed, _ = await _render_sherpa_only_embed(guild, str(data["activity"]), data)  # type: ignore
        else:
            embed, _ = await _render_event_embed(guild, str(data["activity"]), data)  # type: ignore
        # Only remove attachments if we have a persisted CDN image URL to use.
        # Otherwise, preserve existing attachments so the embed image doesn't disappear.
        try:
            if data.get("image_url"):
                await msg.edit(embed=embed, attachments=[])
            else:
                await msg.edit(embed=embed)
        except Exception:
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
                if not start_ts: continue
                cap = int(data.get("capacity", 0))
                if str(data.get("type")) == "sherpa_only":
                    player_slots = cap
                else:
                    reserved = int(data.get("reserved_sherpas", 0))
                    player_slots = max(0, cap - reserved)
                participants: List[int] = data.get("players", [])  # type: ignore

                # At T-2h, open signups if slots remain
                if str(data.get("type")) != "sherpa_only" and (not data.get("signups_open")) and now >= start_ts - 2*60*60 and len(participants) < player_slots:
                    data["signups_open"] = True
                    # Try to promote from backups immediately when opening
                    try:
                        moved = _autofill_from_backups(data)
                        guild = bot.get_guild(int(data.get("guild_id"))) if data.get("guild_id") else None  # type: ignore
                        await _dm_promoted_users(guild, moved, data)
                    except Exception:
                        pass
                    # Add ✅, 📝, ❌ to main event post
                    try:
                        ch = bot.get_channel(int(data.get("channel_id"))) or await bot.fetch_channel(int(data.get("channel_id")))
                        if ch:
                            msg = await ch.fetch_message(int(mid))
                            for emoji in ("✅", "📝", "❌"):
                                try: await msg.add_reaction(emoji)
                                except Exception: pass
                    except Exception:
                        pass
                    # LFG announcement ONLY if channel configured: @everyone and point to event signup channel
                    # Before announcing, pull available backups into open player slots
                    if LFG_CHAT_CHANNEL_ID:
                        try:
                            moved = _autofill_from_backups(data)
                            guild2 = bot.get_guild(int(data.get("guild_id"))) if data.get("guild_id") else None  # type: ignore
                            await _dm_promoted_users(guild2, moved, data)
                        except Exception:
                            pass
                        event_link = None
                        try:
                            ch = bot.get_channel(int(data.get("channel_id"))) or await bot.fetch_channel(int(data.get("channel_id")))
                            m = await ch.fetch_message(int(mid)) if ch else None
                            event_link = m.jump_url if m else None
                        except Exception:
                            event_link = None
                        # Always direct to the configured event signup channel if present
                        target_signup_ch = int(EVENT_SIGNUP_CHANNEL_ID) if EVENT_SIGNUP_CHANNEL_ID else (int(data.get('channel_id')) if data.get('channel_id') else None)
                        signup_channel_mention = f"<#{target_signup_ch}>" if target_signup_ch else "the event signup channel"
                        await _send_to_channel_id(
                            LFG_CHAT_CHANNEL_ID,
                            content=(
                                f"@everyone 📣 Slots open for **{data['activity']}** starting in ~2 hours!\n"
                                f"Head to {signup_channel_mention} to join. "
                                + (f"Jump to the event: {event_link}" if event_link else "")
                            ).strip(),
                        )

                # DM Reminders: 2h, 30m, start
                for label, delta, key in (("2h", 2*60*60, "r_2h"), ("30m", 30*60, "r_30m"), ("start", 0, "r_0m")):
                    if not data.get(key) and now >= start_ts - delta:
                        await _send_reminders(data, label)
                        data[key] = True

        except Exception as e:
            print("scheduler error:", e)
        finally:
            await asyncio.sleep(60)


async def _autosave_loop():
    # Periodically persist queues to reduce data loss windows
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            await persist_queues()
        except Exception:
            pass
        await asyncio.sleep(60)

async def _send_reminders(data: Dict[str, object], label: str):
    guild = bot.get_guild(int(data.get("guild_id"))) if data.get("guild_id") else None  # type: ignore
    if not guild: return
    activity = data.get("activity", "Event")
    when_text = data.get("when_text", "soon")
    participants: List[int] = data.get("players", [])  # type: ignore
    sherpas: Set[int] = data.get("sherpas", set())  # type: ignore

    voice_mention = None
    try:
        vc_id = int(data.get("voice_channel_id")) if data.get("voice_channel_id") else None  # type: ignore
        if vc_id:
            voice_mention = f" <#{vc_id}>"
    except Exception:
        voice_mention = None

    msg = {
        "2h": f"Eyes up! Your **{activity}** starts in ~2 hours ({when_text}). Be in{voice_mention or ' voice channel'} on time. If you can’t make it, hit ❌ on the signup to free the slot.",
        "30m": f"30-minute check: **{activity}** starts soon ({when_text}). Grab loadout, shaders, and water. See you in{voice_mention or ' voice channel'}.",
        "start": f"It’s go time: **{activity}** ({when_text}). Join{voice_mention or ' voice channel'} now. If you’re late, we may pull from Backup.",
    }.get(label, f"Reminder: **{activity}** ({when_text}).")

    async def dm(uid: int):
        try:
            member = guild.get_member(uid)
            if not member: return False
            d = await member.create_dm()
            await d.send(msg)
            return True
        except Exception as e:
            try: print("DM reminder failed:", label, uid, e)
            except Exception: pass
            return False

    sent_p = 0; sent_s = 0
    for uid in participants:
        if await dm(uid): sent_p += 1
    for uid in sherpas:
        if await dm(uid): sent_s += 1
    try: print(f"Reminders sent ({label}): players={sent_p}, sherpas={sent_s}")
    except Exception: pass

    # Schedule a survey DM 3h after start (for 'start' only)
    if label == "start":
        async def survey_task():
            try:
                await asyncio.sleep(3 * 60 * 60)
                g = bot.get_guild(int(data.get("guild_id"))) if data.get("guild_id") else None  # type: ignore
                if not g: return
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

# ---------------------------
# Auto-restore deleted event embeds
# ---------------------------

@bot.event
async def on_message_delete(message: discord.Message):
    try:
        data = SCHEDULES.get(message.id)
        if not data:
            return
        guild = message.guild or (bot.get_guild(int(data.get("guild_id"))) if data.get("guild_id") else None)  # type: ignore
        if str(data.get("type")) == "sherpa_only":
            embed, f = await _render_sherpa_only_embed(guild, str(data.get("activity", "Event")), data)
        else:
            embed, f = await _render_event_embed(guild, str(data.get("activity", "Event")), data)
        ch_id = int(data.get("channel_id")) if data.get("channel_id") else (message.channel.id if message.channel else None)  # type: ignore
        if not ch_id:
            return
        new_msg = await _send_to_channel_id(int(ch_id), embed=embed, file=f)
        if not new_msg:
            return
        # Re-add standard reactions depending on type
        if str(data.get("type")) == "sherpa_only":
            for emoji in ("✅", "🔁", "❌"):
                try:
                    await new_msg.add_reaction(emoji)
                except Exception:
                    pass
        else:
            for emoji in ("📝", "🔁", "❌"):
                try:
                    await new_msg.add_reaction(emoji)
                except Exception:
                    pass
        # Persist rehosted image URL if present on restored embed and convert to embed-only image
        try:
            if new_msg.embeds and new_msg.embeds[0].image and new_msg.embeds[0].image.url:
                data["image_url"] = new_msg.embeds[0].image.url
                # Re-render without file attachment to avoid duplicate upload preview
                if str(data.get("type")) == "sherpa_only":
                    restored_embed, _ = await _render_sherpa_only_embed(guild, str(data.get("activity", "Event")), data)
                else:
                    restored_embed, _ = await _render_event_embed(guild, str(data.get("activity", "Event")), data)
                try:
                    await new_msg.edit(embed=restored_embed, attachments=[])
                except Exception:
                    # Fallback without explicit attachments param if unsupported
                    try:
                        await new_msg.edit(embed=restored_embed)
                    except Exception:
                        pass
        except Exception:
            pass
        # Update schedule mapping to include the new message id while preserving the old for DM callbacks
        new_mid = int(new_msg.id)
        SCHEDULES[new_mid] = data
        # Also keep old key mapped to the same data so existing DM views continue to work
        SCHEDULES[message.id] = data
        # Update stored channel id in case the restore posted to a different channel
        data["channel_id"] = int(new_msg.channel.id)

        # If a Sherpa signup alert exists, update its link to point to the restored event
        try:
            alert_mid = int(data.get("sherpa_alert_message_id")) if data.get("sherpa_alert_message_id") else None  # type: ignore
            alert_ch = int(data.get("sherpa_alert_channel_id")) if data.get("sherpa_alert_channel_id") else None  # type: ignore
            if alert_mid and alert_ch:
                ch = bot.get_channel(alert_ch) or await bot.fetch_channel(alert_ch)
                if ch:
                    amsg = await ch.fetch_message(alert_mid)
                    if amsg and amsg.embeds:
                        src = amsg.embeds[0]
                        new_emb = discord.Embed(title=src.title, description=src.description, color=src.color)
                        # Preserve existing fields, but update/ensure Main Event link
                        main_event_updated = False
                        for field in src.fields:
                            if str(field.name).lower().startswith("main event"):
                                new_emb.add_field(name=field.name, value=f"[Jump to event]({new_msg.jump_url})", inline=field.inline)
                                main_event_updated = True
                            else:
                                new_emb.add_field(name=field.name, value=field.value, inline=field.inline)
                        if not main_event_updated:
                            new_emb.add_field(name="Main Event", value=f"[Jump to event]({new_msg.jump_url})", inline=False)
                        # Preserve image if any
                        try:
                            if src.image and src.image.url:
                                new_emb.set_image(url=src.image.url)
                        except Exception:
                            pass
                        await amsg.edit(embed=new_emb)
        except Exception:
            pass
    except Exception:
        pass

# ---------------------------
# /schedule
# ---------------------------

@bot.tree.command(name="schedule", description="(Founder) Create event: 2 embeds + 2 announcements, DM queue, reminders")
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
    try:
        await interaction.response.defer(ephemeral=True)
    except Exception:
        pass

    try:
        act, sug = _resolve_activity(activity)
        if not act:
            hint = (" Try: " + ", ".join(sug)) if sug else ""
            await interaction.followup.send(f"Unknown activity.{hint}", ephemeral=True); return

        # Channel: main event embed must go into EVENT_SIGNUP_CHANNEL_ID (fallback: current channel)
        channel_id = (EVENT_SIGNUP_CHANNEL_ID or interaction.channel_id)

        cap = _cap_for_activity(act)
        reserved = max(0, min(int(reserved_sherpas or 0), cap))

        q = QUEUES.get(act, [])
        candidates = list(q)  # DM everyone in queue

        # Parse datetime_str (MM-DD HH:MM) with current year
        try:
            date_part, time_part = datetime_str.strip().split()
            year = datetime.now().year
            date_full = f"{year}-{date_part}"
        except Exception:
            await interaction.followup.send("Invalid datetime format. Use MM-DD HH:MM.", ephemeral=True); return

        start_ts = _parse_date_time_to_epoch(date_full, time_part, tz_name=timezone)
        when_text = f"<t:{start_ts}:F> ({timezone})" if start_ts else "TBD"

        guild = interaction.guild
        sherpa_ids = set(_parse_user_ids(sherpas or "", guild)) if sherpas else set()
        participant_ids = _parse_user_ids(participants or "", guild) if participants else []

        promoter_id = interaction.user.id
        if promoter_id not in participant_ids:
            participant_ids.insert(0, promoter_id)

        # Build players/backups from non-sherpa participants only
        # Sherpas are tracked separately in data["sherpas"] and do not appear in Players
        player_slots = max(0, cap - reserved)
        seen = set(); uniq_participants: List[int] = []
        for uid in participant_ids:
            if uid in sherpa_ids:
                continue
            if uid not in seen:
                uniq_participants.append(uid); seen.add(uid)
        players_final = uniq_participants[:player_slots]
        backups_final = uniq_participants[player_slots:]

        data = {
            "guild_id": guild.id if guild else None,
            "activity": act,
            "desc": f"Scheduled by {interaction.user.mention}. Check your DMs to confirm.",
            "when_text": when_text,
            "capacity": cap,
            "reserved_sherpas": reserved,
            "sherpas": sherpa_ids,
            "sherpa_backup": set(),
            "candidates": candidates,
            "players": players_final,
            "backups": backups_final,
            "promoter_id": promoter_id,
            "signups_open": False,
            "channel_id": channel_id,
            "start_ts": start_ts,
            "r_2h": False, "r_30m": False, "r_0m": False,
        }

        # ---- EMBED 1: Main Event Embed (EVENT_SIGNUP_CHANNEL_ID) ----
        embed, f = await _render_event_embed(guild, act, data)
        ev_msg = await _send_to_channel_id(int(channel_id), embed=embed, file=f)
        if not ev_msg:
            await interaction.followup.send("Failed to post event — set RAID_DUNGEON_EVENT_SIGNUP_CHANNEL_ID or run this in a channel.", ephemeral=True)
            return

        # Add initial 📝 and ❌ only; ✅ appears at T-2h if player slots remain
        for emoji in ("📝", "❌"):
            try: await ev_msg.add_reaction(emoji)
            except Exception: pass

        mid = ev_msg.id
        # Persist image URL if Discord re-hosted the attachment and immediately convert to embed-only image
        try:
            if ev_msg.embeds and ev_msg.embeds[0].image and ev_msg.embeds[0].image.url:
                data["image_url"] = ev_msg.embeds[0].image.url
                # Re-render embed with CDN URL and remove attachment to avoid duplicate file upload preview
                embed_cdn, _ = await _render_event_embed(guild, act, data)
                try:
                    await ev_msg.edit(embed=embed_cdn, attachments=[])
                except Exception:
                    try:
                        await ev_msg.edit(embed=embed_cdn)
                    except Exception:
                        pass
        except Exception:
            pass
        SCHEDULES[mid] = data
        # Immediately re-render using the CDN image URL and remove attachments to avoid duplicate image card
        try:
            if guild:
                await _update_schedule_message(guild, int(mid))
        except Exception:
            pass

        # ---- EMBED 2: Sherpa Signup Embed (RAID_SIGN_UP_CHANNEL_ID) ----
        sherpa_alert_url = None
        posted_sherpa_signup = False
        sherpa_signup_fallback = None
        if RAID_SIGN_UP_CHANNEL_ID:
            try:
                sherpa_embed = discord.Embed(
                    title=f"🧭 Sherpa Signup — {act}",
                    description=(
                        f"{reserved} reserved Sherpa slot(s). React ✅ on **this** post to claim your Sherpa slot.\n"
                        f"Or react 🔁 to be **Sherpa Backup**."
                    ),
                    color=_activity_color(act),
                )
                sherpa_embed.add_field(name="When", value=when_text, inline=True)
                try:
                    sherpa_embed.add_field(name="Main Event", value=f"[Jump to event]({ev_msg.jump_url})", inline=False)
                except Exception:
                    pass

                alert = await _send_to_channel_id(int(RAID_SIGN_UP_CHANNEL_ID), embed=sherpa_embed)
                if alert:
                    SCHEDULES[mid]["sherpa_alert_channel_id"] = str(alert.channel.id)
                    SCHEDULES[mid]["sherpa_alert_message_id"] = str(alert.id)
                    try: await alert.add_reaction("✅")
                    except Exception: pass
                    try: await alert.add_reaction("🔁")
                    except Exception: pass
                    try:
                        sherpa_alert_url = alert.jump_url
                    except Exception:
                        pass
                    posted_sherpa_signup = True
            except Exception as e:
                try: print("Sherpa signup post failed:", e)
                except Exception: pass
        # fallback: if RAID_SIGN_UP_CHANNEL_ID missing or failed, try posting in the event channel
        if not posted_sherpa_signup:
            try:
                sherpa_embed = discord.Embed(
                    title=f"🧭 Sherpa Signup — {act}",
                    description=(
                        f"{reserved} reserved Sherpa slot(s). React ✅ on **this** post to claim your Sherpa slot.\n"
                        f"Or react 🔁 to be **Sherpa Backup**."
                    ),
                    color=_activity_color(act),
                )
                sherpa_embed.add_field(name="When", value=when_text, inline=True)
                try:
                    sherpa_embed.add_field(name="Main Event", value=f"[Jump to event]({ev_msg.jump_url})", inline=False)
                except Exception:
                    pass
                alert = await _send_to_channel_id(int(channel_id), embed=sherpa_embed)
                if alert:
                    try: await alert.add_reaction("✅")
                    except Exception: pass
                    try: await alert.add_reaction("🔁")
                    except Exception: pass
                    try:
                        sherpa_alert_url = alert.jump_url
                    except Exception:
                        pass
                    sherpa_signup_fallback = int(channel_id)
                    posted_sherpa_signup = True
            except Exception as e:
                try: print("Sherpa signup fallback post failed:", e)
                except Exception: pass

        # ---- ANNOUNCEMENT 1: General Sherpa ping (GENERAL_SHERPA_CHANNEL_ID) ----
        posted_general_announce = False
        general_announce_fallback = None
        if GENERAL_SHERPA_CHANNEL_ID:
            try:
                ping_text = f"<@&{SHERPA_ASSISTANT_ROLE_ID}>" if SHERPA_ASSISTANT_ROLE_ID else None
                gen_embed = discord.Embed(
                    title=f"Sherpa Signup — {act}",
                    description=(
                        f"{when_text}\n"
                        f"Please use the **Sherpa signup post** to claim your slot (✅). "
                        f"Extras become **Sherpa Backup**."
                    ),
                    color=_activity_color(act),
                )
                # Prefer linking directly to the Sherpa signup post; fall back to main event
                try:
                    if sherpa_alert_url:
                        gen_embed.add_field(name="Sherpa Signup", value=f"[Tap here to claim]({sherpa_alert_url})", inline=False)
                    elif ev_msg:
                        gen_embed.add_field(name="Main Event", value=f"[Jump to event]({ev_msg.jump_url})", inline=False)
                except Exception:
                    pass
                msg = await _send_to_channel_id(int(GENERAL_SHERPA_CHANNEL_ID), content=ping_text, embed=gen_embed)
                if msg:
                    posted_general_announce = True
            except Exception as e:
                try: print("General Sherpa announcement failed:", e)
                except Exception: pass
        # fallback: if GENERAL_SHERPA_CHANNEL_ID missing or failed, try GENERAL_CHANNEL_ID
        if not posted_general_announce and GENERAL_CHANNEL_ID:
            try:
                ping_text = f"<@&{SHERPA_ASSISTANT_ROLE_ID}>" if SHERPA_ASSISTANT_ROLE_ID else None
                gen_embed = discord.Embed(
                    title=f"Sherpa Signup — {act}",
                    description=(
                        f"{when_text}\n"
                        f"Please use the **Sherpa signup post** to claim your slot (✅). "
                        f"Extras become **Sherpa Backup**."
                    ),
                    color=_activity_color(act),
                )
                try:
                    if sherpa_alert_url:
                        gen_embed.add_field(name="Sherpa Signup", value=f"[Tap here to claim]({sherpa_alert_url})", inline=False)
                    elif ev_msg:
                        gen_embed.add_field(name="Main Event", value=f"[Jump to event]({ev_msg.jump_url})", inline=False)
                except Exception:
                    pass
                msg = await _send_to_channel_id(int(GENERAL_CHANNEL_ID), content=ping_text, embed=gen_embed)
                if msg:
                    posted_general_announce = True
                    general_announce_fallback = int(GENERAL_CHANNEL_ID)
            except Exception as e:
                try: print("General announcement fallback failed:", e)
                except Exception: pass

        # ---- DM pre-slotted sherpas (info-only) ----
        try:
            for sid in list(sherpa_ids):
                try:
                    m = guild.get_member(sid) if guild else None
                    if not m: continue
                    dm = await m.create_dm()
                    content = (
                        f"You're pre-slotted as a **Sherpa** for **{act}** at **{when_text}**.\n"
                        "No action needed. If plans change, please let the promoter know."
                    )
                    await dm.send(content=content)
                except Exception:
                    pass
        except Exception:
            pass

        # ---- DMs to entire queue (ConfirmView) ----
        sent = 0
        for uid in candidates:
            try:
                m = guild.get_member(uid) if guild else None
                if not m: continue
                dm = await m.create_dm()
                await dm.send(
                    content=(
                        f"You've been selected for **{act}** at **{when_text}** in {guild.name if guild else 'server'}.\n"
                        f"Tap **Confirm** to lock your spot."
                    ),
                    view=ConfirmView(mid=mid, uid=uid),
                )
                sent += 1
            except Exception as e:
                print("DM failed:", e)

        # DM any pre-slotted players we didn't DM above (info-only)
        pre_dmed = set(candidates)
        p_sent = 0
        for uid in data.get("players", []) or []:
            try:
                if uid in pre_dmed: continue
                m = guild.get_member(uid) if guild else None
                if not m: continue
                dm = await m.create_dm()
                content = (
                    f"You're pre-slotted as a **Player** for **{act}** at **{when_text}** in {guild.name if guild else 'server'}.\n"
                    "No action needed. If you can't make it, please let the promoter know."
                )
                await dm.send(content=content)
                p_sent += 1
            except Exception as e:
                print("Pre-slot DM failed:", e)

        # Build a concise status summary for the promoter
        status_lines = [
            f"Scheduled **{act}**.",
            f"DMed {sent} queued player(s), notified {p_sent} pre-slotted participant(s).",
            f"Sherpa signup posted: {'Yes' if posted_sherpa_signup else 'No'}" + (f" (fallback in <#{sherpa_signup_fallback}>)" if sherpa_signup_fallback else ""),
            f"General-sherpa announcement: {'Yes' if posted_general_announce else 'No'}" + (f" (fallback in <#{general_announce_fallback}>)" if general_announce_fallback else ""),
        ]
        await interaction.followup.send("\n".join(status_lines), ephemeral=True)

    except Exception as e:
        print("/schedule command error:", e)
        try:
            await interaction.followup.send("An error occurred while scheduling the event. Check the bot logs.", ephemeral=True)
        except Exception:
            try:
                await interaction.response.send_message("An error occurred while scheduling the event. Check the bot logs.", ephemeral=True)
            except Exception:
                pass

# ---------------------------
# /event — Player-Created Signup (with Sherpa Requests)
# ---------------------------

@bot.tree.command(name="event", description="Create a player event signup with requested Sherpas and LFG notify")
@app_commands.describe(
    activity="Activity name",
    encounter="(Optional) encounter/preset image selector",
    datetime="Date and time (single field, e.g., 10-05 19:00)",
    timezone="Timezone (dropdown)",
    requested_sherpas="Number of Sherpas requested (>= 0)",
    notes="(Optional) special instructions",
    voice_channel="(Optional) voice channel for meetup",
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
async def event_cmd(
    interaction: discord.Interaction,
    activity: str,
    datetime: str,
    timezone: str,
    requested_sherpas: int,
    encounter: Optional[str] = None,
    notes: Optional[str] = None,
    voice_channel: Optional[discord.VoiceChannel] = None,
):
    try:
        await interaction.response.defer(ephemeral=True)
    except Exception:
        pass

    # Channel safety
    if not EVENT_SIGNUP_CHANNEL_ID or not LFG_CHAT_CHANNEL_ID:
        await interaction.followup.send("Event channels are not configured. Ask an admin to set EVENT_SIGNUP_CHANNEL_ID and LFG_CHAT_CHANNEL_ID.", ephemeral=True)
        return

    # Resolve activity and capacity
    act, sug = _resolve_activity(activity)
    if not act:
        hint = (" Try: " + ", ".join(sug)) if sug else ""
        await interaction.followup.send(f"Unknown activity.{hint}", ephemeral=True)
        return
    cap = _cap_for_activity(act)

    # Parse date
    try:
        date_part, time_part = datetime.strip().split()
        year = datetime_module.datetime.now().year
        date_full = f"{year}-{date_part}"
    except Exception:
        await interaction.followup.send("Invalid datetime format. Use MM-DD HH:MM.", ephemeral=True)
        return

    start_ts = _parse_date_time_to_epoch(date_full, time_part, tz_name=timezone)
    when_text = f"<t:{start_ts}:F> ({timezone})" if start_ts else "TBD"

    # Validate requested sherpas
    req_s = max(0, int(requested_sherpas))
    if req_s > max(0, cap - 1):
        req_s = max(0, cap - 1)
        try:
            await interaction.followup.send(f"requested_sherpas capped at {req_s} (capacity - 1).", ephemeral=True)
        except Exception:
            pass

    guild = interaction.guild
    promoter_id = interaction.user.id

    # Participants and backups
    players: List[int] = []
    backups: List[int] = []
    if EVENT_HOST_AUTOJOIN:
        players.append(promoter_id)

    data = {
        "format": "user_event",
        "guild_id": guild.id if guild else None,
        "activity": act,
        "encounter": encounter,
        "desc": notes or "",
        "when_text": when_text,
        "capacity": cap,
        "requested_sherpas": req_s,
        "players": players,
        "backups": backups,
        "sherpas": set(),
        "sherpa_backup": set(),
        "promoter_id": promoter_id,
        "signups_open": False,
        "channel_id": int(EVENT_SIGNUP_CHANNEL_ID),
        "start_ts": start_ts,
        "voice_channel_id": int(voice_channel.id) if voice_channel else None,
        "voice_name": getattr(voice_channel, "name", None) if voice_channel else None,
        "r_2h": False, "r_30m": False, "r_0m": False,
    }

    # Post embed to signup channel
    embed, f = await _render_event_embed(guild, act, data)
    ev_msg = await _send_to_channel_id(int(EVENT_SIGNUP_CHANNEL_ID), embed=embed, file=f)
    if not ev_msg:
        await interaction.followup.send("Failed to post event.", ephemeral=True)
        return

    # Add reactions: ✅ appears immediately for user events, plus 🔁 and ❌
    for emoji in ("✅", "🔁", "❌"):
        try: await ev_msg.add_reaction(emoji)
        except Exception: pass

    mid = ev_msg.id
    SCHEDULES[mid] = data

    # LFG announcement
    try:
        event_link = ev_msg.jump_url
    except Exception:
        event_link = None
    lfg_lines = [
        "@everyone",
        f"{act} — {when_text}",
        f"Slots: {cap} • Sherpas requested: {req_s}",
        "Tap the embed to ✅ Join or 🔁 Backup. New players welcome!",
        event_link or "",
    ]
    content = "\n".join([ln for ln in lfg_lines if ln])
    await _send_to_channel_id(LFG_CHAT_CHANNEL_ID, content=content)

    # Optional Sherpa ping if requested
    if req_s > 0 and SHERPA_ASSISTANT_ROLE_ID:
        await _send_to_channel_id(LFG_CHAT_CHANNEL_ID, content=f"<@&{SHERPA_ASSISTANT_ROLE_ID}> — Need {req_s} Sherpa(s) for this run.")

    await interaction.followup.send("Event posted.", ephemeral=True)

# ---------------------------
# Reactions
# ---------------------------

@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if bot.user and payload.user_id == bot.user.id:
        return
    # Normalize emoji to string once
    emoji_str = str(payload.emoji)

    # Sherpa alert claim (✅ or 🔁 on the sherpa signup message in RAID_SIGN_UP_CHANNEL)
    for mid, data in list(SCHEDULES.items()):
        alert_id = int(data.get("sherpa_alert_message_id")) if data.get("sherpa_alert_message_id") else None
        alert_ch = int(data.get("sherpa_alert_channel_id")) if data.get("sherpa_alert_channel_id") else None
        if alert_id and payload.message_id == alert_id and (alert_ch is None or payload.channel_id == alert_ch):
            # Only allow ✅ and 🔁 on the Sherpa signup alert
            if emoji_str in ("✅", "🔁"):
                guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
                if not guild: return
                member = guild.get_member(payload.user_id)
                if not member or not _is_sherpa_assistant(member):
                    return
                reserved = int(data.get("reserved_sherpas", 0))
                sherpas: Set[int] = data.get("sherpas")  # type: ignore
                backup: Set[int] = data.get("sherpa_backup")  # type: ignore
                if emoji_str == "✅":
                    # Dedup across lists
                    exists = _user_in_any_event_list(data, member.id)
                    if exists in (None, "sherpas"):
                        if len(sherpas) < reserved and member.id not in sherpas:
                            sherpas.add(member.id)
                        else:
                            backup.add(member.id)
                    await _update_schedule_message(guild, int(mid))
                    try:
                        dm = await member.create_dm()
                        when_text = data.get("when_text"); activity = data.get("activity")
                        await dm.send(
                            content=(
                                f"You've claimed a Sherpa slot for **{activity}** at **{when_text}**.\n"
                                "Tap **Confirm Sherpa** to lock your Sherpa slot."
                            ),
                            view=SherpaConfirmView(mid=int(mid), uid=member.id),
                        )
                    except Exception:
                        pass
                    return
                elif emoji_str == "🔁":
                    if _user_in_any_event_list(data, member.id) is None:
                        backup.add(member.id)
                        await _update_schedule_message(guild, int(mid))
                    return
            else:
                # Remove any non-whitelisted reactions on the Sherpa signup alert
                try:
                    guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
                    channel = bot.get_channel(payload.channel_id) if payload.channel_id else None
                    if channel:
                        msg = await channel.fetch_message(payload.message_id)
                        user = None
                        if guild:
                            user = guild.get_member(payload.user_id)
                        if not user:
                            try:
                                user = await bot.fetch_user(payload.user_id)
                            except Exception:
                                user = None
                        if user:
                            try:
                                await msg.remove_reaction(payload.emoji, user)
                            except Exception:
                                pass
                except Exception:
                    pass
                return

    # Sherpa-only event reactions
    data = SCHEDULES.get(payload.message_id)
    if data and str(data.get("type")) == "sherpa_only":
        guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
        if not guild:
            return
        member = guild.get_member(payload.user_id)
        if not member:
            return
        # Only Sherpas can join/backup/leave
        if not _is_sherpa(member):
            return
        sherpas: Set[int] = data.get("sherpas") or set()  # type: ignore
        sbackup: List[int] = data.get("sherpa_backup") or []  # type: ignore
        cap = int(data.get("capacity", 0))

        if str(payload.emoji) == "✅":
            if member.id not in sherpas and member.id not in sbackup:
                if len(sherpas) < cap:
                    sherpas.add(member.id); data["sherpas"] = sherpas
                else:
                    sbackup.append(member.id); data["sherpa_backup"] = sbackup
            await _update_schedule_message(guild, int(payload.message_id))
            return

        if str(payload.emoji) == "🔁":
            if member.id not in sherpas and member.id not in sbackup:
                sbackup.append(member.id); data["sherpa_backup"] = sbackup
            await _update_schedule_message(guild, int(payload.message_id))
            return

        if str(payload.emoji) == "❌":
            changed = False
            if member.id in sherpas:
                sherpas.discard(member.id); data["sherpas"] = sherpas; changed = True
                # Auto promote
                promoted = None
                if sbackup:
                    promoted = sbackup.pop(0); data["sherpa_backup"] = sbackup
                    sherpas.add(promoted); data["sherpas"] = sherpas
                await _update_schedule_message(guild, int(payload.message_id))
                # DM promoted
                if promoted:
                    try:
                        m = guild.get_member(promoted)
                        if m:
                            d = await m.create_dm()
                            await d.send(f"You've been promoted from backup to Sherpa for **{data.get('activity')}** at **{data.get('when_text') or _format_title_when(data.get('start_ts'), data.get('timezone'))}**.")
                    except Exception:
                        pass
                return
            if member.id in sbackup:
                data["sherpa_backup"] = [x for x in sbackup if x != member.id]; changed = True
                await _update_schedule_message(guild, int(payload.message_id))
                return

    # For the main event embed created by /schedule, allow only specific reactions
    # Whitelist: 📝, 🔁, ✅, ❌. Remove any others users add.
    data = SCHEDULES.get(payload.message_id)
    if data and ("reserved_sherpas" in data) and str(data.get("format") or "") != "user_event":
        allowed_emojis = {"📝", "🔁", "✅", "❌"}
        if emoji_str not in allowed_emojis:
            try:
                guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
                channel = bot.get_channel(payload.channel_id) if payload.channel_id else None
                if channel:
                    msg = await channel.fetch_message(payload.message_id)
                    user = None
                    if guild:
                        user = guild.get_member(payload.user_id)
                    if not user:
                        try:
                            user = await bot.fetch_user(payload.user_id)
                        except Exception:
                            user = None
                    if user:
                        try:
                            await msg.remove_reaction(payload.emoji, user)
                        except Exception:
                            pass
            except Exception:
                pass
            return

        # Prevent Sherpas from using main event reactions; direct them to Sherpa signup
        try:
            guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
            if guild:
                member = guild.get_member(payload.user_id)
                if member and _is_sherpa(member):
                    channel = bot.get_channel(payload.channel_id) if payload.channel_id else None
                    if channel:
                        try:
                            msg = await channel.fetch_message(payload.message_id)
                            await msg.remove_reaction(payload.emoji, member)
                        except Exception:
                            pass
                    # DM the member to use the Sherpa signup instead
                    try:
                        d = await member.create_dm()
                        alert_mid = int(data.get("sherpa_alert_message_id")) if data.get("sherpa_alert_message_id") else None  # type: ignore
                        alert_ch = int(data.get("sherpa_alert_channel_id")) if data.get("sherpa_alert_channel_id") else None  # type: ignore
                        link = None
                        if alert_mid and alert_ch:
                            ch = bot.get_channel(alert_ch) or await bot.fetch_channel(alert_ch)
                            if ch:
                                try:
                                    m = await ch.fetch_message(alert_mid)
                                    link = m.jump_url
                                except Exception:
                                    link = None
                        await d.send(
                            ("Sherpas should use the dedicated Sherpa signup post to claim slots." + (f"\nLink: {link}" if link else ""))
                        )
                    except Exception:
                        pass
                    return
        except Exception:
            pass

    # 📝 on main event message → add as backup
    if emoji_str in ("📝", "🔁"):
        data = SCHEDULES.get(payload.message_id)
        if not data: return
        guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
        if not guild: return
        participants: List[int] = data.get("players", [])  # type: ignore
        backups: List[int] = data.get("backups", [])  # type: ignore
        if _user_in_any_event_list(data, payload.user_id) is None:
            backups.append(payload.user_id)
            await _update_schedule_message(guild, int(payload.message_id))
        return

    # ✅ on main event message
    if emoji_str == "✅":
        data = SCHEDULES.get(payload.message_id)
        if not data: return
        guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
        if not guild: return
        participants: List[int] = data.get("players", [])  # type: ignore
        backups: List[int] = data.get("backups", [])  # type: ignore
        cap = int(data.get("capacity", 0))
        reserved = int(data.get("reserved_sherpas", 0))
        player_slots = max(0, cap - reserved)

        if not data.get("signups_open"):
            # Before T-2h, ✅ acts as backup intent with cross-list dedupe
            exists = _user_in_any_event_list(data, payload.user_id)
            if exists is None:
                backups.append(payload.user_id)
            else:
                try: print("skip add pre-open ✅:", payload.user_id, "already in", exists)
                except Exception: pass
            await _update_schedule_message(guild, int(payload.message_id))
            return

        # After open: ✅ tries to join as player; else backup
        if _user_in_any_event_list(data, payload.user_id) is not None:
            await _update_schedule_message(guild, int(payload.message_id)); return
        if len(participants) < player_slots:
            participants.append(payload.user_id)
        else:
            backups.append(payload.user_id)
        await _update_schedule_message(guild, int(payload.message_id))
        return

    # ❌ on main event message → leave players/backups
    if str(payload.emoji) == "❌":
        data = SCHEDULES.get(payload.message_id)
        if not data: return
        guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
        if not guild: return
        participants: List[int] = data.get("players", [])  # type: ignore
        backups: List[int] = data.get("backups", [])  # type: ignore
        removed = False
        if payload.user_id in participants:
            participants[:] = [x for x in participants if x != payload.user_id]; removed = True
            moved = _autofill_from_backups(data)
            await _dm_promoted_users(guild, moved, data)
        if payload.user_id in backups:
            backups[:] = [x for x in backups if x != payload.user_id]; removed = True
        if removed: await _update_schedule_message(guild, int(payload.message_id))
        return

@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    data = SCHEDULES.get(payload.message_id)
    if not data:
        return
    guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
    if not guild:
        return

    # Sherpa-only event reaction removals
    if str(data.get("type")) == "sherpa_only":
        member = guild.get_member(payload.user_id)
        if not member:
            return
        sherpas: Set[int] = data.get("sherpas") or set()  # type: ignore
        sbackup: List[int] = data.get("sherpa_backup") or []  # type: ignore
        cap = int(data.get("capacity", 0))
        if str(payload.emoji) == "✅":
            if payload.user_id in sherpas:
                sherpas.discard(payload.user_id); data["sherpas"] = sherpas
                # Fill from backup
                promoted = None
                if sbackup and len(sherpas) < cap:
                    promoted = sbackup.pop(0); data["sherpa_backup"] = sbackup
                    sherpas.add(promoted); data["sherpas"] = sherpas
                await _update_schedule_message(guild, int(payload.message_id))
                if promoted:
                    try:
                        m = guild.get_member(promoted)
                        if m:
                            d = await m.create_dm()
                            await d.send(f"You've been promoted from backup to Sherpa for **{data.get('activity')}** at **{data.get('when_text') or _format_title_when(data.get('start_ts'), data.get('timezone'))}**.")
                    except Exception:
                        pass
                return
        if str(payload.emoji) == "🔁":
            if payload.user_id in sbackup:
                data["sherpa_backup"] = [x for x in sbackup if x != payload.user_id]
                await _update_schedule_message(guild, int(payload.message_id))
                return

    if str(payload.emoji) == "✅":
        if data.get("signups_open"):
            participants: List[int] = data.get("players", [])  # type: ignore
            if payload.user_id in participants:
                participants[:] = [x for x in participants if x != payload.user_id]
                moved = _autofill_from_backups(data)
                await _dm_promoted_users(guild, moved, data)
                await _update_schedule_message(guild, int(payload.message_id))
        else:
            backups: List[int] = data.get("backups", [])  # type: ignore
            if payload.user_id in backups:
                backups[:] = [x for x in backups if x != payload.user_id]
                await _update_schedule_message(guild, int(payload.message_id))
        return

# ---------------------------
# /event_sherpa
# ---------------------------

@bot.tree.command(name="event_sherpa", description="Create a Sherpa-only signup post with reminders and announcement")
@sherpa_host_only()
@app_commands.describe(
    activity="Activity name",
    datetime_str="Date and time (MM-DD HH:MM, 24h)",
    timezone="Timezone (dropdown)",
    slots="Number of Sherpas needed",
    voice_channel="(Optional) voice channel for meetup",
    notes="(Optional) Extra details",
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
async def event_sherpa_cmd(
    interaction: discord.Interaction,
    activity: str,
    datetime_str: str,
    timezone: str = "America/New_York",
    slots: int = 2,
    voice_channel: Optional[discord.VoiceChannel] = None,
    notes: Optional[str] = None,
):
    try:
        await interaction.response.defer(ephemeral=True)
    except Exception:
        pass

    guild = interaction.guild
    if not guild:
        await interaction.followup.send("Use this in a server.", ephemeral=True)
        return

    act, sug = _resolve_activity(activity)
    if not act:
        hint = (" Try: " + ", ".join(sug)) if sug else ""
        await interaction.followup.send(f"Unknown activity.{hint}", ephemeral=True)
        return

    # Parse datetime_str (MM-DD HH:MM) with current year
    try:
        date_part, time_part = datetime_str.strip().split()
        year = datetime.now().year
        date_full = f"{year}-{date_part}"
    except Exception:
        await interaction.followup.send("Invalid datetime format. Use MM-DD HH:MM.", ephemeral=True)
        return

    start_ts = _parse_date_time_to_epoch(date_full, time_part, tz_name=timezone)
    when_text = _format_title_when(start_ts, timezone)

    cap_limit = _cap_for_activity(act)
    capacity = max(1, min(int(slots or 1), cap_limit))

    # Target channel: #raid-sign-up
    channel_id = RAID_SIGN_UP_CHANNEL_ID or interaction.channel_id

    # Initialize data store
    host_id = interaction.user.id
    sherpa_set: Set[int] = set([host_id])
    data = {
        "type": "sherpa_only",
        "guild_id": guild.id,
        "channel_id": int(channel_id),
        "activity": act,
        "capacity": capacity,
        "sherpas": sherpa_set,
        "sherpa_backup": [],
        "host_id": host_id,
        "voice_channel_id": int(voice_channel.id) if voice_channel else None,
        "voice_name": getattr(voice_channel, "name", None) if voice_channel else None,
        "notes": (notes or "").strip(),
        "start_ts": start_ts,
        "timezone": timezone,
        "when_text": when_text,
        "r_2h": False, "r_30m": False, "r_0m": False,
    }

    # Post embed
    embed, f = await _render_sherpa_only_embed(guild, act, data)
    msg = await _send_to_channel_id(int(channel_id), embed=embed, file=f)
    if not msg:
        await interaction.followup.send("Failed to post Sherpa-only signup. Configure RAID_SIGN_UP_CHANNEL_ID or run in a channel.", ephemeral=True)
        return

    # Add reactions
    for emoji in ("✅", "🔁", "❌"):
        try:
            await msg.add_reaction(emoji)
        except Exception:
            pass

    # Persist image URL if Discord re-hosted the attachment and convert to embed-only image
    try:
        if msg.embeds and msg.embeds[0].image and msg.embeds[0].image.url:
            data["image_url"] = msg.embeds[0].image.url
            embed_cdn, _ = await _render_sherpa_only_embed(guild, act, data)
            try:
                await msg.edit(embed=embed_cdn, attachments=[])
            except Exception:
                try:
                    await msg.edit(embed=embed_cdn)
                except Exception:
                    pass
    except Exception:
        pass
    SCHEDULES[int(msg.id)] = data
    # Re-render to force embed to use CDN-hosted image and strip attachment file
    try:
        await _update_schedule_message(guild, int(msg.id))
    except Exception:
        pass

    # Announcement in #general-sherpa
    announce_ok = False
    try:
        link = msg.jump_url
    except Exception:
        link = None
    if GENERAL_SHERPA_CHANNEL_ID:
        try:
            # Prefer explicit role id; otherwise try to resolve by name in this guild
            ping_text = None
            if SHERPA_ROLE_ID:
                ping_text = f"<@&{int(SHERPA_ROLE_ID)}>"
            else:
                try:
                    sherpa_role = discord.utils.find(lambda r: r.name.lower().startswith("sherpa"), guild.roles)
                    if sherpa_role:
                        ping_text = f"<@&{sherpa_role.id}>"
                except Exception:
                    ping_text = None
            emb = discord.Embed(
                title=f"Sherpa Run — {act}",
                description=(
                    f"📅 {when_text}\n"
                    f"🎯 Slots: {capacity} Sherpas\n"
                    f"✅ React on the signup embed to join or 🔁 for backup.\n"
                    + (f"\n[Link to signup]({link})" if link else "")
                ).strip(),
                color=_activity_color(act),
            )
            await _send_to_channel_id(int(GENERAL_SHERPA_CHANNEL_ID), content=ping_text, embed=emb)
            announce_ok = True
        except Exception:
            announce_ok = False

    await interaction.followup.send(
        f"Posted Sherpa signup in <#{int(channel_id)}> with {capacity} slot(s). " + ("Announced in #general-sherpa." if announce_ok else ""),
        ephemeral=True,
    )

# ---------------------------
# Error handler
# ---------------------------

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: Exception):
    try:
        if interaction.response.is_done():
            await interaction.followup.send(f"Error: {error.__class__.__name__}: {error}", ephemeral=True)
        else:
            await interaction.response.send_message(f"Error: {error.__class__.__name__}: {error}", ephemeral=True)
    except Exception:
        pass

# ---------------------------
# Boot
# ---------------------------

if __name__ == "__main__":
    token = get_token("DISCORD_TOKEN")
    bot.run(token)
