# GoonBot main.py — queues, check-in, promotions, scheduling
# Exact behavior:
# - /list is a separate command from /schedule: its own sessions (LIST_SESSIONS), DMs, embed, and Next/Done controls
# - Main Event Embed -> EVENT_SIGNUP_CHANNEL_ID (aka RAID_DUNGEON_EVENT_SIGNUP_CHANNEL_ID)
# - Sherpa Signup Embed -> RAID_SIGN_UP_CHANNEL_ID (✅ to claim Sherpa; overflow -> Sherpa Backup)
# - Sherpa Announcement -> GENERAL_SHERPA_CHANNEL_ID (pings SHERPA_ROLE_ID if set; points to Sherpa signup post)
# - T-2h before start (if player slots remain): add ✅ to main embed + single LFG nudge in LFG_CHAT_CHANNEL_ID
# - DM unchecked queue users with Confirm buttons; confirming as participant adds a queue ✅
# - Colors based on category; optional activity images from ./assets/** by fuzzy filename match
# - Reminders at T-2h, T-30m, and start; survey DM 3h after start

import os
import asyncio
import json
import re
import uuid
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
BUILD_OF_THE_WEEK_CHANNEL_ID  = _env_int("BUILD_OF_THE_WEEK_CHANNEL_ID")  # Build submissions channel

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
        global GENERAL_SHERPA_CHANNEL_ID, RAID_SIGN_UP_CHANNEL_ID, GENERAL_CHANNEL_ID, LFG_CHAT_CHANNEL_ID, RAID_QUEUE_CHANNEL_ID, EVENT_SIGNUP_CHANNEL_ID, WELCOME_CHANNEL_ID, BUILD_OF_THE_WEEK_CHANNEL_ID
        gs = _to_int(data.get("GENERAL_SHERPA_CHANNEL_ID"))
        rs = _to_int(data.get("RAID_SIGN_UP_CHANNEL_ID"))
        gc = _to_int(data.get("GENERAL_CHANNEL_ID"))
        lf = _to_int(data.get("LFG_CHAT_CHANNEL_ID"))
        rq = _to_int(data.get("RAID_QUEUE_CHANNEL_ID"))
        ev = _to_int(data.get("EVENT_SIGNUP_CHANNEL_ID")) or _to_int(data.get("RAID_DUNGEON_EVENT_SIGNUP_CHANNEL_ID"))
        wc = _to_int(data.get("WELCOME_CHANNEL_ID"))
        bw = _to_int(data.get("BUILD_OF_THE_WEEK_CHANNEL_ID"))
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
        if bw and not BUILD_OF_THE_WEEK_CHANNEL_ID:
            BUILD_OF_THE_WEEK_CHANNEL_ID = bw
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
# Active /list sessions (separate from SCHEDULES — not events, not schedule DMs)
LIST_SESSIONS: Dict[str, Dict[str, object]] = {}
LIST_RUNS_BY_CHANNEL: Dict[int, str] = {}
LIST_RUNS_MSG_TO_SESSION: Dict[int, str] = {}
LIST_REPOST_DEBOUNCE: Dict[int, float] = {}
QUEUES: Dict[str, List[int]] = {}
CHECKED: Dict[str, Set[int]] = {}
# activity -> users who requested catty/weapon run while queued
CATTY_RUNS: Dict[str, Set[int]] = {}
# activity -> { user_id -> cooldown_until_epoch }
COOLDOWNS: Dict[str, Dict[int, int]] = {}
# (channel_id, user_id) -> last auto-help reply epoch seconds
HELP_REPLY_LAST_SENT: Dict[Tuple[int, int], int] = {}
HELP_REPLY_COOLDOWN_SECONDS = 20
_ACTIVITY_ALIAS_CACHE: Optional[Dict[str, str]] = None
HELP_QUEUE_CONFIRM_PENDING: Dict[Tuple[int, int], Dict[str, object]] = {}
HELP_QUEUE_CONFIRM_TTL_SECONDS = 180
HELP_REMINDER_FOOTER = "Don't forget to message @GFerryGoon."

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

def _ensure_catty(activity: str) -> Set[int]:
    return CATTY_RUNS.setdefault(activity, set())

def _queue_members_needing_prompt(activity: str) -> List[int]:
    checked = CHECKED.get(activity, set()) or set()
    return [int(uid) for uid in (QUEUES.get(activity, []) or []) if int(uid) not in checked]

async def _mark_queue_participants_checked(activity: Optional[str], user_ids: List[int]) -> bool:
    if not activity or not user_ids:
        return False
    act = str(activity)
    q = QUEUES.get(act, []) or []
    if not q:
        return False
    queued = {int(uid) for uid in q}
    checked = _ensure_checked(act)
    before = len(checked)
    for uid in user_ids:
        try:
            uid_int = int(uid)
        except Exception:
            continue
        if uid_int in queued:
            checked.add(uid_int)
    if len(checked) == before:
        return False
    await persist_checked()
    await _post_activity_board(act)
    return True

def _cap_for_activity(activity: str) -> int:
    """Return player capacity based on presets, with sensible fallbacks.

    Guarantees:
    - Activities listed under `raids` in presets have capacity 6
    - Activities listed under `dungeons` in presets have capacity 3
    """
    act = activity or ""

    # Primary: exact membership in presets
    try:
        if act in (PRESETS.get("raids") or []):
            return 6
        if act in (PRESETS.get("dungeons") or []):
            return 3

        # Secondary: normalized text match (strip emojis/symbols/case)
        norm = _normalize_activity_text(act)
        raid_norms = {_normalize_activity_text(a) for a in (PRESETS.get("raids") or [])}
        dungeon_norms = {_normalize_activity_text(a) for a in (PRESETS.get("dungeons") or [])}
        if norm in raid_norms:
            return 6
        if norm in dungeon_norms:
            return 3
    except Exception:
        # If presets are missing or malformed, fall through to heuristics
        pass

    # Heuristic fallback by keywords (kept broad, errs toward raid=6)
    a = act.lower()
    if any(k in a for k in ("raid", "vault", "wish", "garden", "crota", "salvation", "vow", "king", "root", "nightmare", "edge", "desert", "pantheon", "calus")):
        return 6
    if any(k in a for k in ("dungeon", "pit", "spire", "deep", "watcher", "throne", "prophecy", "grasp", "duality", "ghost", "warlord", "ruin", "sunder", "doctrine", "vesper", "host", "avarice")):
        return 3
    return 6

def _is_sherpa(member: discord.Member) -> bool:
    try:
        # Prefer explicit role id when configured; fall back to name matching.
        # IMPORTANT: avoid treating "Sherpa Assistant" as a full Sherpa role.
        if SHERPA_ROLE_ID:
            try:
                rid = int(SHERPA_ROLE_ID)
                if any(r.id == rid for r in member.roles):
                    return True
            except Exception:
                pass
        for r in member.roles:
            name = (r.name or "").lower().strip()
            if name.startswith("sherpa") and "assistant" not in name:
                return True
        return False
    except Exception:
        return False

def _is_sherpa_assistant(member: discord.Member) -> bool:
    try:
        # If a role id is configured but stale/wrong, still fall back to name matching
        # so assistants aren't blocked from signup.
        if SHERPA_ASSISTANT_ROLE_ID:
            try:
                rid = int(SHERPA_ASSISTANT_ROLE_ID)
                if any(r.id == rid for r in member.roles):
                    return True
            except Exception:
                pass
        for r in member.roles:
            name = (r.name or "").lower().strip()
            # Be permissive (servers vary: "Sherpa Assistant", "Sherpa Assistants", etc.)
            if "sherpa" in name and "assistant" in name:
                return True
        return False
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
    cur_raw = (current or "").strip()
    cur = cur_raw.lower()
    cur_norm = _normalize_activity_text(cur_raw)

    def _match_score(act: str) -> int:
        if not cur:
            return 0
        act_low = act.lower()
        act_norm = _normalize_activity_text(act)
        if act_low.startswith(cur) or act_norm.startswith(cur_norm):
            return 3
        if cur in act_low or (cur_norm and cur_norm in act_norm):
            return 2
        for alias, target in _activity_alias_map().items():
            if target == act and (cur == alias or cur_norm == _normalize_activity_text(alias)):
                return 4
        return 0

    ranked: List[Tuple[int, str]] = []
    for act in ALL_ACTIVITIES:
        score = _match_score(act)
        if not cur or score > 0:
            ranked.append((score, act))

    if cur:
        ranked.sort(key=lambda item: (-item[0], item[1].lower()))
    else:
        ranked.sort(key=lambda item: item[1].lower())

    out: List[app_commands.Choice[str]] = []
    for _, act in ranked[:25]:
        out.append(app_commands.Choice(name=act, value=act))
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

def _is_raid_or_dungeon(activity: str) -> bool:
    """Best-effort raid/dungeon classifier for activity-specific options."""
    act = activity or ""
    try:
        if act in (PRESETS.get("raids") or []):
            return True
        if act in (PRESETS.get("dungeons") or []):
            return True
        norm = _normalize_activity_text(act)
        raid_norms = {_normalize_activity_text(a) for a in (PRESETS.get("raids") or [])}
        dungeon_norms = {_normalize_activity_text(a) for a in (PRESETS.get("dungeons") or [])}
        return norm in raid_norms or norm in dungeon_norms
    except Exception:
        pass
    a = act.lower()
    return any(k in a for k in ("raid", "vault", "wish", "garden", "crota", "salvation", "vow", "king", "root", "nightmare", "pantheon", "calus")) or any(
        k in a for k in ("dungeon", "pit", "spire", "deep", "watcher", "throne", "prophecy", "grasp", "duality", "ghost", "warlord", "ruin", "avarice")
    )

async def _send_to_channel_id(
    channel_id: Optional[int],
    content: Optional[str] = None,
    *,
    embed: Optional[discord.Embed] = None,
    file: Optional[discord.File] = None,
    allowed_mentions: Optional[discord.AllowedMentions] = None,
    view: Optional[discord.ui.View] = None,
):
    try:
        if not channel_id:
            return None
        ch = bot.get_channel(int(channel_id)) or await bot.fetch_channel(int(channel_id))
        if not ch:
            return None
        kwargs: Dict[str, object] = {}
        if allowed_mentions is not None:
            kwargs["allowed_mentions"] = allowed_mentions
        if view is not None:
            kwargs["view"] = view
        if file and embed:
            return await ch.send(content=content, embed=embed, file=file, **kwargs)  # type: ignore[arg-type]
        if embed:
            return await ch.send(content=content, embed=embed, **kwargs)  # type: ignore[arg-type]
        return await ch.send(content=content, **kwargs)  # type: ignore[arg-type]
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

def _help_channel_ids() -> Set[int]:
    ids: Set[int] = set()
    for cid in (GENERAL_CHANNEL_ID, LFG_CHAT_CHANNEL_ID):
        try:
            if cid:
                ids.add(int(cid))
        except Exception:
            pass
    return ids

def _channel_mention_or_fallback(channel_id: Optional[int], fallback: str) -> str:
    try:
        if channel_id:
            return f"<#{int(channel_id)}>"
    except Exception:
        pass
    return fallback

def _help_reply_rate_limited(channel_id: int, user_id: int) -> bool:
    """Return True if we should skip replying due to short cooldown."""
    try:
        now = int(datetime.utcnow().timestamp())
        key = (int(channel_id), int(user_id))
        last = int(HELP_REPLY_LAST_SENT.get(key, 0) or 0)
        if (now - last) < int(HELP_REPLY_COOLDOWN_SECONDS):
            return True
        HELP_REPLY_LAST_SENT[key] = now
        # Keep this small and bounded over long uptimes.
        if len(HELP_REPLY_LAST_SENT) > 5000:
            cutoff = now - 6 * 60 * 60
            for k, ts in list(HELP_REPLY_LAST_SENT.items()):
                if int(ts) < cutoff:
                    HELP_REPLY_LAST_SENT.pop(k, None)
        return False
    except Exception:
        return False

def _find_activity_by_terms(*terms: str) -> Optional[str]:
    norm_terms = [_normalize_activity_text(t) for t in terms if t]
    if not norm_terms:
        return None
    for act in ALL_ACTIVITIES:
        norm_act = _normalize_activity_text(act)
        if norm_act and all(t in norm_act for t in norm_terms):
            return act
    return None

def _activity_alias_map() -> Dict[str, str]:
    global _ACTIVITY_ALIAS_CACHE
    if _ACTIVITY_ALIAS_CACHE is not None:
        return _ACTIVITY_ALIAS_CACHE

    aliases: Dict[str, str] = {}

    # Auto-generate simple initialisms (e.g., "last wish" -> "lw").
    stop_words = {"of", "the", "and", "a", "an", "to", "for", "in", "on"}
    for act in ALL_ACTIVITIES:
        words = [w for w in _normalize_activity_text(act).split() if w and w not in stop_words]
        if len(words) >= 2:
            init = "".join(w[0] for w in words)
            if 2 <= len(init) <= 5:
                aliases.setdefault(init, act)

    # Common Destiny abbreviations (raids + dungeons).
    manual_alias_terms: List[Tuple[str, Tuple[str, ...]]] = [
        ("vog", ("vault", "glass")),
        ("lw", ("last", "wish")),
        ("gos", ("garden", "salvation")),
        ("dsc", ("deep", "stone", "crypt")),
        ("votd", ("vow", "disciple")),
        ("vow", ("vow", "disciple")),
        ("kf", ("king", "fall")),
        ("ron", ("root", "nightmare")),
        ("ce", ("crota", "end")),
        ("se", ("salvation", "edge")),
        ("calus", ("pantheon", "calus")),
        ("pantheon", ("pantheon", "calus")),
        ("sos", ("spire", "stars")),
        ("eow", ("eater", "world")),
        ("poh", ("pit", "heresy")),
        ("st", ("shattered", "throne")),
        ("goa", ("grasp", "avarice")),
        ("sotw", ("spire", "watcher")),
        ("gotd", ("ghost", "deep")),
        ("wr", ("warlord", "ruin")),
        ("vh", ("vesper", "host")),
        ("sd", ("sunder", "doctrine")),
    ]
    for alias, terms in manual_alias_terms:
        act = _find_activity_by_terms(*terms)
        if act:
            aliases[alias] = act

    _ACTIVITY_ALIAS_CACHE = aliases
    return aliases

def _extract_activity_from_help_text(message_text: str) -> Tuple[Optional[str], bool]:
    """Return (activity_name, matched_by_alias)."""
    norm_text = _normalize_activity_text(message_text or "")
    if not norm_text:
        return None, False

    # Full-name mention first (prefer longest activity name).
    sorted_activities = sorted(ALL_ACTIVITIES, key=lambda a: len(_normalize_activity_text(a)), reverse=True)
    for act in sorted_activities:
        norm_act = _normalize_activity_text(act)
        if not norm_act:
            continue
        if re.search(rf"\b{re.escape(norm_act)}\b", norm_text):
            return act, False

    # Abbreviation mention second.
    for alias, act in _activity_alias_map().items():
        if re.search(rf"\b{re.escape(alias)}\b", norm_text):
            return act, True
    return None, False

def _set_help_queue_confirm(channel_id: int, user_id: int, activity: str) -> None:
    try:
        now = int(datetime.utcnow().timestamp())
        HELP_QUEUE_CONFIRM_PENDING[(int(channel_id), int(user_id))] = {
            "activity": str(activity),
            "expires_at": now + int(HELP_QUEUE_CONFIRM_TTL_SECONDS),
        }
        # Keep pending confirmations bounded.
        if len(HELP_QUEUE_CONFIRM_PENDING) > 3000:
            for key, data in list(HELP_QUEUE_CONFIRM_PENDING.items()):
                try:
                    if int(data.get("expires_at", 0) or 0) < now:
                        HELP_QUEUE_CONFIRM_PENDING.pop(key, None)
                except Exception:
                    HELP_QUEUE_CONFIRM_PENDING.pop(key, None)
    except Exception:
        pass

def _get_help_queue_confirm(channel_id: int, user_id: int) -> Optional[str]:
    try:
        now = int(datetime.utcnow().timestamp())
        data = HELP_QUEUE_CONFIRM_PENDING.get((int(channel_id), int(user_id)))
        if not data:
            return None
        expires_at = int(data.get("expires_at", 0) or 0)
        if expires_at and now > expires_at:
            HELP_QUEUE_CONFIRM_PENDING.pop((int(channel_id), int(user_id)), None)
            return None
        activity = str(data.get("activity") or "").strip()
        return activity or None
    except Exception:
        return None

def _clear_help_queue_confirm(channel_id: int, user_id: int) -> None:
    try:
        HELP_QUEUE_CONFIRM_PENDING.pop((int(channel_id), int(user_id)), None)
    except Exception:
        pass

def _is_affirmative_help_reply(message_text: str) -> bool:
    txt = _normalize_activity_text(message_text or "")
    if not txt:
        return False
    affirm_set = {
        "y", "yes", "yea", "yeah", "yep", "yup", "ok", "okay", "sure", "please",
        "do it", "go ahead", "sounds good", "for sure", "affirmative",
    }
    if txt in affirm_set:
        return True
    return txt.startswith(("yes ", "yeah ", "yep ", "ok ", "okay ", "sure "))

def _is_negative_help_reply(message_text: str) -> bool:
    txt = _normalize_activity_text(message_text or "")
    if not txt:
        return False
    negative_set = {
        "n", "no", "nah", "nope", "not now", "cancel", "stop", "never mind",
    }
    if txt in negative_set:
        return True
    return txt.startswith(("no ", "nah ", "nope ", "not now "))

async def _reply_queue_confirmation_embed(
    message: discord.Message,
    *,
    activity: str,
    status: str,
    details: str,
    success: bool,
) -> None:
    embed = discord.Embed(
        title="Queue Confirmation",
        description=details,
        color=(0x57F287 if success else 0xED4245),
    )
    embed.add_field(name="Activity", value=activity, inline=True)
    embed.add_field(name="Status", value=status, inline=True)
    embed.set_footer(text=HELP_REMINDER_FOOTER)
    await message.reply(embed=embed, mention_author=False)

async def _join_queue_from_help_confirmation(message: discord.Message, activity: str) -> None:
    member = message.author if isinstance(message.author, discord.Member) else None
    uid = int(message.author.id)

    # Queue is for players; Sherpas and Assistants use Sherpa signup posts.
    if member and (_is_sherpa(member) or _is_sherpa_assistant(member)):
        await _reply_queue_confirmation_embed(
            message,
            activity=activity,
            status="Not Added",
            details="Sherpas and Sherpa Assistants cannot join player queues.",
            success=False,
        )
        return

    try:
        await load_queues()
        await load_checked()
        await load_catty()
    except Exception:
        pass

    act, _ = _resolve_activity(activity, list(ALL_ACTIVITIES) + list(QUEUES.keys()))
    if not act:
        await _reply_queue_confirmation_embed(
            message,
            activity=activity,
            status="Not Added",
            details="I couldn't find that activity queue. Try `/join` and pick from autocomplete.",
            success=False,
        )
        return

    # Enforce per-activity cooldown.
    try:
        now = int(datetime.utcnow().timestamp())
        cd_map = COOLDOWNS.get(act, {})
        until = int(cd_map.get(uid, 0) or 0)
        if until and now < until:
            remaining = until - now
            hrs = max(1, int((remaining + 3599) // 3600))
            await _reply_queue_confirmation_embed(
                message,
                activity=act,
                status="Cooldown Active",
                details=f"You can rejoin **{act}** in about {hrs} hour(s).",
                success=False,
            )
            return
    except Exception:
        pass

    in_any = [a for a, lst in QUEUES.items() if uid in lst]
    q = _ensure_queue(act)
    if act in in_any:
        pos = q.index(uid) + 1 if uid in q else len(q)
        await _reply_queue_confirmation_embed(
            message,
            activity=act,
            status="Already Queued",
            details=f"You're already in the queue at position **#{pos}** ({len(q)} total).",
            success=True,
        )
        return

    if len(in_any) >= 2:
        await _reply_queue_confirmation_embed(
            message,
            activity=act,
            status="Queue Limit Reached",
            details="You can be in at most **2** different activity queues at a time.",
            success=False,
        )
        return

    q.append(uid)
    _ensure_checked(act).discard(uid)
    _ensure_catty(act).discard(uid)
    await persist_queues()
    _append_queue_event("join", act, [uid])
    await persist_checked()
    await persist_catty()
    pos = q.index(uid) + 1
    await _post_activity_board(act)
    await _reply_queue_confirmation_embed(
        message,
        activity=act,
        status="Added to Queue",
        details=f"You're signed up for **{act}** at position **#{pos}** ({len(q)} total).",
        success=True,
    )

def _chat_help_reply(message_text: str, *, direct_bot_question: bool = False) -> Tuple[Optional[str], Optional[str]]:
    text = " ".join((message_text or "").lower().split())
    if not text:
        return None, None

    def has_any(*phrases: str) -> bool:
        return any(p in text for p in phrases)

    def with_footer(reply_text: str) -> str:
        return reply_text.rstrip() + f"\n\n{HELP_REMINDER_FOOTER}"

    has_question_tone = ("?" in text) or text.startswith(
        ("how ", "where ", "what ", "can ", "do ", "is ", "are ", "when ", "which ", "who ", "why ", "help")
    )
    asks_signup = has_any("sign up", "signup", "join", "register", "queue", "lfg", "sing up")
    asks_sign_up_for = has_any("how do i sign up for", "how to sign up for", "how can i sign up for", "sign me up for", "sing up for")
    asks_join_raid = has_any("how do i join a raid", "join raid", "join a raid")
    asks_how_to_join = has_any("how do i join", "how to join", "how can i join", "where do i join")
    asks_join_queue = has_any("how do i join the queue", "how to join the queue", "how can i join the queue", "join the queue", "can i join queue", "queue me")
    asks_want_activity = has_any(
        "i want to", "iwant to", "i wanna", "i wanna do", "wanna do", "i need", "i need to",
        "need to run", "need to do", "put me in", "put me on", "add me to", "queue me for",
        "im trying to", "i m trying to", "id like to", "i d like to", "i would like to", "im down for",
    )
    asks_where = has_any("where", "which channel", "what channel")
    asks_leave = has_any("leave", "cancel", "drop", "remove me", "step out")
    asks_commands = has_any("commands", "command", "slash", "bot command", "how to use bot")
    asks_reactions = has_any("reaction", "reactions", "emoji", "what does ✅", "what does 🔁", "what does 📝", "what does ❌", "backup")
    asks_queue_spot = has_any("queue position", "spot in queue", "where am i in queue", "am i queued", "queue spot")
    asks_sherpa = has_any("sherpa", "teach", "teaching run", "first time", "new player")
    asks_hosting = has_any("create event", "make event", "host run", "post event", "schedule run")
    asks_time = has_any("when", "what time", "start time", "timezone", "utc", "est", "pst", "cst", "mst")
    raid_or_dungeon = has_any("raid", "raids", "dungeon", "dungeons")
    activity_context = has_any(
        "raid", "raids", "dungeon", "dungeons", "lfg", "queue", "join", "signup", "sign up",
        "event", "events", "run", "runs", "sherpa", "fireteam", "activity"
    )

    signup_channel = _channel_mention_or_fallback(EVENT_SIGNUP_CHANNEL_ID, "the event-signup channel")
    queue_channel = _channel_mention_or_fallback(RAID_QUEUE_CHANNEL_ID, "the queue channel")
    lfg_channel = _channel_mention_or_fallback(LFG_CHAT_CHANNEL_ID, "#lfg")
    mentioned_activity, matched_by_alias = _extract_activity_from_help_text(text)

    if asks_commands and activity_context:
        return with_footer(
            "Common commands:\n"
            "• **/join** — join an activity queue\n"
            "• **/queue** — view current queues\n"
            "• **/leave** — leave a queue/signup\n"
            "• **/event** — create a player event signup\n"
            "• **/event_sherpa** — create a Sherpa-only signup\n"
            "If you’re unsure, ask your question and I’ll point you to the right command."
        ), None

    if (
        mentioned_activity
        and _is_raid_or_dungeon(mentioned_activity)
        and (asks_how_to_join or asks_signup or asks_join_raid or asks_join_queue or asks_sign_up_for or asks_want_activity)
    ):
        alias_hint = " (recognized from abbreviation)" if matched_by_alias else ""
        return with_footer(
            f"I see **{mentioned_activity}**{alias_hint}. Do you want to be in that queue?\n"
            f"If yes, reply **yes** and I'll sign you up for **{mentioned_activity}**.\n"
            f"Then use **/queue** (or check {queue_channel}) to confirm your spot."
        ), mentioned_activity

    if asks_join_raid:
        return with_footer(
            "To join a raid:\n"
            "1) Use **/join** and select the raid.\n"
            f"2) Use **/queue** to check your place (or watch {queue_channel}).\n"
            f"3) Watch {signup_channel} and react **✅** when your run is posted."
        ), None

    if asks_join_queue and not mentioned_activity:
        return with_footer(
            "I can queue you right here. Tell me which raid or dungeon (name or abbreviation like VOG/KF/RON), "
            "and I’ll ask you to confirm."
        ), None

    if asks_signup and raid_or_dungeon:
        return with_footer(
            "For raid/dungeon signups:\n"
            "1) Use **/join** and pick the activity.\n"
            f"2) Check your spot with **/queue** (or watch {queue_channel}).\n"
            f"3) Watch {signup_channel} for event posts and react **✅** to join or **🔁** for backup.\n"
            "Need to back out? Use **/leave**."
        ), None

    if asks_queue_spot and activity_context:
        return with_footer(f"Use **/queue** to see your current position. Queue boards are posted in {queue_channel}."), None

    if asks_where and any(k in text for k in ("sign", "join", "event", "raid", "dungeon")):
        return with_footer(
            f"Raid and dungeon events are posted in {signup_channel}, and chat/LFG updates happen in {lfg_channel}. "
            "Use **/join** to enter the queue, then **/queue** to see your position."
        ), None

    if asks_reactions and activity_context:
        return with_footer(
            "Reaction guide on event posts:\n"
            "• **✅** = join if a slot is open\n"
            "• **🔁** = backup/waitlist\n"
            "• **📝** = interest note (when enabled)\n"
            "• **❌** = leave/cancel your participation"
        ), None

    if asks_leave and any(k in text for k in ("raid", "dungeon", "queue", "run", "signup")):
        return with_footer("Use **/leave** to step out of a raid/dungeon queue or signup."), None

    if asks_sherpa and activity_context:
        return with_footer(
            "New/learning runs are welcome. Join with **/join**, then watch event posts in "
            f"{signup_channel}. If Sherpas are needed, those runs will call it out in {lfg_channel}."
        ), None

    if asks_hosting and activity_context:
        return with_footer(
            "To host, use:\n"
            "• **/event** for normal player signups\n"
            "• **/event_sherpa** for Sherpa-only signups\n"
            "If permissions block it, ask a founder/admin for event-host access."
        ), None

    if asks_time and activity_context:
        return with_footer(
            f"Event posts in {signup_channel} include the scheduled time. "
            "You’ll also get reminders before start when applicable."
        ), None

    if has_question_tone and (activity_context or direct_bot_question):
        return with_footer(
            "I can help with raid/dungeon/LFG questions. Try asking:\n"
            "• **How do I join a raid?**\n"
            "• **How do I sign up for VOG?**\n"
            "• **How do I join the queue for KF?**\n"
            "• **How do I sign up for raids?**\n"
            "• **Where are event posts?**\n"
            "• **How do I check queue position?**\n"
            "• **What do the reactions mean?**\n"
            "• **How do I leave the queue?**\n"
            "• **Which command should I use?**"
        ), None

    return None, None

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
QUEUE_EVENTS_FILE = os.path.join(DATA_DIR, "queue_events.jsonl")
QUEUE_BACKUP_DIR = os.path.join(DATA_DIR, "queue_backups")
QUEUE_EMPTY_MARKER_FILE = os.path.join(DATA_DIR, "queues.empty")
QUEUES_LOCK = asyncio.Lock()

# Persistent storage for green-check marks on queue users
CHECKED_FILE = os.path.join(DATA_DIR, "checked.json")
CHECKED_LOCK = asyncio.Lock()

# Persistent storage for queue users requesting catty/weapon runs
CATTY_FILE = os.path.join(DATA_DIR, "catty_runs.json")
CATTY_LOCK = asyncio.Lock()

# Persistent storage for queue cooldowns (per-activity)
COOLDOWN_FILE = os.path.join(DATA_DIR, "cooldowns.json")
COOLDOWNS_LOCK = asyncio.Lock()

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
def _queue_total(state: Optional[Dict[str, List[int]]]) -> int:
    try:
        return sum(len(v or []) for v in (state or {}).values())
    except Exception:
        return 0

def _normalize_queue_state(raw: object) -> Dict[str, List[int]]:
    out: Dict[str, List[int]] = {}
    try:
        for k, v in ((raw or {}) if isinstance(raw, dict) else {}).items():
            try:
                name = str(k)
                seen: Set[int] = set()
                ids: List[int] = []
                for x in (v or []):
                    uid = int(x)
                    if uid not in seen:
                        seen.add(uid)
                        ids.append(uid)
                out[name] = ids
            except Exception:
                continue
    except Exception:
        return {}
    return out

def _read_queue_json_file(path: str) -> Tuple[Dict[str, List[int]], bool]:
    try:
        if not os.path.isfile(path):
            return {}, False
        with open(path, "r") as f:
            raw = json.load(f)
        return _normalize_queue_state(raw), True
    except Exception:
        return {}, False

def _queue_empty_marker_exists() -> bool:
    try:
        return os.path.isfile(QUEUE_EMPTY_MARKER_FILE)
    except Exception:
        return False

def _set_queue_empty_marker(enabled: bool) -> None:
    try:
        if enabled:
            with open(QUEUE_EMPTY_MARKER_FILE, "w") as f:
                f.write(str(int(datetime.utcnow().timestamp())))
        elif os.path.isfile(QUEUE_EMPTY_MARKER_FILE):
            os.remove(QUEUE_EMPTY_MARKER_FILE)
    except Exception:
        pass

def _write_queue_backup(state: Dict[str, List[int]]) -> None:
    if _queue_total(state) <= 0:
        return
    try:
        _ensure_dir(QUEUE_BACKUP_DIR)
        stamp = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
        path = os.path.join(QUEUE_BACKUP_DIR, f"queues-{stamp}.json")
        with open(path, "w") as f:
            json.dump({str(k): [int(x) for x in (v or [])] for k, v in state.items()}, f)
            try:
                f.flush(); os.fsync(f.fileno())
            except Exception:
                pass
        try:
            backups = sorted(
                [
                    os.path.join(QUEUE_BACKUP_DIR, name)
                    for name in os.listdir(QUEUE_BACKUP_DIR)
                    if name.startswith("queues-") and name.endswith(".json")
                ],
                key=lambda p: os.path.getmtime(p),
            )
            for old in backups[:-25]:
                try:
                    os.remove(old)
                except Exception:
                    pass
        except Exception:
            pass
    except Exception as e:
        try:
            print("Queue backup failed:", e)
        except Exception:
            pass

def _read_latest_queue_backup() -> Dict[str, List[int]]:
    try:
        if not os.path.isdir(QUEUE_BACKUP_DIR):
            return {}
        backups = sorted(
            [
                os.path.join(QUEUE_BACKUP_DIR, name)
                for name in os.listdir(QUEUE_BACKUP_DIR)
                if name.startswith("queues-") and name.endswith(".json")
            ],
            key=lambda p: os.path.getmtime(p),
            reverse=True,
        )
        for path in backups:
            state, ok = _read_queue_json_file(path)
            if ok and _queue_total(state) > 0:
                return state
    except Exception:
        pass
    return {}

def _append_queue_event(action: str, activity: Optional[str] = None, user_ids: Optional[List[int]] = None) -> None:
    try:
        record = {
            "ts": int(datetime.utcnow().timestamp()),
            "action": str(action),
            "activity": str(activity) if activity else None,
            "user_ids": [int(uid) for uid in (user_ids or [])],
        }
        with open(QUEUE_EVENTS_FILE, "a") as f:
            f.write(json.dumps(record, separators=(",", ":")) + "\n")
            try:
                f.flush(); os.fsync(f.fileno())
            except Exception:
                pass
    except Exception as e:
        try:
            print("Queue event log failed:", e)
        except Exception:
            pass

def _replay_queue_events() -> Tuple[Dict[str, List[int]], bool]:
    state: Dict[str, List[int]] = {}
    saw_event = False
    try:
        if not os.path.isfile(QUEUE_EVENTS_FILE):
            return {}, False
        with open(QUEUE_EVENTS_FILE, "r") as f:
            for line in f:
                try:
                    evt = json.loads(line)
                    action = str(evt.get("action") or "")
                    act = str(evt.get("activity") or "") if evt.get("activity") else None
                    user_ids = [int(uid) for uid in (evt.get("user_ids") or [])]
                    saw_event = True
                    if action in ("join", "add") and act:
                        q = state.setdefault(act, [])
                        for uid in user_ids:
                            if uid not in q:
                                q.append(uid)
                    elif action in ("leave", "remove"):
                        if act:
                            q = state.setdefault(act, [])
                            state[act] = [uid for uid in q if uid not in set(user_ids)]
                        else:
                            uid_set = set(user_ids)
                            for key in list(state.keys()):
                                state[key] = [uid for uid in state.get(key, []) if uid not in uid_set]
                    elif action == "clear":
                        if act:
                            state[act] = []
                        else:
                            state = {}
                except Exception:
                    continue
    except Exception:
        return {}, False
    return state, saw_event

def _read_queues_from_disk() -> Dict[str, List[int]]:
    # Prefer new data dir path; fall back to legacy file near this module.
    state, ok = _read_queue_json_file(QUEUES_FILE)
    if not ok:
        legacy = os.path.join(os.path.dirname(__file__), "queues.json")
        state, ok = _read_queue_json_file(legacy)
    if ok and _queue_total(state) > 0:
        return state
    if _queue_empty_marker_exists():
        return state if ok else {}
    journal_state, saw_journal = _replay_queue_events()
    if saw_journal:
        if _queue_total(journal_state) > 0:
            try:
                print("Recovered queues from event log")
            except Exception:
                pass
            return journal_state
        return state if ok else {}
    backup_state = _read_latest_queue_backup()
    if _queue_total(backup_state) > 0:
        try:
            print("Recovered queues from backup snapshot")
        except Exception:
            pass
        return backup_state
    return state if ok else {}

def _merge_queue_states(existing: Dict[str, List[int]], incoming: Dict[str, List[int]]) -> Dict[str, List[int]]:
    merged: Dict[str, List[int]] = {}
    for act in set(existing.keys()) | set(incoming.keys()):
        seen: Set[int] = set()
        ordered: List[int] = []
        for uid in list(existing.get(act, []) or []) + list(incoming.get(act, []) or []):
            try:
                uid_int = int(uid)
            except Exception:
                continue
            if uid_int in seen:
                continue
            seen.add(uid_int)
            ordered.append(uid_int)
        merged[str(act)] = ordered
    return merged

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
        _write_queue_backup(serializable)
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

async def persist_queues(*, allow_removals: bool = False) -> None:
    async with QUEUES_LOCK:
        state = {str(k): [int(x) for x in (v or [])] for k, v in QUEUES.items()}
        if not allow_removals:
            state = _merge_queue_states(_read_queues_from_disk(), state)
            QUEUES.clear()
            for k, v in state.items():
                QUEUES[k] = list(v)
        _write_queues_to_disk(state)
        _set_queue_empty_marker(bool(allow_removals and _queue_total(state) == 0))

async def load_queues() -> None:
    async with QUEUES_LOCK:
        loaded = _read_queues_from_disk()
        QUEUES.clear()
        for k, v in loaded.items():
            QUEUES[k] = list(v)

def _parse_queue_board_embed(embed: discord.Embed) -> Tuple[Optional[str], List[int], Set[int], Set[int]]:
    try:
        title = str(getattr(embed, "title", "") or "")
        m_title = re.match(r"^\s*Queue\s*(?:—|-|:)\s*(.+?)\s*$", title, flags=re.IGNORECASE)
        if not m_title:
            return None, [], set(), set()
        activity = m_title.group(1).strip()
        q: List[int] = []
        checked: Set[int] = set()
        catty: Set[int] = set()
        all_candidate_lines: List[str] = []
        for field in getattr(embed, "fields", []) or []:
            name = str(getattr(field, "name", "") or "").lower()
            value_lines = str(getattr(field, "value", "") or "").splitlines()
            all_candidate_lines.extend(value_lines)
            if not any(token in name for token in ("player", "signed", "queue")):
                continue
            for line in value_lines:
                m = re.search(r"<@!?(\d+)>", line)
                if not m:
                    continue
                uid = int(m.group(1))
                if uid not in q:
                    q.append(uid)
                if "✅" in line:
                    checked.add(uid)
                if "⭐" in line:
                    catty.add(uid)
        # Fallback for older embeds whose player field had a different name.
        if not q:
            for line in all_candidate_lines:
                m = re.search(r"<@!?(\d+)>", line)
                if not m:
                    continue
                uid = int(m.group(1))
                if uid not in q:
                    q.append(uid)
                if "✅" in line:
                    checked.add(uid)
                if "⭐" in line:
                    catty.add(uid)
        return activity or None, q, checked, catty
    except Exception:
        return None, [], set(), set()

def _queue_restore_summary() -> str:
    parts = [f"{act}: {len(q or [])}" for act, q in sorted(QUEUES.items()) if q]
    if not parts:
        return "No activity queues restored."
    summary = "; ".join(parts)
    if len(summary) > 1500:
        summary = summary[:1497] + "..."
    return summary

async def _recover_queues_from_queue_boards(
    *,
    include_older_nonempty: bool = False,
    replace_existing: bool = False,
    prefer_fullest: bool = False,
    prefer_oldest: bool = False,
    include_all_names: bool = False,
    activity_filter: Optional[str] = None,
    history_limit: int = 1000,
) -> bool:
    if not RAID_QUEUE_CHANNEL_ID:
        return False
    try:
        ch = bot.get_channel(int(RAID_QUEUE_CHANNEL_ID)) or await bot.fetch_channel(int(RAID_QUEUE_CHANNEL_ID))
    except Exception:
        return False
    recovered: Dict[str, List[int]] = {}
    recovered_checked: Dict[str, Set[int]] = {}
    recovered_catty: Dict[str, Set[int]] = {}
    seen_activities: Set[str] = set()
    filter_norm = _normalize_activity_text(activity_filter) if activity_filter else ""
    try:
        async for msg in ch.history(limit=int(history_limit)):  # type: ignore[attr-defined]
            for embed in getattr(msg, "embeds", []) or []:
                activity, q, checked, catty = _parse_queue_board_embed(embed)
                if not activity:
                    continue
                if filter_norm and filter_norm not in _normalize_activity_text(activity):
                    continue
                if include_all_names:
                    if q:
                        existing = recovered.setdefault(activity, [])
                        for uid in q:
                            if uid not in existing:
                                existing.append(uid)
                        recovered_checked.setdefault(activity, set()).update(checked)
                        recovered_catty.setdefault(activity, set()).update(catty)
                    continue
                if prefer_fullest:
                    if q and len(q) > len(recovered.get(activity, []) or []):
                        recovered[activity] = q
                        recovered_checked[activity] = checked
                        recovered_catty[activity] = catty
                    continue
                if prefer_oldest:
                    # History scans newest -> oldest, so overwriting with each
                    # non-empty board leaves the oldest non-empty board found.
                    if q:
                        recovered[activity] = q
                        recovered_checked[activity] = checked
                        recovered_catty[activity] = catty
                    continue
                if activity in seen_activities:
                    continue
                # The newest board for an activity is authoritative. If it is
                # empty, do not resurrect older signups for that activity.
                if not q:
                    if not include_older_nonempty:
                        seen_activities.add(activity)
                    continue
                seen_activities.add(activity)
                recovered[activity] = q
                recovered_checked[activity] = checked
                recovered_catty[activity] = catty
    except Exception as e:
        try:
            print("Queue board recovery failed:", e)
        except Exception:
            pass
        return False
    if _queue_total(recovered) <= 0:
        return False
    if replace_existing:
        QUEUES.clear()
        CHECKED.clear()
        CATTY_RUNS.clear()
    for act, q in recovered.items():
        QUEUES[act] = list(q)
        CHECKED[act] = set(recovered_checked.get(act, set()))
        CATTY_RUNS[act] = set(recovered_catty.get(act, set()))
    try:
        print(f"Recovered queues from queue board embeds: {sorted(recovered.keys())}")
    except Exception:
        pass
    return True

async def _preview_queue_board_history(
    *,
    activity_filter: Optional[str] = None,
    history_limit: int = 2000,
) -> Tuple[int, int, List[str]]:
    if not RAID_QUEUE_CHANNEL_ID:
        return 0, 0, ["RAID_QUEUE_CHANNEL_ID is not configured."]
    try:
        ch = bot.get_channel(int(RAID_QUEUE_CHANNEL_ID)) or await bot.fetch_channel(int(RAID_QUEUE_CHANNEL_ID))
    except Exception as e:
        return 0, 0, [f"Could not read queue channel: {e.__class__.__name__}"]
    filter_norm = _normalize_activity_text(activity_filter) if activity_filter else ""
    messages_seen = 0
    boards_seen = 0
    lines: List[str] = []
    try:
        async for msg in ch.history(limit=int(history_limit)):  # type: ignore[attr-defined]
            messages_seen += 1
            for embed in getattr(msg, "embeds", []) or []:
                activity, q, checked, catty = _parse_queue_board_embed(embed)
                if not activity:
                    continue
                if filter_norm and filter_norm not in _normalize_activity_text(activity):
                    continue
                boards_seen += 1
                title = str(getattr(embed, "title", "") or activity)
                msg_id = int(getattr(msg, "id", 0) or 0)
                lines.append(
                    f"{boards_seen}. {title} — players={len(q)} checks={len(checked)} stars={len(catty)} msg={msg_id}"
                )
                if len(lines) >= 25:
                    return messages_seen, boards_seen, lines
    except Exception as e:
        lines.append(f"History scan failed: {e.__class__.__name__}")
    return messages_seen, boards_seen, lines


# ---------------
# Checked persistence
# ---------------
def _read_checked_from_disk() -> Dict[str, Set[int]]:
    try:
        path = CHECKED_FILE
        if not os.path.isfile(path):
            legacy = os.path.join(os.path.dirname(__file__), "checked.json")
            if os.path.isfile(legacy):
                path = legacy
            else:
                return {}
        with open(path, "r") as f:
            raw = json.load(f)
        out: Dict[str, Set[int]] = {}
        for k, v in (raw or {}).items():
            try:
                name = str(k)
                ids = {int(x) for x in (v or [])}
                out[name] = ids
            except Exception:
                continue
        return out
    except Exception:
        return {}

def _write_checked_to_disk(state: Dict[str, Set[int]]) -> None:
    try:
        tmp_path = f"{CHECKED_FILE}.tmp"
        serializable = {str(k): [int(x) for x in (v or set())] for k, v in state.items()}
        with open(tmp_path, "w") as f:
            json.dump(serializable, f)
            try:
                f.flush(); os.fsync(f.fileno())
            except Exception:
                pass
        os.replace(tmp_path, CHECKED_FILE)
        try:
            dir_fd = os.open(os.path.dirname(CHECKED_FILE) or ".", os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except Exception:
            pass
    except Exception as e:
        try:
            print("Checked write failed:", e)
        except Exception:
            pass

async def persist_checked() -> None:
    async with CHECKED_LOCK:
        _write_checked_to_disk(CHECKED)

async def load_checked() -> None:
    async with CHECKED_LOCK:
        loaded = _read_checked_from_disk()
        if loaded:
            for k, v in loaded.items():
                CHECKED[k] = set(v)


# ---------------
# Catty/weapon run persistence
# ---------------
def _read_catty_from_disk() -> Dict[str, Set[int]]:
    try:
        path = CATTY_FILE
        if not os.path.isfile(path):
            legacy = os.path.join(os.path.dirname(__file__), "catty_runs.json")
            if os.path.isfile(legacy):
                path = legacy
            else:
                return {}
        with open(path, "r") as f:
            raw = json.load(f)
        out: Dict[str, Set[int]] = {}
        for k, v in (raw or {}).items():
            try:
                name = str(k)
                ids = {int(x) for x in (v or [])}
                out[name] = ids
            except Exception:
                continue
        return out
    except Exception:
        return {}

def _write_catty_to_disk(state: Dict[str, Set[int]]) -> None:
    try:
        tmp_path = f"{CATTY_FILE}.tmp"
        serializable = {str(k): [int(x) for x in (v or set())] for k, v in state.items()}
        with open(tmp_path, "w") as f:
            json.dump(serializable, f)
            try:
                f.flush(); os.fsync(f.fileno())
            except Exception:
                pass
        os.replace(tmp_path, CATTY_FILE)
        try:
            dir_fd = os.open(os.path.dirname(CATTY_FILE) or ".", os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except Exception:
            pass
    except Exception as e:
        try:
            print("Catty write failed:", e)
        except Exception:
            pass

async def persist_catty() -> None:
    async with CATTY_LOCK:
        _write_catty_to_disk(CATTY_RUNS)

async def load_catty() -> None:
    async with CATTY_LOCK:
        loaded = _read_catty_from_disk()
        if loaded:
            for k, v in loaded.items():
                CATTY_RUNS[k] = set(v)


# ---------------
# Cooldown persistence (per-activity, user -> epoch_until)
# ---------------
def _read_cooldowns_from_disk() -> Dict[str, Dict[int, int]]:
    try:
        path = COOLDOWN_FILE
        if not os.path.isfile(path):
            legacy = os.path.join(os.path.dirname(__file__), "cooldowns.json")
            if os.path.isfile(legacy):
                path = legacy
            else:
                return {}
        with open(path, "r") as f:
            raw = json.load(f)
        out: Dict[str, Dict[int, int]] = {}
        for act, users in (raw or {}).items():
            try:
                act_name = str(act)
                m: Dict[int, int] = {}
                for uid_str, until in (users or {}).items():
                    try:
                        uid = int(uid_str)
                        m[uid] = int(until)
                    except Exception:
                        continue
                out[act_name] = m
            except Exception:
                continue
        return out
    except Exception:
        return {}

def _write_cooldowns_to_disk(state: Dict[str, Dict[int, int]]) -> None:
    try:
        tmp_path = f"{COOLDOWN_FILE}.tmp"
        serializable: Dict[str, Dict[str, int]] = {}
        for act, mapping in (state or {}).items():
            serializable[str(act)] = {str(int(uid)): int(until) for uid, until in (mapping or {}).items()}
        with open(tmp_path, "w") as f:
            json.dump(serializable, f)
            try:
                f.flush(); os.fsync(f.fileno())
            except Exception:
                pass
        os.replace(tmp_path, COOLDOWN_FILE)
        try:
            dir_fd = os.open(os.path.dirname(COOLDOWN_FILE) or ".", os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except Exception:
            pass
    except Exception as e:
        try:
            print("Cooldown write failed:", e)
        except Exception:
            pass

async def persist_cooldowns() -> None:
    async with COOLDOWNS_LOCK:
        _write_cooldowns_to_disk(COOLDOWNS)

async def load_cooldowns() -> None:
    async with COOLDOWNS_LOCK:
        loaded = _read_cooldowns_from_disk()
        if loaded:
            for act, mapping in loaded.items():
                COOLDOWNS[act] = dict(mapping)


# ---------------
# Build of the Week persistence
# ---------------
BUILDS_FILE = os.path.join(DATA_DIR, "builds.json")
BUILDS_LOCK = asyncio.Lock()

# In-memory cache for builds data
BUILDS_DATA: Dict[str, object] = {"builds": [], "winners": [], "current_week_start": None}

def _get_current_week_start() -> str:
    """Return the Monday of the current week as YYYY-MM-DD string."""
    today = datetime.now()
    days_since_monday = today.weekday()  # Monday=0
    monday = today - timedelta(days=days_since_monday)
    return monday.strftime("%Y-%m-%d")

def _week_start_for_date(d: datetime) -> str:
    """Return the Monday of the given date's week as YYYY-MM-DD string."""
    days_since_monday = d.weekday()  # Monday=0
    monday = d - timedelta(days=days_since_monday)
    return monday.strftime("%Y-%m-%d")

def _parse_date_input_any(date_str: Optional[str]) -> Optional[datetime]:
    """
    Parse a user-provided date in a few common formats.

    Supported:
    - YYYY-MM-DD (preferred)
    - YYYY/M/D
    - M-D-YYYY
    - M/D/YYYY
    """
    if not date_str:
        return None
    s = str(date_str).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m-%d-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    # Allow single-digit month/day without zero padding in the same formats.
    # (datetime.strptime already handles single digits for %m/%d on most platforms,
    # but this is a safe fallback for edge cases like "12-8-2025".)
    m = re.match(r"^\s*(\d{1,2})[/-](\d{1,2})[/-](\d{4})\s*$", s)
    if m:
        try:
            mm = int(m.group(1)); dd = int(m.group(2)); yy = int(m.group(3))
            return datetime(yy, mm, dd)
        except Exception:
            return None
    return None

def _parse_week_start_from_input(date_str: Optional[str]) -> Optional[str]:
    """
    Parse a user-provided date and return the Monday of that week as YYYY-MM-DD.
    Returns None if date_str is not provided or invalid.
    """
    d = _parse_date_input_any(date_str)
    if not d:
        return None
    return _week_start_for_date(d)

def _week_bounds_utc(week_start: str) -> Tuple[datetime, datetime]:
    """Return [start, end) bounds in UTC for the given week_start (YYYY-MM-DD)."""
    start = datetime.strptime(str(week_start).strip(), "%Y-%m-%d").replace(tzinfo=datetime_module.timezone.utc)
    end = start + timedelta(days=7)
    return start, end

async def _fetch_thread_starter_message(thread: discord.Thread) -> Optional[discord.Message]:
    """
    Best-effort fetch of the starter message for a forum thread.
    """
    # Fast path that sometimes works for forum posts
    try:
        return await thread.fetch_message(int(thread.id))
    except Exception:
        pass
    # Fallback: oldest message in the thread
    try:
        async for m in thread.history(limit=1, oldest_first=True):
            return m
    except Exception:
        return None

def _extract_week_of_from_embed(embed: discord.Embed) -> Optional[str]:
    try:
        footer = getattr(embed, "footer", None)
        text = getattr(footer, "text", None) if footer else None
        if not text:
            return None
        m = re.search(r"\bWeek of\s+(\d{4}-\d{2}-\d{2})\b", str(text))
        return m.group(1) if m else None
    except Exception:
        return None

def _extract_user_id_from_embed(embed: discord.Embed) -> Optional[int]:
    try:
        for f in (embed.fields or []):
            name = str(getattr(f, "name", "") or "")
            if "submitted by" in name.lower():
                val = str(getattr(f, "value", "") or "")
                m = re.search(r"<@!?(\d+)>", val)
                if m:
                    return int(m.group(1))
        return None
    except Exception:
        return None

def _extract_activity_from_embed(embed: discord.Embed) -> Optional[str]:
    try:
        for f in (embed.fields or []):
            name = str(getattr(f, "name", "") or "")
            if "activity" in name.lower():
                v = str(getattr(f, "value", "") or "").strip()
                return v or None
        return None
    except Exception:
        return None

async def _scan_builds_in_forum_for_week(channel: discord.ForumChannel, week_start: str) -> List[Dict[str, object]]:
    """
    Scan a forum channel for build posts for the specified week.
    Uses the build embed footer ("Week of YYYY-MM-DD") when available, otherwise falls back to thread.created_at bounds.
    """
    start_dt, end_dt = _week_bounds_utc(week_start)
    threads: List[discord.Thread] = []
    try:
        threads.extend(list(channel.threads or []))
    except Exception:
        pass
    # Include archived threads (recent weeks may already be archived)
    try:
        async for t in channel.archived_threads(limit=200):
            try:
                threads.append(t)
            except Exception:
                continue
    except Exception:
        pass
    # Deduplicate by thread id
    dedup: Dict[int, discord.Thread] = {}
    for t in threads:
        try:
            dedup[int(t.id)] = t
        except Exception:
            continue

    builds: List[Dict[str, object]] = []
    for thread in dedup.values():
        created_at = getattr(thread, "created_at", None)
        if created_at and created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=datetime_module.timezone.utc)
        in_range = bool(created_at and (start_dt <= created_at < end_dt))
        # If the thread is clearly out of range, skip fetching unless we need to rely on the embed footer.
        # (Fetching every starter message can be slow in big forums.)
        if not in_range:
            continue

        starter = await _fetch_thread_starter_message(thread)
        if not starter:
            continue
        embed = None
        try:
            embed = starter.embeds[0] if starter.embeds else None
        except Exception:
            embed = None

        embed_week = _extract_week_of_from_embed(embed) if embed else None
        # If the embed declares a week, trust it (it is how /build tags submissions).
        if embed_week and embed_week != week_start:
            continue
        # If no embed week, only keep it if created_at is within the week bounds (already checked).

        user_id = _extract_user_id_from_embed(embed) if embed else None
        activity = _extract_activity_from_embed(embed) if embed else None
        builds.append(
            {
                "id": str(thread.id),
                "message_id": int(getattr(starter, "id", 0) or 0),
                "thread_id": int(thread.id),
                "channel_id": int(channel.id),
                "user_id": int(user_id) if user_id else None,
                "build_title": str(getattr(thread, "name", "") or ""),
                "submitted_at": int(starter.created_at.timestamp()) if getattr(starter, "created_at", None) else 0,
                "week_of": week_start,
                "activity": activity or "Unknown",
            }
        )
    return builds

def _read_builds_from_disk() -> Dict[str, object]:
    try:
        if not os.path.isfile(BUILDS_FILE):
            return {"builds": [], "winners": [], "current_week_start": None}
        with open(BUILDS_FILE, "r") as f:
            data = json.load(f)
        # Ensure required keys exist
        if "builds" not in data:
            data["builds"] = []
        if "winners" not in data:
            data["winners"] = []
        if "current_week_start" not in data:
            data["current_week_start"] = None
        return data
    except Exception:
        return {"builds": [], "winners": [], "current_week_start": None}

def _write_builds_to_disk(data: Dict[str, object]) -> None:
    try:
        tmp_path = f"{BUILDS_FILE}.tmp"
        with open(tmp_path, "w") as f:
            json.dump(data, f, indent=2)
            try:
                f.flush()
                os.fsync(f.fileno())
            except Exception:
                pass
        os.replace(tmp_path, BUILDS_FILE)
        try:
            dir_fd = os.open(os.path.dirname(BUILDS_FILE) or ".", os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except Exception:
            pass
    except Exception as e:
        try:
            print("Builds write failed:", e)
        except Exception:
            pass

async def load_builds() -> None:
    global BUILDS_DATA
    async with BUILDS_LOCK:
        BUILDS_DATA = _read_builds_from_disk()

async def persist_builds() -> None:
    async with BUILDS_LOCK:
        _write_builds_to_disk(BUILDS_DATA)

async def add_build(build: Dict[str, object]) -> None:
    """Add a new build submission to storage."""
    global BUILDS_DATA
    async with BUILDS_LOCK:
        # Reload from disk to ensure we have latest data
        BUILDS_DATA = _read_builds_from_disk()
        builds_list = BUILDS_DATA.get("builds", [])
        if not isinstance(builds_list, list):
            builds_list = []
        builds_list.append(build)
        BUILDS_DATA["builds"] = builds_list
        _write_builds_to_disk(BUILDS_DATA)

async def get_builds_for_week(week_start: Optional[str] = None) -> List[Dict[str, object]]:
    """Get all builds for a specific week (defaults to current week)."""
    async with BUILDS_LOCK:
        BUILDS_DATA_LOCAL = _read_builds_from_disk()
        if week_start is None:
            week_start = _get_current_week_start()
        builds_list = BUILDS_DATA_LOCAL.get("builds", [])
        if not isinstance(builds_list, list):
            return []
        return [b for b in builds_list if b.get("week_of") == week_start]

async def add_winner(winner: Dict[str, object]) -> None:
    """Add a winner record to storage."""
    global BUILDS_DATA
    async with BUILDS_LOCK:
        BUILDS_DATA = _read_builds_from_disk()
        winners_list = BUILDS_DATA.get("winners", [])
        if not isinstance(winners_list, list):
            winners_list = []
        winners_list.append(winner)
        BUILDS_DATA["winners"] = winners_list
        _write_builds_to_disk(BUILDS_DATA)

async def delete_build(message_id: int) -> Optional[Dict[str, object]]:
    """Delete a build from storage by message ID. Returns the deleted build or None."""
    global BUILDS_DATA
    async with BUILDS_LOCK:
        BUILDS_DATA = _read_builds_from_disk()
        builds_list = BUILDS_DATA.get("builds", [])
        if not isinstance(builds_list, list):
            return None
        
        # Find and remove the build
        deleted_build = None
        new_builds = []
        for build in builds_list:
            if build.get("message_id") == message_id:
                deleted_build = build
            else:
                new_builds.append(build)
        
        if deleted_build:
            BUILDS_DATA["builds"] = new_builds
            _write_builds_to_disk(BUILDS_DATA)
        
        return deleted_build

async def delete_build_by_thread(thread_id: int) -> Optional[Dict[str, object]]:
    """Delete a build from storage by thread ID. Returns the deleted build or None."""
    global BUILDS_DATA
    async with BUILDS_LOCK:
        BUILDS_DATA = _read_builds_from_disk()
        builds_list = BUILDS_DATA.get("builds", [])
        if not isinstance(builds_list, list):
            return None
        
        # Find and remove the build by thread_id
        deleted_build = None
        new_builds = []
        for build in builds_list:
            if build.get("thread_id") == thread_id:
                deleted_build = build
            else:
                new_builds.append(build)
        
        if deleted_build:
            BUILDS_DATA["builds"] = new_builds
            _write_builds_to_disk(BUILDS_DATA)
        
        return deleted_build

# Load Destiny 2 data for build command
def _load_destiny_data() -> Dict[str, object]:
    try:
        path = os.path.join(os.path.dirname(__file__), "destiny_data.json")
        if not os.path.isfile(path):
            return {}
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}

DESTINY_DATA = _load_destiny_data()


def _normalize_choice(value: str, options: List[str]) -> Optional[str]:
    """Return the canonical option matching the user-provided value."""
    if not value:
        return None
    cleaned = value.strip().lower()
    for option in options:
        if option.lower() == cleaned:
            return option
    return None


def _get_aspect_slot_count(aspect_name: str, subclass: str) -> Optional[int]:
    slots_data = DESTINY_DATA.get("aspect_slots", {})
    default_slots = slots_data.get("default", {})
    prism_slots = slots_data.get("prismatic_overrides", {})
    if subclass == "Prismatic":
        return prism_slots.get(aspect_name, default_slots.get(aspect_name))
    return default_slots.get(aspect_name)


def _split_csv_list(raw_value: str) -> List[str]:
    if not raw_value:
        return []
    parts: List[str] = []
    for chunk in raw_value.replace("\n", ",").split(","):
        cleaned = chunk.strip()
        if cleaned:
            parts.append(cleaned)
    return parts


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

def _is_list_host_or_founder(interaction: discord.Interaction, data: Optional[Dict[str, object]] = None) -> bool:
    """Permission check for /list controls only (not shared with /schedule)."""
    try:
        uid = int(interaction.user.id)
        if FOUNDER_USER_ID and uid == int(FOUNDER_USER_ID):
            return True
        if data and data.get("type") == "list_run" and data.get("host_id") and int(data["host_id"]) == uid:  # type: ignore[arg-type]
            return True
    except Exception:
        pass
    return False

def _is_promoter_or_founder(interaction: discord.Interaction, data: Optional[Dict[str, object]] = None) -> bool:
    try:
        uid = int(interaction.user.id)
        if FOUNDER_USER_ID and uid == int(FOUNDER_USER_ID):
            return True
        if data and "promoter_id" in data and int(data["promoter_id"]) == uid:
            return True
        # Allow sherpa-only host to act as promoter for permissions
        if data and "host_id" in data and int(data["host_id"]) == uid:
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
        if data.get("difficulty"):
            embed.add_field(name="Difficulty", value=str(data.get("difficulty")), inline=True)

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

    # Display team occupancy counting Players + Sherpas + (Host if not listed)
    if not is_user_event:
        promoter_id = int(data.get("promoter_id")) if data.get("promoter_id") else None  # type: ignore
        players_set = set(int(p) for p in players)
        sherpas_set = set(int(s) for s in sherpas)
        team_count = len(players_set) + len(sherpas_set)
        if promoter_id is not None and promoter_id not in players_set and promoter_id not in sherpas_set:
            team_count += 1
        embed.add_field(name="Capacity", value=f"{team_count}/{cap}", inline=True)

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
            # Show only the number of listed Players here to avoid confusion.
            # Overall occupancy (Players + Sherpas + Host-if-not-listed) is shown in the Capacity field above.
            embed.add_field(name=f"Players ({len(players)})", value="\n".join(f"<@{p}>" for p in players), inline=False)
    if backups:
        if is_user_event:
            embed.add_field(name=f"Backup ({len(backups)})", value="\n".join(f"– <@{b}>" for b in backups), inline=False)
        else:
            embed.add_field(name=f"Backups ({len(backups)})", value="\n".join(f"<@{b}>" for b in backups), inline=False)

    if is_user_event and desc:
        embed.add_field(name="Notes", value=desc, inline=False)

    # Preserve previously uploaded image if known (ignore attachment:// placeholders)
    try:
        img_url = data.get("image_url")
        if img_url and not str(img_url).startswith("attachment://"):
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

    # Preserve previously uploaded image if known (ignore attachment:// placeholders)
    try:
        img_url = data.get("image_url")
        if img_url and not str(img_url).startswith("attachment://"):
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
    # Load queues/checked from disk once
    if not getattr(bot, "_queues_loaded", False):  # type: ignore[attr-defined]
        try:
            await load_queues()
            await load_checked()
            await load_catty()
            await load_cooldowns()
            await load_builds()
            try:
                if _queue_total(QUEUES) <= 0 and await _recover_queues_from_queue_boards():
                    await persist_queues()
                    await persist_checked()
                    await persist_catty()
            except Exception as e:
                try: print("Queue board recovery skipped:", e)
                except Exception: pass
            bot._queues_loaded = True  # type: ignore[attr-defined]
            print("Queues, checked, catty, and builds loaded from disk")
        except Exception as e:
            print("Queue/checked/catty/builds load failed:", e)
    if not getattr(bot, "_sched_task", None):
        bot._sched_task = bot.loop.create_task(_scheduler_loop())  # type: ignore[attr-defined]
    if not getattr(bot, "_autosave_task", None):
        bot._autosave_task = bot.loop.create_task(_autosave_loop())  # type: ignore[attr-defined]
    if not getattr(bot, "_list_views_registered", False):
        try:
            for session_id, data in list(LIST_SESSIONS.items()):
                if str(data.get("status")) == "done":
                    continue
                _register_list_views(str(session_id), list(data.get("line") or []))  # type: ignore[arg-type]
            bot._list_views_registered = True  # type: ignore[attr-defined]
        except Exception as e:
            try:
                print("List view registration failed:", e)
            except Exception:
                pass
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
                        "• /schedule — founder-only: creates a one-time scheduled event from the queue\n"
                        "• /list — founder-only: repeat runs with a line, Next/Done groups, and Sherpa fill"
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
                "• /schedule — founder-only: creates a one-time scheduled event from the queue\n"
                "• /list — founder-only: repeat runs with a line, Next/Done groups, and Sherpa fill\n\n"
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

@bot.event
async def on_message(message: discord.Message):
    try:
        if message.author.bot:
            return
        channel_id = getattr(message.channel, "id", None)
        if not channel_id:
            return

        if EVENT_SIGNUP_CHANNEL_ID and int(channel_id) == int(EVENT_SIGNUP_CHANNEL_ID):
            await _maybe_repost_list_control(int(channel_id))

        if int(channel_id) not in _help_channel_ids():
            return

        pending_activity = _get_help_queue_confirm(int(channel_id), int(message.author.id))
        if pending_activity:
            if _is_affirmative_help_reply(message.content or ""):
                _clear_help_queue_confirm(int(channel_id), int(message.author.id))
                await _join_queue_from_help_confirmation(message, pending_activity)
                return
            if _is_negative_help_reply(message.content or ""):
                _clear_help_queue_confirm(int(channel_id), int(message.author.id))
                await message.reply(
                    f"No problem. If you change your mind, use **/join** for **{pending_activity}**.\n\n{HELP_REMINDER_FOOTER}",
                    mention_author=False,
                )
                return

        is_direct_bot_question = False
        try:
            if bot.user:
                is_direct_bot_question = bot.user.mentioned_in(message)
        except Exception:
            is_direct_bot_question = False

        reply, pending_prompt_activity = _chat_help_reply(message.content or "", direct_bot_question=is_direct_bot_question)
        if reply:
            if _help_reply_rate_limited(int(channel_id), int(message.author.id)):
                return
            await message.reply(reply, mention_author=False)
            if pending_prompt_activity:
                _set_help_queue_confirm(int(channel_id), int(message.author.id), pending_prompt_activity)
    except Exception as e:
        try: print("chat help reply failed:", e)
        except Exception: pass
    finally:
        # Keep prefix-command behavior intact when on_message is present.
        await bot.process_commands(message)

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
    catty = _ensure_catty(activity)
    embed = discord.Embed(title=f"Queue — {activity}", color=_activity_color(activity))
    embed.add_field(name="Signed Up", value=str(len(q)), inline=True)
    if q:
        # Annotate queue symbols for schedule/check state and catty/weapon requests.
        note = "\n\n✅ = scheduled participant\n⭐ = needs catty/weapon run"
        lines = [f"<@{uid}>{' ✅' if uid in checked else ''}{' ⭐' if uid in catty else ''}" for uid in q]
        value = "\n".join(lines) + (note if any(uid in checked or uid in catty for uid in q) else "")
        embed.add_field(name="Players (in order)", value=value, inline=False)
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
@app_commands.describe(
    activity="Choose an activity to join",
    catty_weapon_run="Need catty/weapon run? (Yes/No)",
)
@app_commands.choices(
    catty_weapon_run=[
        app_commands.Choice(name="No", value="no"),
        app_commands.Choice(name="Yes", value="yes"),
    ]
)
@app_commands.autocomplete(activity=_activity_autocomplete)
async def join_cmd(
    interaction: discord.Interaction,
    activity: str,
    catty_weapon_run: str = "no",
):
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    # Queue is for players; Sherpas and Assistants should use Sherpa signup posts instead.
    if member and (_is_sherpa(member) or _is_sherpa_assistant(member)):
        await interaction.response.send_message("Sherpas and Sherpa Assistants cannot join queues.", ephemeral=True)
        return
    act, sug = _resolve_activity(activity)
    if not act:
        hint = (" Try: " + ", ".join(sug)) if sug else ""
        await interaction.response.send_message(f"Unknown activity.{hint}", ephemeral=True)
        return
    try:
        await load_queues()
        await load_checked()
        await load_catty()
    except Exception:
        pass
    uid = interaction.user.id
    catty_needed = str(catty_weapon_run or "no").strip().lower() == "yes"
    catty = _ensure_catty(act)
    # Enforce cooldown for players who just completed this activity via /schedule
    try:
        now = int(datetime.utcnow().timestamp())
        cd_map = COOLDOWNS.get(act, {})
        until = int(cd_map.get(uid, 0) or 0)
        if until and now < until:
            remaining = until - now
            hrs = max(1, int((remaining + 3599) // 3600))
            await interaction.response.send_message(
                f"You can rejoin the {act} queue in ~{hrs} hour(s).", ephemeral=True
            )
            return
    except Exception:
        pass
    in_any = [a for a, lst in QUEUES.items() if uid in lst]
    if act in in_any:
        before = uid in catty
        if catty_needed:
            catty.add(uid)
        else:
            catty.discard(uid)
        if before != catty_needed:
            await persist_catty()
            await interaction.response.send_message(
                f"You're already in that queue. Updated catty/weapon run: {'Yes ⭐' if catty_needed else 'No'}",
                ephemeral=True,
            )
            await _post_activity_board(act)
            return
        await interaction.response.send_message("You're already in that queue.", ephemeral=True)
        return
    if len(in_any) >= 2:
        await interaction.response.send_message("You can be in at most 2 different activity queues.", ephemeral=True)
        return
    _ensure_queue(act).append(uid)
    _ensure_checked(act).discard(uid)
    if catty_needed:
        catty.add(uid)
    else:
        catty.discard(uid)
    await persist_queues()
    _append_queue_event("join", act, [uid])
    await persist_checked()
    await persist_catty()
    await interaction.response.send_message(
        f"Joined queue for: {act}" + (" (⭐ catty/weapon run requested)" if catty_needed else ""),
        ephemeral=True,
    )
    await _post_activity_board(act)
@bot.tree.command(name="leave", description="Leave an activity queue or an event by message ID")
@app_commands.describe(activity="(Optional) activity name to leave", message_id="(Optional) event message ID to leave")
@app_commands.autocomplete(activity=_activity_autocomplete)
async def leave_cmd(interaction: discord.Interaction, activity: Optional[str] = None, message_id: Optional[int] = None):
    # Refresh queues from disk to ensure we use the latest queue file state
    try:
        await load_queues()
        await load_checked()
        await load_catty()
    except Exception:
        pass
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
            _ensure_checked(act).discard(uid)
            _ensure_catty(act).discard(uid)
            await persist_queues(allow_removals=True); await persist_checked(); await persist_catty()
            _append_queue_event("leave", act, [uid])
            await interaction.response.send_message(f"Left queue: {act}", ephemeral=True)
            await _post_activity_board(act)
            return
        else:
            # Fallback: user is not in the queue; try to leave an active event for this activity
            try:
                candidates: List[Tuple[int, Dict[str, object]]] = []
                for mid, d in list(SCHEDULES.items()):
                    try:
                        if str(d.get("activity") or "") != str(act):
                            continue
                        players: List[int] = d.get("players", [])  # type: ignore
                        backups: List[int] = d.get("backups", [])  # type: ignore
                        if uid in players or uid in backups:
                            candidates.append((int(mid), d))
                    except Exception:
                        continue
                target: Optional[Tuple[int, Dict[str, object]]] = None
                if candidates:
                    # Prefer an event in the current channel; otherwise pick the most recent
                    try:
                        ch_id = int(interaction.channel.id) if interaction.channel else None  # type: ignore
                    except Exception:
                        ch_id = None
                    channel_matches: List[Tuple[int, Dict[str, object]]] = []
                    for mid, d in candidates:
                        try:
                            ev_ch = int(d.get("channel_id")) if d.get("channel_id") else None  # type: ignore
                        except Exception:
                            ev_ch = None
                        if ch_id and ev_ch == ch_id:
                            channel_matches.append((mid, d))
                    target = max(channel_matches or candidates, key=lambda x: x[0])
                if target:
                    t_mid, t_data = target
                    participants: List[int] = t_data.get("players", [])  # type: ignore
                    backups: List[int] = t_data.get("backups", [])  # type: ignore
                    did_change = False
                    if uid in participants:
                        participants[:] = [x for x in participants if x != uid]
                        moved = _autofill_from_backups(t_data)
                        did_change = True
                        guild = interaction.client.get_guild(int(t_data.get("guild_id"))) if t_data.get("guild_id") else None  # type: ignore
                        await _dm_promoted_users(guild, moved, t_data)
                    if uid in backups:
                        backups[:] = [x for x in backups if x != uid]
                        did_change = True
                    if did_change:
                        guild = interaction.client.get_guild(int(t_data.get("guild_id"))) if t_data.get("guild_id") else None  # type: ignore
                        if guild:
                            await _update_schedule_message(guild, int(t_mid))
                        await interaction.response.send_message(f"Left the event: {act}", ephemeral=True)
                        return
            except Exception:
                pass
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
        if status == "Player":
            try:
                await _mark_queue_participants_checked(str(data.get("activity") or ""), [uid])
            except Exception:
                pass
        if guild: await _update_schedule_message(guild, message_id)  # type: ignore
        await interaction.response.send_message(f"Added user as {status}.", ephemeral=True)
        return

    if activity:
        act, sug = _resolve_activity(activity)
        if not act:
            hint = (" Try: " + ", ".join(sug)) if sug else ""
            await interaction.response.send_message(f"Unknown activity.{hint}", ephemeral=True)
            return
        try:
            await load_queues()
            await load_checked()
            await load_catty()
        except Exception:
            pass
        q = _ensure_queue(act)
        if uid in q:
            await interaction.response.send_message("User already in queue.", ephemeral=True)
            return
        q.append(uid)
        # Adding someone to a queue is not the same as making them a participant.
        _ensure_checked(act).discard(uid)
        # /add does not specify catty/weapon run; default to not requested.
        _ensure_catty(act).discard(uid)
        await persist_queues(); await persist_checked(); await persist_catty()
        _append_queue_event("add", act, [uid])
        await interaction.response.send_message(f"Added user to queue: {act}", ephemeral=True)
        await _post_activity_board(act)
        return

    await interaction.response.send_message("Specify an activity or message_id to add the user to.", ephemeral=True)

@bot.tree.command(name="remove", description="Remove user(s) from a queue or event (founder only)")
@founder_only()
@app_commands.describe(activity="(Optional) activity to remove from", message_id="(Optional) event message ID", user="User mention(s) or ID(s) to remove (space/comma-separated)")
@app_commands.autocomplete(activity=_activity_autocomplete)
async def remove_cmd(interaction: discord.Interaction, user: str, activity: Optional[str] = None, message_id: Optional[int] = None):
    # Ensure we are operating on the latest queue state (important with multiple bot instances)
    try:
        await load_queues()
        await load_checked()
        await load_catty()
    except Exception:
        pass
    guild = interaction.guild
    uid_list = _parse_user_ids(user, guild) if guild else []
    if not uid_list:
        await interaction.response.send_message("Couldn't resolve any users.", ephemeral=True)
        return
    uid_set = set(uid_list)
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
        prev_players = len(participants)
        prev_backups = len(backups)
        # Remove from event lists
        participants[:] = [x for x in participants if x not in uid_set]
        backups[:] = [x for x in backups if x not in uid_set]
        removed_any = (len(participants) < prev_players) or (len(backups) < prev_backups)
        # Autofill if players were removed
        if len(participants) < prev_players:
            _autofill_from_backups(data)
        if removed_any and guild:
            await _update_schedule_message(guild, message_id)  # type: ignore
        await interaction.response.send_message(
            "Removed selected user(s) from event." if removed_any else "None of the specified users are in that event.",
            ephemeral=True,
        )
        return

    if activity:
        act, _ = _resolve_activity(activity, list(ALL_ACTIVITIES) + list(QUEUES.keys()))
        if not act:
            await interaction.response.send_message("Unknown activity.", ephemeral=True)
            return
        q = QUEUES.get(act, [])
        before = len(q)
        if before:
            q[:] = [x for x in q if x not in uid_set]
        after = len(q)
        removed_any = after < before
        # Also clear green checks and catty flags if present
        try:
            check = _ensure_checked(act)
            catty = _ensure_catty(act)
            for uid in uid_set:
                if uid in check:
                    check.discard(uid)
                if uid in catty:
                    catty.discard(uid)
        except Exception:
            pass
        if removed_any:
            await persist_queues(allow_removals=True); await persist_checked(); await persist_catty()
            _append_queue_event("remove", act, list(uid_set))
            await interaction.response.send_message("Removed selected user(s) from queue.", ephemeral=True)
            await _post_activity_board(act)
            return
        await interaction.response.send_message("None of the specified users are in that queue.", ephemeral=True)
        return

    # No specific activity or message provided — remove the user(s) from ALL activity queues
    changed_acts: List[str] = []
    try:
        for act in list(QUEUES.keys()):
            q = QUEUES.get(act, [])
            if not q:
                continue
            before_len = len(q)
            # Remove any matching user IDs from this activity's queue
            q[:] = [x for x in q if x not in uid_set]
            if len(q) < before_len:
                changed_acts.append(act)
                # Also clear green-check marks and catty flags for removed users in this activity
                try:
                    chk = _ensure_checked(act)
                    cat = _ensure_catty(act)
                    for uid in uid_set:
                        if uid in chk:
                            chk.discard(uid)
                        if uid in cat:
                            cat.discard(uid)
                except Exception:
                    pass
    except Exception:
        changed_acts = []

    if changed_acts:
        await persist_queues(allow_removals=True); await persist_checked(); await persist_catty()
        for act in changed_acts:
            _append_queue_event("remove", act, list(uid_set))
        await interaction.response.send_message(
            f"Removed selected user(s) from queues: {', '.join(changed_acts)}.", ephemeral=True
        )
        # Update activity boards for all modified activities
        for act in changed_acts:
            await _post_activity_board(act)
        return

    await interaction.response.send_message("None of the specified users are in any queues.", ephemeral=True)

@bot.tree.command(name="clearqueue", description="Clear one queue or all queues (founder only)")
@founder_only()
@app_commands.describe(activity="(Optional) activity to clear. Leave blank to clear all queues")
@app_commands.autocomplete(activity=_activity_autocomplete)
async def clearqueue_cmd(interaction: discord.Interaction, activity: Optional[str] = None):
    await interaction.response.defer(ephemeral=True)
    # Refresh queue state from disk first to avoid stale clears in multi-instance setups
    try:
        await load_queues()
        await load_checked()
        await load_catty()
    except Exception:
        pass

    if activity:
        act, sug = _resolve_activity(activity, list(ALL_ACTIVITIES) + list(QUEUES.keys()))
        if not act:
            hint = (" Try: " + ", ".join(sug)) if sug else ""
            await interaction.followup.send(f"Unknown activity.{hint}", ephemeral=True)
            return

        queue_was_nonempty = bool(QUEUES.get(act))
        checked_was_nonempty = bool(CHECKED.get(act))
        catty_was_nonempty = bool(CATTY_RUNS.get(act))
        _ensure_queue(act).clear()
        _ensure_checked(act).clear()
        _ensure_catty(act).clear()
        await persist_queues(allow_removals=True); await persist_checked(); await persist_catty()
        _append_queue_event("clear", act, [])
        await _post_activity_board(act)
        if queue_was_nonempty or checked_was_nonempty or catty_was_nonempty:
            await interaction.followup.send(f"Cleared queue: {act}.", ephemeral=True)
        else:
            await interaction.followup.send(f"Queue already empty: {act}.", ephemeral=True)
        return

    changed_acts: List[str] = []
    all_acts = set(ALL_ACTIVITIES) | set(QUEUES.keys()) | set(CHECKED.keys()) | set(CATTY_RUNS.keys())
    for act in all_acts:
        queue_was_nonempty = bool(QUEUES.get(act))
        checked_was_nonempty = bool(CHECKED.get(act))
        catty_was_nonempty = bool(CATTY_RUNS.get(act))
        _ensure_queue(act).clear()
        _ensure_checked(act).clear()
        _ensure_catty(act).clear()
        if queue_was_nonempty or checked_was_nonempty or catty_was_nonempty:
            changed_acts.append(act)

    await persist_queues(allow_removals=True); await persist_checked(); await persist_catty()
    _append_queue_event("clear", None, [])
    for act in changed_acts:
        await _post_activity_board(act)
    if changed_acts:
        await interaction.followup.send(
            f"Cleared all queues ({len(changed_acts)} updated): {', '.join(sorted(changed_acts))}.",
            ephemeral=True,
        )
    else:
        await interaction.followup.send("All queues are already empty.", ephemeral=True)

@bot.tree.command(name="cancel", description="Cancel an event: deletes its embed(s) and prevents restore")
@app_commands.describe(message_id="(Optional) event message ID to cancel")
async def cancel_cmd(interaction: discord.Interaction, message_id: Optional[int] = None):
    try:
        await interaction.response.defer(ephemeral=True)
    except Exception:
        pass

    # Locate target event
    target_mid: Optional[int] = None
    data: Optional[Dict[str, object]] = None
    if message_id is not None:
        try:
            target_mid = int(message_id)
        except Exception:
            target_mid = None
        if target_mid is not None:
            data = SCHEDULES.get(target_mid)

    # Auto-detect if not provided: prefer authorized events in current channel; else latest owned
    if data is None or target_mid is None:
        try:
            invoker_uid = int(interaction.user.id)
            channel_id = int(interaction.channel.id) if interaction.channel else None  # type: ignore

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
                    if pid == invoker_uid or (FOUNDER_USER_ID and invoker_uid == int(FOUNDER_USER_ID)) or (int(d.get("host_id") or 0) == invoker_uid):
                        authorized_in_channel.append((mid, d))

                if authorized_in_channel:
                    target_mid, data = max(authorized_in_channel, key=lambda x: x[0])

            if data is None:
                owned: List[Tuple[int, Dict[str, object]]] = []
                for mid, d in list(SCHEDULES.items()):
                    try:
                        pid = int(d.get("promoter_id")) if d.get("promoter_id") else None  # type: ignore
                    except Exception:
                        pid = None
                    if pid == invoker_uid or (int(d.get("host_id") or 0) == invoker_uid):
                        owned.append((int(mid), d))
                if owned:
                    target_mid, data = max(owned, key=lambda x: x[0])
        except Exception:
            data = None
            target_mid = None

    if data is None or target_mid is None:
        await interaction.followup.send("No event found to cancel.", ephemeral=True)
        return

    # Permission check: promoter, host (for sherpa-only), or founder
    if not _is_promoter_or_founder(interaction, data):
        await interaction.followup.send("Only the promoter or founder can cancel this event.", ephemeral=True)
        return

    # Mark as cancelled to prevent auto-restore
    try:
        data["cancelled"] = True
    except Exception:
        pass

    # Capture all message IDs that reference this data object
    related_mids: List[int] = []
    try:
        for mid, d in list(SCHEDULES.items()):
            if d is data:
                try:
                    related_mids.append(int(mid))
                except Exception:
                    pass
    except Exception:
        related_mids = [int(target_mid)]

    # Delete linked Sherpa signup alert if present
    alert_mid = None
    alert_ch = None
    try:
        alert_mid = int(data.get("sherpa_alert_message_id")) if data.get("sherpa_alert_message_id") else None  # type: ignore
        alert_ch = int(data.get("sherpa_alert_channel_id")) if data.get("sherpa_alert_channel_id") else None  # type: ignore
    except Exception:
        alert_mid = None
        alert_ch = None
    if alert_mid and alert_ch:
        try:
            ch = bot.get_channel(alert_ch) or await bot.fetch_channel(alert_ch)
            if ch:
                amsg = await ch.fetch_message(alert_mid)
                await amsg.delete()
        except Exception:
            pass

    # Delete main embed messages in the recorded channel
    ch_id = None
    try:
        ch_id = int(data.get("channel_id")) if data.get("channel_id") else None  # type: ignore
    except Exception:
        ch_id = None
    if ch_id:
        try:
            ch = bot.get_channel(ch_id) or await bot.fetch_channel(ch_id)
            if ch:
                for mid in sorted(set(related_mids)):
                    try:
                        m = await ch.fetch_message(int(mid))
                        await m.delete()
                    except Exception:
                        pass
        except Exception:
            pass

    # Remove from schedule store so scheduler/reminders stop
    for mid in related_mids:
        try:
            SCHEDULES.pop(int(mid), None)
        except Exception:
            pass

    await interaction.followup.send("Event canceled and embeds deleted.", ephemeral=True)

@bot.tree.command(name="delete_schedule", description="Delete /schedule embed(s) (alias of /cancel)")
@app_commands.describe(message_id="(Optional) event message ID to delete")
async def delete_schedule_cmd(interaction: discord.Interaction, message_id: Optional[int] = None):
    # Delegate to cancel_cmd to ensure embeds are deleted and auto-restore is bypassed
    await cancel_cmd(interaction, message_id)

@bot.tree.command(name="queue", description="Post the current queues (one embed per activity, or pick a specific activity)")
@app_commands.describe(activity="(Optional) Choose an activity to show its queue only")
@app_commands.autocomplete(activity=_activity_autocomplete)
async def queue_cmd(interaction: discord.Interaction, activity: Optional[str] = None):
    await interaction.response.defer(ephemeral=True)
    # Ensure the most recent on-disk state is used, especially with multiple instances
    try:
        await load_queues()
        await load_checked()
        await load_catty()
    except Exception:
        pass
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


@bot.tree.command(name="restorequeue", description="Founder only: restore queues from queue board history")
@founder_only()
@app_commands.describe(
    strategy="Which queue board to use for each activity",
    history_limit="How many recent #raid-queue messages to scan (100-2000)",
    activity_filter="Optional: only restore activities whose queue title contains this text, e.g. Pantheon",
    replace_existing="Replace all current queues instead of merging restored activities into them",
)
@app_commands.choices(
    strategy=[
        app_commands.Choice(name="All names found across boards", value="all"),
        app_commands.Choice(name="Fullest board per activity (recommended)", value="fullest"),
        app_commands.Choice(name="Oldest non-empty board per activity", value="oldest"),
        app_commands.Choice(name="Newest non-empty board per activity", value="newest"),
    ]
)
async def restorequeue_cmd(
    interaction: discord.Interaction,
    strategy: str = "fullest",
    history_limit: Optional[int] = 1000,
    activity_filter: Optional[str] = None,
    replace_existing: bool = False,
):
    await interaction.response.defer(ephemeral=True)
    try:
        chosen_strategy = str(strategy or "fullest").strip().lower()
        if chosen_strategy not in ("all", "fullest", "oldest", "newest"):
            chosen_strategy = "fullest"
        scan_limit = max(100, min(int(history_limit or 1000), 2000))
        recovered = await _recover_queues_from_queue_boards(
            include_older_nonempty=True,
            replace_existing=bool(replace_existing),
            include_all_names=(chosen_strategy == "all"),
            prefer_fullest=(chosen_strategy == "fullest"),
            prefer_oldest=(chosen_strategy == "oldest"),
            activity_filter=activity_filter,
            history_limit=scan_limit,
        )
        if not recovered:
            suffix = f" matching `{activity_filter}`" if activity_filter else ""
            await interaction.followup.send(f"No queue signups found in recent queue board history{suffix}.", ephemeral=True)
            return
        await persist_queues(allow_removals=bool(replace_existing))
        await persist_checked()
        await persist_catty()
        await _post_all_activity_boards(interaction.channel_id)
        total = _queue_total(QUEUES)
        await interaction.followup.send(
            f"Restored {total} queued signup(s) from queue board history "
            f"across {len([q for q in QUEUES.values() if q])} activity queue(s).\n"
            f"Strategy: {chosen_strategy}; scanned: {scan_limit} messages.\n"
            f"Filter: {activity_filter or 'none'}; replace existing: {bool(replace_existing)}.\n"
            f"Found: {_queue_restore_summary()}",
            ephemeral=True,
        )
    except Exception as e:
        await interaction.followup.send(f"Queue restore failed: {e.__class__.__name__}", ephemeral=True)


@bot.tree.command(name="restorequeue_preview", description="Founder only: preview queue boards seen by restore")
@founder_only()
@app_commands.describe(
    activity_filter="Optional: only show queue boards whose title contains this text, e.g. Pantheon",
    history_limit="How many recent #raid-queue messages to scan (100-2000)",
)
async def restorequeue_preview_cmd(
    interaction: discord.Interaction,
    activity_filter: Optional[str] = None,
    history_limit: Optional[int] = 2000,
):
    await interaction.response.defer(ephemeral=True)
    try:
        scan_limit = max(100, min(int(history_limit or 2000), 2000))
        messages_seen, boards_seen, lines = await _preview_queue_board_history(
            activity_filter=activity_filter,
            history_limit=scan_limit,
        )
        body = "\n".join(lines) if lines else "No matching queue-board embeds found."
        if len(body) > 1800:
            body = body[:1797] + "..."
        await interaction.followup.send(
            f"Scanned {messages_seen} message(s), found {boards_seen} matching queue board(s).\n"
            f"Filter: {activity_filter or 'none'}.\n{body}",
            ephemeral=True,
        )
    except Exception as e:
        await interaction.followup.send(f"Queue restore preview failed: {e.__class__.__name__}", ephemeral=True)


@bot.tree.command(name="check", description="Add a green check next to a user in a queue")
@app_commands.describe(activity="Activity name", user="User mention or ID to mark")
@app_commands.autocomplete(activity=_activity_autocomplete)
async def check_cmd(interaction: discord.Interaction, activity: str, user: str):
    guild = interaction.guild
    if not guild:
        await interaction.response.send_message("Use this in a server.", ephemeral=True)
        return
    # Refresh queues to avoid stale membership checks
    try:
        await load_queues()
        await load_checked()
    except Exception:
        pass
    act, sug = _resolve_activity(activity)
    if not act:
        hint = (" Try: " + ", ".join(sug)) if sug else ""
        await interaction.response.send_message(f"Unknown activity.{hint}", ephemeral=True)
        return
    ids = _parse_user_ids(user, guild)
    if not ids:
        await interaction.response.send_message("Couldn't resolve that user.", ephemeral=True)
        return
    uid = ids[0]
    q = QUEUES.get(act, [])
    if uid not in q:
        await interaction.response.send_message("User is not in that queue.", ephemeral=True)
        return
    _ensure_checked(act).add(uid)
    await persist_checked()
    await interaction.response.send_message("Marked with green check.", ephemeral=True)
    await _post_activity_board(act)


@bot.tree.command(name="uncheck", description="Remove the green check next to a user in a queue")
@app_commands.describe(activity="Activity name", user="User mention or ID to unmark")
@app_commands.autocomplete(activity=_activity_autocomplete)
async def uncheck_cmd(interaction: discord.Interaction, activity: str, user: str):
    guild = interaction.guild
    if not guild:
        await interaction.response.send_message("Use this in a server.", ephemeral=True)
        return
    # Refresh queues to avoid stale membership checks
    try:
        await load_queues()
        await load_checked()
    except Exception:
        pass
    act, sug = _resolve_activity(activity)
    if not act:
        hint = (" Try: " + ", ".join(sug)) if sug else ""
        await interaction.response.send_message(f"Unknown activity.{hint}", ephemeral=True)
        return
    ids = _parse_user_ids(user, guild)
    if not ids:
        await interaction.response.send_message("Couldn't resolve that user.", ephemeral=True)
        return
    uid = ids[0]
    check = _ensure_checked(act)
    if uid in check:
        check.discard(uid)
        await persist_checked()
    await interaction.response.send_message("Removed green check (if present).", ephemeral=True)
    await _post_activity_board(act)

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
    """Parse a free-form list of users into user IDs.

    Supports:
    - Mentions like <@123> or <@!123>
    - Raw numeric IDs
    - Quoted names with spaces ("First Last")
    - Comma/semicolon separated names
    - Unique exact/partial matches on display_name/global_name/username (case-insensitive)
    """
    if not text or not guild:
        return []

    resolved_ids: List[int] = []

    # 1) Direct mentions
    for m in re.findall(r"<@!?([0-9]{5,25})>", text):
        try:
            resolved_ids.append(int(m))
        except Exception:
            pass

    # 2) Bare numeric IDs
    for m in re.findall(r"(?<![<@!#])\b([0-9]{15,25})\b", text):
        try:
            uid = int(m)
            if uid not in resolved_ids:
                resolved_ids.append(uid)
        except Exception:
            pass

    # Helper normalization for names
    def _norm(s: Optional[str]) -> str:
        if not s:
            return ""
        base = " ".join(str(s).strip().lower().split())
        return base

    # Build lightweight member snapshots once
    members: List[Tuple[int, str, str, str]] = []  # (id, display_name, global_name, username)
    try:
        for mem in list(guild.members or []):
            dn = getattr(mem, "display_name", "") or ""
            gn = getattr(mem, "global_name", "") or ""
            un = getattr(mem, "name", "") or ""
            members.append((int(mem.id), _norm(dn), _norm(gn), _norm(un)))
    except Exception:
        members = []

    # 3) Extract quoted names to preserve spaces
    remaining = text
    for quoted in re.findall(r"\"([^\"\n]+)\"|'([^'\n]+)'", text):
        # regex returns tuples; pick the first non-empty capture
        name = next((p for p in quoted if p), None)
        if not name:
            continue
        key = _norm(name)
        matches_exact = [mid for (mid, dn, gn, un) in members if key in (dn, gn, un)]
        if len(matches_exact) == 1:
            if matches_exact[0] not in resolved_ids:
                resolved_ids.append(matches_exact[0])
        else:
            # Unique partial
            matches_partial = [mid for (mid, dn, gn, un) in members if key and (key in dn or key in gn or key in un)]
            if len(matches_partial) == 1 and matches_partial[0] not in resolved_ids:
                resolved_ids.append(matches_partial[0])
        # Remove this quoted chunk from remaining to avoid double-processing
        try:
            remaining = remaining.replace(f'"{name}"', ' ')
            remaining = remaining.replace(f"'{name}'", ' ')
        except Exception:
            pass

    # 4) Split the rest by commas/semicolons/newlines; keep internal spaces (already removed quotes)
    chunks = [c.strip() for c in re.split(r"[\n;,]+", remaining) if c.strip()]
    # Further break chunks that are only whitespace-separated lists of mentions/ids/names
    tokens: List[str] = []
    for c in chunks:
        # If it contains a mention, keep it as a whole token so it matches the regex above
        if re.search(r"<@!?[0-9]{5,25}>", c):
            tokens.append(c)
        else:
            tokens.extend([t for t in c.split() if t])

    # Try to resolve tokens that weren't captured already
    for tok in tokens:
        # Skip if already parsed via mention/ID
        if re.fullmatch(r"<@!?[0-9]{5,25}>", tok):
            continue
        if tok.isdigit():
            try:
                uid = int(tok)
                if uid not in resolved_ids:
                    resolved_ids.append(uid)
            except Exception:
                pass
            continue
        # Normalize token, strip leading @ and surrounding punctuation
        key = _norm(re.sub(r"^[^A-Za-z0-9@]+|[^A-Za-z0-9]+$", "", tok.lstrip("@")))
        if not key:
            continue
        # Exact match across known names
        matches_exact = [mid for (mid, dn, gn, un) in members if key in (dn, gn, un)]
        if len(matches_exact) == 1:
            if matches_exact[0] not in resolved_ids:
                resolved_ids.append(matches_exact[0])
            continue
        # Unique partial match
        matches_partial = [mid for (mid, dn, gn, un) in members if key and (key in dn or key in gn or key in un)]
        if len(matches_partial) == 1:
            if matches_partial[0] not in resolved_ids:
                resolved_ids.append(matches_partial[0])

    # Deduplicate preserving order
    seen: Set[int] = set()
    unique_ids: List[int] = []
    for uid in resolved_ids:
        if uid not in seen:
            unique_ids.append(uid)
            seen.add(uid)
    return unique_ids

# ---------------------------
# /list command — helpers & views (standalone; does not use SCHEDULES or ConfirmView)
# ---------------------------

LIST_JOIN_EMOJI = "📋"

def _list_run_sherpa_slots_needed(data: Dict[str, object], player_count: int) -> int:
    """Sherpa slots per group. Host always reserves 1 fireteam slot."""
    cap = int(data.get("capacity", 0) or 0)
    host_slots = 1 if data.get("host_in_fireteam", True) else 0
    players = int(player_count)
    room = max(0, cap - host_slots - players)
    explicit = data.get("num_sherpas")
    if explicit is not None:
        return max(0, min(int(explicit), room))
    return room

def _list_max_group_size(data: Dict[str, object]) -> int:
    """Max players pulled from the line per Next (host slot is separate)."""
    cap = int(data.get("capacity", 0) or 0)
    host_slots = 1 if data.get("host_in_fireteam", True) else 0
    explicit = data.get("num_sherpas")
    if explicit is not None:
        return max(1, cap - host_slots - max(0, int(explicit)))
    return max(1, cap - host_slots)

def _list_rebuild_waiting_with_priority(
    pulled: List[int],
    waiting: List[int],
    activity: str,
) -> List[int]:
    """Queue members in waiting sort ahead of react/open joiners."""
    q = [int(u) for u in (QUEUES.get(activity, []) or [])]
    queue_positions = {u: i for i, u in enumerate(q)}
    queue_waiting = [int(u) for u in waiting if int(u) in queue_positions]
    open_waiting = [int(u) for u in waiting if int(u) not in queue_positions]
    queue_waiting.sort(key=lambda u: queue_positions[int(u)])
    # Preserve join order among open (react) joiners
    open_order = {int(u): i for i, u in enumerate(waiting) if int(u) not in queue_positions}
    open_waiting.sort(key=lambda u: open_order.get(int(u), 0))
    return list(pulled) + queue_waiting + open_waiting

async def _list_try_add_to_line(
    guild: Optional[discord.Guild],
    session_id: str,
    uid: int,
) -> Tuple[bool, str]:
    data = _list_session_data(session_id)
    if not data or str(data.get("status")) == "done":
        return False, "This list is no longer active."
    line: List[int] = list(data.get("line") or [])  # type: ignore[arg-type]
    uid = int(uid)
    if uid in line:
        return False, "You're already in line."
    max_size = data.get("max_list_size")
    if max_size is not None:
        try:
            limit = int(max_size)
            if limit > 0 and len(line) >= limit:
                return False, f"The list is full ({limit} max)."
        except Exception:
            pass
    declined: Set[int] = set(int(x) for x in (data.get("declined") or set()))  # type: ignore[arg-type]
    if uid in declined:
        declined.discard(uid)
        data["declined"] = declined

    activity = str(data.get("activity") or "")
    try:
        await load_queues()
    except Exception:
        pass

    next_index = int(data.get("next_index", 0) or 0)
    pulled = line[:next_index]
    waiting = list(line[next_index:])
    waiting.append(uid)
    data["line"] = _list_rebuild_waiting_with_priority(pulled, waiting, activity)

    on_queue = uid in {int(u) for u in (QUEUES.get(activity, []) or [])}
    if guild:
        await _update_list_control_message(guild, session_id)
    if on_queue:
        return True, "You're in line with **queue priority**! We'll DM you when it's your turn."
    return True, "You're in line! Queue members are ahead of react joiners — we'll DM you when it's your turn."

async def _list_add_join_reaction(message: Optional[discord.Message]) -> None:
    if not message:
        return
    try:
        await message.add_reaction(LIST_JOIN_EMOJI)
    except Exception:
        pass

def _guild_sherpa_member_ids(guild: Optional[discord.Guild]) -> List[int]:
    if not guild:
        return []
    out: List[int] = []
    seen: Set[int] = set()
    for member in guild.members:
        try:
            if _is_sherpa(member) or _is_sherpa_assistant(member):
                if member.id not in seen:
                    out.append(int(member.id))
                    seen.add(int(member.id))
        except Exception:
            continue
    return out

def _pick_sherpas_for_batch(guild: Optional[discord.Guild], data: Dict[str, object], count: int) -> List[int]:
    if count <= 0:
        return []
    pool: List[int] = list(data.get("sherpa_pool") or [])  # type: ignore[arg-type]
    idx = int(data.get("sherpa_index", 0) or 0)
    picked: List[int] = []
    seen: Set[int] = set()
    # Prefer registered sherpa pool (round-robin)
    attempts = 0
    while len(picked) < count and pool and attempts < len(pool) * 2:
        uid = int(pool[idx % len(pool)])
        idx += 1
        attempts += 1
        if uid in seen:
            continue
        seen.add(uid)
        picked.append(uid)
    # Fill any remaining slots from guild Sherpa roles
    if len(picked) < count and guild:
        for uid in _guild_sherpa_member_ids(guild):
            if uid in seen:
                continue
            seen.add(uid)
            picked.append(uid)
            if len(picked) >= count:
                break
    data["sherpa_index"] = idx
    return picked

async def _render_list_embed(guild: Optional[discord.Guild], data: Dict[str, object]) -> Tuple[discord.Embed, Optional[discord.File]]:
    activity = str(data.get("activity") or "Activity")
    when = str(data.get("when_text") or "TBD")
    group_size = int(data.get("group_size", 1) or 1)
    cap = int(data.get("capacity", 0) or 0)
    batch_no = int(data.get("batch_number", 0) or 0)
    round_no = int(data.get("round_number", 1) or 1)
    status = str(data.get("status") or "active")
    line: List[int] = list(data.get("line") or [])  # type: ignore[arg-type]
    next_index = int(data.get("next_index", 0) or 0)
    waiting = line[next_index:]
    completed_batches: List[List[int]] = list(data.get("completed_batches") or [])  # type: ignore[arg-type]
    host_id = data.get("host_id") or data.get("promoter_id")

    max_list_size = data.get("max_list_size")
    num_sherpas = data.get("num_sherpas")
    host_in = bool(data.get("host_in_fireteam", True))

    title = f"📋 List — {activity}"
    if status == "done":
        desc = "This **/list** session is **finished**. Thanks everyone!"
    else:
        sherpa_note = (
            f"**{int(num_sherpas)}** Sherpa(s) per group"
            if num_sherpas is not None
            else "remaining slots filled with **Sherpas**"
        )
        desc = (
            f"**/list** session for **{activity}** — running it back-to-back.\n"
            f"React {LIST_JOIN_EMOJI} on this post (or tap **Yes** in DMs) to join the line.\n"
            f"**Queue members get priority** over {LIST_JOIN_EMOJI} joiners.\n"
            f"Each group: **host** + **{group_size}** player(s) + {sherpa_note} (fireteam **{cap}**)."
        )
    embed = discord.Embed(title=title, description=desc, color=_activity_color(activity))
    embed.add_field(name="When", value=when, inline=False)
    if data.get("difficulty"):
        embed.add_field(name="Difficulty", value=str(data.get("difficulty")), inline=True)
    embed.add_field(name="Group Size", value=str(group_size), inline=True)
    embed.add_field(name="Fireteam Size", value=str(cap), inline=True)
    if host_in:
        embed.add_field(name="Host Slot", value="Yes (1 per group)", inline=True)
    if num_sherpas is not None:
        embed.add_field(name="Sherpas / Group", value=str(int(num_sherpas)), inline=True)
    else:
        embed.add_field(name="Sherpas / Group", value="Auto-fill", inline=True)
    if max_list_size is not None:
        try:
            lim = int(max_list_size)
            embed.add_field(name="List Cap", value=str(lim) if lim > 0 else "Unlimited", inline=True)
        except Exception:
            embed.add_field(name="List Cap", value="Unlimited", inline=True)
    else:
        embed.add_field(name="List Cap", value="Unlimited", inline=True)
    if host_id:
        embed.add_field(name="Hosted by", value=f"<@{int(host_id)}>", inline=True)
    embed.add_field(name="Batches Run", value=str(batch_no), inline=True)
    embed.add_field(name="Round", value=str(round_no), inline=True)
    embed.add_field(name="In Line", value=str(len(line)), inline=True)
    embed.add_field(name="Waiting", value=str(len(waiting)), inline=True)

    if waiting:
        q_order = {int(u): i for i, u in enumerate(QUEUES.get(activity, []) or [])}
        lines = []
        for i, uid in enumerate(waiting[:20]):
            pos = next_index + i + 1
            tag = " 🎫" if int(uid) in q_order else ""
            lines.append(f"{pos}. <@{uid}>{tag}")
        extra = f"\n…and {len(waiting) - 20} more" if len(waiting) > 20 else ""
        note = "\n🎫 = on activity queue (priority)"
        embed.add_field(name="Up Next", value="\n".join(lines) + extra + note, inline=False)
    elif status != "done":
        embed.add_field(name="Up Next", value="_No one left in line._", inline=False)

    if completed_batches:
        last = completed_batches[-1]
        embed.add_field(
            name="Last Group",
            value=", ".join(f"<@{uid}>" for uid in last) or "—",
            inline=False,
        )

    if status == "active":
        embed.set_footer(text=f"React {LIST_JOIN_EMOJI} to join • Next pulls the next group • Done ends this list")

    try:
        img_url = data.get("image_url")
        if img_url and not str(img_url).startswith("attachment://"):
            embed.set_image(url=str(img_url))
            return embed, None
    except Exception:
        pass
    return _apply_activity_image(embed, activity)

def _list_session_data(session_id: str) -> Optional[Dict[str, object]]:
    return LIST_SESSIONS.get(str(session_id))

def _list_control_message_id(data: Dict[str, object]) -> Optional[int]:
    try:
        mid = data.get("control_message_id")
        return int(mid) if mid else None
    except Exception:
        return None

def _register_list_views(session_id: str, line_user_ids: Optional[List[int]] = None) -> None:
    sid = str(session_id)
    try:
        bot.add_view(ListControlView(session_id=sid))
    except Exception:
        pass
    if line_user_ids:
        for uid in line_user_ids:
            try:
                bot.add_view(ListConfirmView(session_id=sid, uid=int(uid)))
            except Exception:
                pass

async def _update_list_control_message(guild: Optional[discord.Guild], session_id: str) -> None:
    data = _list_session_data(session_id)
    if not data or not guild:
        return
    mid = _list_control_message_id(data)
    channel_id = int(data.get("channel_id") or 0)
    if not mid or not channel_id:
        return
    channel = guild.get_channel(channel_id) or await guild.fetch_channel(channel_id)
    if not channel:
        return
    try:
        msg = await channel.fetch_message(int(mid))
    except Exception:
        return
    embed, _ = await _render_list_embed(guild, data)
    view = None if str(data.get("status")) == "done" else ListControlView(session_id=str(session_id))
    try:
        await msg.edit(embed=embed, view=view)
    except Exception:
        pass

async def _repost_list_to_bottom(guild: Optional[discord.Guild], session_id: str) -> Optional[int]:
    data = _list_session_data(session_id)
    if not data or not guild or str(data.get("status")) == "done":
        return None
    channel_id = int(data.get("channel_id") or 0)
    old_mid = _list_control_message_id(data)
    if not channel_id:
        return None
    channel = guild.get_channel(channel_id) or await guild.fetch_channel(channel_id)
    if not channel:
        return None
    embed, f = await _render_list_embed(guild, data)
    view = ListControlView(session_id=str(session_id))
    if old_mid:
        try:
            old_msg = await channel.fetch_message(int(old_mid))
            try:
                await old_msg.delete()
            except Exception:
                pass
        except Exception:
            pass
        LIST_RUNS_MSG_TO_SESSION.pop(int(old_mid), None)
    try:
        if f:
            new_msg = await channel.send(embed=embed, file=f, view=view)
        else:
            new_msg = await channel.send(embed=embed, view=view)
    except Exception:
        return None
    await _list_add_join_reaction(new_msg)
    new_mid = int(new_msg.id)
    data["control_message_id"] = new_mid
    LIST_RUNS_MSG_TO_SESSION[new_mid] = str(session_id)
    _register_list_views(str(session_id))
    try:
        if new_msg.embeds and new_msg.embeds[0].image and new_msg.embeds[0].image.url:
            url = str(new_msg.embeds[0].image.url)
            if not url.startswith("attachment://"):
                data["image_url"] = url
                embed_cdn, _ = await _render_list_embed(guild, data)
                await new_msg.edit(embed=embed_cdn, view=view, attachments=[])
    except Exception:
        pass
    return new_mid

async def _maybe_repost_list_control(channel_id: int) -> None:
    session_id = LIST_RUNS_BY_CHANNEL.get(int(channel_id))
    if not session_id:
        return
    data = _list_session_data(session_id)
    if not data or str(data.get("status")) == "done":
        return
    now = float(datetime.utcnow().timestamp())
    last = float(LIST_REPOST_DEBOUNCE.get(int(channel_id), 0) or 0)
    if (now - last) < 3.0:
        return
    LIST_REPOST_DEBOUNCE[int(channel_id)] = now
    guild_id = data.get("guild_id")
    guild = bot.get_guild(int(guild_id)) if guild_id else None  # type: ignore[arg-type]
    if guild:
        await _repost_list_to_bottom(guild, str(session_id))

class ListConfirmView(discord.ui.View):
    def __init__(self, session_id: str, uid: int):
        super().__init__(timeout=None)
        self.session_id = str(session_id)
        self.uid = int(uid)
        yes_btn = discord.ui.Button(
            label="Yes",
            style=discord.ButtonStyle.success,
            custom_id=f"list_confirm_yes:{self.session_id}:{self.uid}",
        )
        yes_btn.callback = self._yes_callback  # type: ignore[method-assign]
        self.add_item(yes_btn)
        no_btn = discord.ui.Button(
            label="Can't make it",
            style=discord.ButtonStyle.secondary,
            custom_id=f"list_confirm_no:{self.session_id}:{self.uid}",
        )
        no_btn.callback = self._no_callback  # type: ignore[method-assign]
        self.add_item(no_btn)

    async def _yes_callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.uid:
            await interaction.response.send_message("This DM button isn't for you.", ephemeral=True)
            return
        data = _list_session_data(self.session_id)
        if not data or str(data.get("status")) == "done":
            await interaction.response.send_message("This list run is no longer active.", ephemeral=True)
            return
        guild = interaction.client.get_guild(int(data.get("guild_id"))) if data.get("guild_id") else None  # type: ignore[arg-type]
        ok, msg = await _list_try_add_to_line(guild, self.session_id, self.uid)
        await interaction.response.send_message(msg, ephemeral=True)

    async def _no_callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.uid:
            await interaction.response.send_message("This DM button isn't for you.", ephemeral=True)
            return
        data = _list_session_data(self.session_id)
        if not data:
            await interaction.response.send_message("This list run is no longer active.", ephemeral=True)
            return
        line: List[int] = list(data.get("line") or [])  # type: ignore[arg-type]
        declined: Set[int] = set(int(x) for x in (data.get("declined") or set()))  # type: ignore[arg-type]
        if self.uid in line:
            line = [x for x in line if int(x) != self.uid]
            data["line"] = line
            nxt = int(data.get("next_index", 0) or 0)
            if nxt > len(line):
                data["next_index"] = len(line)
        declined.add(self.uid)
        data["declined"] = declined
        await interaction.response.send_message("No worries — skipped for this list run.", ephemeral=True)
        guild = interaction.client.get_guild(int(data.get("guild_id"))) if data.get("guild_id") else None  # type: ignore[arg-type]
        if guild:
            await _update_list_control_message(guild, self.session_id)

class ListControlView(discord.ui.View):
    def __init__(self, session_id: str):
        super().__init__(timeout=None)
        self.session_id = str(session_id)
        next_btn = discord.ui.Button(
            label="Next",
            style=discord.ButtonStyle.primary,
            custom_id=f"list_next:{self.session_id}",
        )
        next_btn.callback = self._next_callback  # type: ignore[method-assign]
        self.add_item(next_btn)
        done_btn = discord.ui.Button(
            label="Done",
            style=discord.ButtonStyle.danger,
            custom_id=f"list_done:{self.session_id}",
        )
        done_btn.callback = self._done_callback  # type: ignore[method-assign]
        self.add_item(done_btn)

    async def _next_callback(self, interaction: discord.Interaction) -> None:
        data = _list_session_data(self.session_id)
        if not data or str(data.get("status")) == "done":
            await interaction.response.send_message("This list run is no longer active.", ephemeral=True)
            return
        if not _is_list_host_or_founder(interaction, data):
            await interaction.response.send_message("Only the list host or a founder can advance the list.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        line: List[int] = list(data.get("line") or [])  # type: ignore[arg-type]
        next_index = int(data.get("next_index", 0) or 0)
        group_size = max(1, min(int(data.get("group_size", 1) or 1), _list_max_group_size(data)))
        host_id = int(data.get("host_id") or 0)
        wrapped_round = False
        waiting: List[int] = []
        if line:
            waiting = line[next_index:]
            if not waiting:
                data["next_index"] = 0
                data["round_number"] = int(data.get("round_number", 1) or 1) + 1
                next_index = 0
                waiting = line[:]
                wrapped_round = True
            batch_players = waiting[:group_size]
            new_index = next_index + len(batch_players)
            if new_index >= len(line):
                data["next_index"] = len(line)
            else:
                data["next_index"] = new_index
        else:
            batch_players = []

        sherpa_count = _list_run_sherpa_slots_needed(data, len(batch_players))
        sherpas = _pick_sherpas_for_batch(guild, data, sherpa_count)
        activity = str(data.get("activity") or "Activity")
        batch_no = int(data.get("batch_number", 0) or 0) + 1
        data["batch_number"] = batch_no
        completed_batches: List[List[int]] = list(data.get("completed_batches") or [])  # type: ignore[arg-type]
        completed_batches.append(list(batch_players))
        data["completed_batches"] = completed_batches

        host_text = f"<@{host_id}>" if host_id else "_None_"
        sherpa_text = ", ".join(f"<@{uid}>" for uid in sherpas) if sherpas else "_None_"
        player_text = ", ".join(f"<@{uid}>" for uid in batch_players) if batch_players else "_None_"
        channel_id = int(data.get("channel_id") or interaction.channel_id)
        turn_msg = (
            f"**Group {batch_no} — your turn for {activity}!**\n"
            f"Host: {host_text}\n"
            f"Players: {player_text}\n"
            f"Sherpas: {sherpa_text}"
        )
        await _send_to_channel_id(
            channel_id,
            content=turn_msg,
            allowed_mentions=discord.AllowedMentions(users=True),
        )

        when_text = str(data.get("when_text") or "")
        for uid in batch_players:
            try:
                member = guild.get_member(int(uid)) if guild else None
                if not member:
                    continue
                dm = await member.create_dm()
                await dm.send(
                    content=(
                        f"It's your turn for **{activity}**"
                        + (f" at **{when_text}**" if when_text else "")
                        + f" (Group {batch_no}).\nHead to the event channel!"
                    )
                )
            except Exception:
                pass

        await _update_list_control_message(guild, self.session_id)
        round_no = int(data.get("round_number", 1) or 1)
        lap_note = f" (round {round_no})" if wrapped_round else ""
        await interaction.followup.send(
            f"Pulled group {batch_no}{lap_note}: host"
            + (f" + {len(batch_players)} player(s)" if batch_players else "")
            + (f" + {len(sherpas)} Sherpa(s)" if sherpas else "")
            + f" = {1 + len(batch_players) + len(sherpas)}/{int(data.get('capacity', 0))} fireteam."
            + " Hit **Next** again whenever you're ready.",
            ephemeral=True,
        )

    async def _done_callback(self, interaction: discord.Interaction) -> None:
        data = _list_session_data(self.session_id)
        if not data:
            await interaction.response.send_message("This list run is no longer active.", ephemeral=True)
            return
        if not _is_list_host_or_founder(interaction, data):
            await interaction.response.send_message("Only the list host or a founder can end the list.", ephemeral=True)
            return
        data["status"] = "done"
        channel_id = int(data.get("channel_id") or 0)
        if channel_id:
            LIST_RUNS_BY_CHANNEL.pop(channel_id, None)
        guild = interaction.guild
        embed, _ = await _render_list_embed(guild, data)
        try:
            await interaction.response.edit_message(embed=embed, view=None)
        except Exception:
            await interaction.response.send_message("List run ended.", ephemeral=True)

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
        backups: List[int] = data.get("backups", [])  # type: ignore
        cap = int(data.get("capacity", 0)); reserved = int(data.get("reserved_sherpas", 0))
        player_slots = max(0, cap - reserved)
        # Late-join cutoff: within 2 hours of start, queued users cannot bump a full roster
        start_ts = int(data.get("start_ts") or 0)
        now_ts = int(datetime.utcnow().timestamp())
        late_full_window = bool(start_ts) and now_ts >= start_ts - 2 * 60 * 60
        # Queue prioritization: users who were in the queue when scheduled are prioritized
        candidates: List[int] = data.get("candidates", []) or []  # type: ignore
        promoter_id: Optional[int] = data.get("promoter_id")  # type: ignore
        is_prioritized = self.uid in candidates
        # Players that were pre-slotted at schedule time cannot be bumped
        locked_players: Set[int] = set(data.get("locked_players") or [])  # type: ignore
        # Try to add to players if there is space; otherwise backups
        if len(participants) < player_slots:
            added, reason = _append_unique_to(data, "players", self.uid)
            if added:
                await interaction.response.send_message("Locked in. See you there! ✅", ephemeral=True)
                _log_confirmation(self.mid, self.uid, "confirm", "added_players")
                # If this confirmer came from the queue, set a 24h cooldown from event end (start + 3h)
                if is_prioritized:
                    try:
                        act = str(data.get("activity"))
                        if act:
                            start_ts = int(data.get("start_ts") or 0)
                            # Assume event duration ~3h; cooldown starts after event end
                            event_end = start_ts + 3 * 60 * 60 if start_ts else int(datetime.utcnow().timestamp())
                            until = event_end + 24 * 60 * 60
                            m = COOLDOWNS.setdefault(act, {})
                            m[self.uid] = max(int(m.get(self.uid, 0) or 0), int(until))
                            await persist_cooldowns()
                    except Exception:
                        pass
            else:
                await interaction.response.send_message("You're already accounted for.", ephemeral=True)
                _log_confirmation(self.mid, self.uid, "confirm", "skipped", reason)
        else:
            # If roster is full but the confirmer is prioritized (queued), try to bump a non-queued participant
            if is_prioritized:
                if late_full_window:
                    await interaction.response.send_message(
                        "I'm so sorry—you were too late for this one. "
                        "If the roster is full, we lock it 2 hours before start so no more bumps happen. "
                        "You're still in the queue though, and you can try again for the next run.",
                        ephemeral=True,
                    )
                    _log_confirmation(self.mid, self.uid, "confirm", "late_full")
                    return
                # Find a participant who is NOT in the queued candidate list and is not the promoter
                bumpable_indices: List[int] = [
                    idx for idx, uid in enumerate(list(participants))
                    if uid not in candidates and (promoter_id is None or uid != int(promoter_id)) and uid != self.uid and uid not in locked_players
                ]
                if bumpable_indices:
                    # Prefer bumping the last bumpable to minimize disruption of earlier ordering
                    bump_idx = bumpable_indices[-1]
                    bumped_uid = participants.pop(bump_idx)
                    # Place bumped user into backups if not already there
                    if bumped_uid not in backups:
                        backups.append(bumped_uid)
                    # Add the prioritized confirmer into players
                    if self.uid not in participants:
                        participants.append(self.uid)
                    await interaction.response.send_message("Locked in. Your queue priority secured a slot. ✅", ephemeral=True)
                    _log_confirmation(self.mid, self.uid, "confirm", "bumped_nonqueued")
                    # Set 24h cooldown (from event end) since this user is now a player
                    try:
                        act = str(data.get("activity"))
                        if act:
                            start_ts = int(data.get("start_ts") or 0)
                            event_end = start_ts + 3 * 60 * 60 if start_ts else int(datetime.utcnow().timestamp())
                            until = event_end + 24 * 60 * 60
                            m = COOLDOWNS.setdefault(act, {})
                            m[self.uid] = max(int(m.get(self.uid, 0) or 0), int(until))
                            await persist_cooldowns()
                    except Exception:
                        pass
                else:
                    # No one to bump; fall back to backups
                    added, reason = _append_unique_to(data, "backups", self.uid)
                    if added:
                        await interaction.response.send_message("Roster is full — added as **Backup**.", ephemeral=True)
                        _log_confirmation(self.mid, self.uid, "confirm", "added_backups")
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
        # Mark queued users with ✅ only once they are actual players, not backups.
        try:
            act = str(data.get("activity") or "")
            current_players: List[int] = data.get("players", []) or []  # type: ignore
            if act and self.uid in current_players:
                await _mark_queue_participants_checked(act, [self.uid])
        except Exception:
            pass

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
    if not moved:
        return
    try:
        if str(data.get("type")) != "sherpa_only":
            await _mark_queue_participants_checked(str(data.get("activity") or ""), moved)
    except Exception:
        pass
    if not guild:
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
    # Apply 24h cooldown (from event end) for promoted players that were original queue candidates
    try:
        if str(data.get("type")) != "sherpa_only":
            act = str(data.get("activity") or "")
            cand: List[int] = data.get("candidates", []) or []  # type: ignore
            if act:
                start_ts = int(data.get("start_ts") or 0)
                now = int(datetime.utcnow().timestamp())
                event_end = start_ts + 3 * 60 * 60 if start_ts else now
                until = event_end + 24 * 60 * 60
                m = COOLDOWNS.setdefault(act, {})
                changed = False
                for uid in moved:
                    if uid in cand:
                        prev = int(m.get(uid, 0) or 0)
                        new_until = max(prev, until)
                        if new_until != prev:
                            m[uid] = new_until
                            changed = True
                if changed:
                    await persist_cooldowns()
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
        # If we have not yet persisted a CDN image URL, or the stored URL is an
        # attachment placeholder, try to capture a CDN URL from the existing
        # message (either the embed's image URL if it's already a CDN, or from
        # an image attachment on the message).
        if (not data.get("image_url")) or str(data.get("image_url")).startswith("attachment://"):
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
                            ctype = (getattr(att, "content_type", None) or "").lower()
                            filename = str(getattr(att, "filename", "") or "")
                            ext = os.path.splitext(filename)[1].lower()
                            is_image = (
                                (ctype.startswith("image"))
                                or bool(getattr(att, "height", None))
                                or ext in (".png", ".jpg", ".jpeg", ".gif", ".webp")
                            )
                        except Exception:
                            is_image = False
                        if is_image:
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
            if data.get("image_url") and not str(data.get("image_url")).startswith("attachment://"):
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
    # Queue mutations persist at the command/action point. Avoid rewriting queue
    # state from a background task because stale bot memory can wipe active queues.
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            await persist_checked(); await persist_catty(); await persist_cooldowns()
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
        # If this event was explicitly cancelled, do not auto-restore
        try:
            if data.get("cancelled"):
                return
        except Exception:
            pass
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
                url = str(new_msg.embeds[0].image.url)
                if not url.startswith("attachment://"):
                    data["image_url"] = url
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
    difficulty="Difficulty (raids/dungeons only)",
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
    ],
    difficulty=[
        app_commands.Choice(name="Normal", value="Normal"),
        app_commands.Choice(name="Master", value="Master"),
    ]
)
async def schedule_cmd(
    interaction: discord.Interaction,
    activity: str,
    datetime_str: str,
    timezone: str = "America/New_York",
    difficulty: Optional[str] = None,
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

        is_raid_dungeon = _is_raid_or_dungeon(act)
        selected_difficulty = difficulty if is_raid_dungeon else None

        # Channel: main event embed must go into EVENT_SIGNUP_CHANNEL_ID (fallback: current channel)
        channel_id = (EVENT_SIGNUP_CHANNEL_ID or interaction.channel_id)

        cap = _cap_for_activity(act)
        reserved = max(0, min(int(reserved_sherpas or 0), cap))

        try:
            await load_queues()
            await load_checked()
            await load_catty()
        except Exception:
            pass
        q = QUEUES.get(act, [])
        checked_queue = CHECKED.get(act, set()) or set()
        skipped_checked = sum(1 for uid in q if int(uid) in checked_queue)
        candidates = _queue_members_needing_prompt(act)

        # Parse datetime_str (MM-DD HH:MM) with current year
        try:
            date_part, time_part = datetime_str.strip().split()
            year = datetime.now().year
            date_full = f"{year}-{date_part}"
        except Exception:
            await interaction.followup.send("Invalid datetime format. Use MM-DD HH:MM.", ephemeral=True); return

        start_ts = _parse_date_time_to_epoch(date_full, time_part, tz_name=timezone)
        # If time parsing fails, cancel scheduling rather than posting with TBD
        if not start_ts:
            await interaction.followup.send(
                "Could not parse the date/time. Scheduling canceled. Use MM-DD HH:MM (24h).",
                ephemeral=True,
            )
            return
        when_text = f"<t:{start_ts}:F> ({timezone})"

        guild = interaction.guild
        sherpa_ids = set(_parse_user_ids(sherpas or "", guild)) if sherpas else set()
        participant_ids = _parse_user_ids(participants or "", guild) if participants else []

        # Pre-slotted sherpas count toward reserved slots.
        # We announce only if there are open Sherpa slots remaining.
        open_sherpa_slots = max(0, int(reserved) - len(set(int(x) for x in sherpa_ids)))

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
        # Reorder to prioritize queued users for participant slots while keeping promoter first
        try:
            queue_set = set(candidates)
        except Exception:
            queue_set = set()
        if promoter_id in uniq_participants:
            rest = [u for u in uniq_participants if u != promoter_id]
            prioritized = [promoter_id]
        else:
            rest = list(uniq_participants)
            prioritized = []
        prioritized.extend([u for u in rest if u in queue_set])
        prioritized.extend([u for u in rest if u not in queue_set])
        uniq_participants = prioritized
        players_final = uniq_participants[:player_slots]
        backups_final = uniq_participants[player_slots:]

        data = {
            "guild_id": guild.id if guild else None,
            "activity": act,
            "desc": f"Scheduled by {interaction.user.mention}. Check your DMs to confirm.",
            "when_text": when_text,
            "capacity": cap,
            "reserved_sherpas": reserved,
            "difficulty": selected_difficulty,
            "sherpas": sherpa_ids,
            "sherpa_backup": set(),
            "candidates": candidates,
            "players": players_final,
            # Players pre-slotted at creation time are protected from queue bumping
            "locked_players": set(players_final),
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
                url = str(ev_msg.embeds[0].image.url)
                # Only persist a proper CDN URL, never an attachment placeholder
                if not url.startswith("attachment://"):
                    data["image_url"] = url
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
        # Auto-mark queue users who were placed as participants by /schedule
        # only after the event exists.
        try:
            await _mark_queue_participants_checked(act, players_final)
        except Exception:
            pass
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
                if open_sherpa_slots > 0:
                    sherpa_desc = (
                        f"{open_sherpa_slots} open Sherpa slot(s) (of {reserved} reserved).\n"
                        f"React ✅ on **this** post to claim your Sherpa slot.\n"
                        f"Or react 🔁 to be **Sherpa Backup**."
                    )
                else:
                    sherpa_desc = (
                        "All reserved Sherpa slots are already filled.\n"
                        "React 🔁 to be **Sherpa Backup** (or ✅ to be added as backup)."
                    )
                sherpa_embed = discord.Embed(
                    title=f"🧭 Sherpa Signup — {act}",
                    description=sherpa_desc,
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
                if open_sherpa_slots > 0:
                    sherpa_desc = (
                        f"{open_sherpa_slots} open Sherpa slot(s) (of {reserved} reserved).\n"
                        f"React ✅ on **this** post to claim your Sherpa slot.\n"
                        f"Or react 🔁 to be **Sherpa Backup**."
                    )
                else:
                    sherpa_desc = (
                        "All reserved Sherpa slots are already filled.\n"
                        "React 🔁 to be **Sherpa Backup** (or ✅ to be added as backup)."
                    )
                sherpa_embed = discord.Embed(
                    title=f"🧭 Sherpa Signup — {act}",
                    description=sherpa_desc,
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
        announce_skipped_full = (open_sherpa_slots <= 0)
        if (not announce_skipped_full) and GENERAL_SHERPA_CHANNEL_ID:
            try:
                ping_text = f"<@&{SHERPA_ASSISTANT_ROLE_ID}>" if SHERPA_ASSISTANT_ROLE_ID else None
                gen_embed = discord.Embed(
                    title=f"Sherpa Signup — {act}",
                    description=(
                        f"{when_text}\n"
                        f"Open Sherpa slots: **{open_sherpa_slots}** (of {reserved} reserved)\n"
                        f"Please use the **Sherpa signup post** to claim your slot (✅). Extras become **Sherpa Backup**."
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
        if (not announce_skipped_full) and (not posted_general_announce) and GENERAL_CHANNEL_ID:
            try:
                ping_text = f"<@&{SHERPA_ASSISTANT_ROLE_ID}>" if SHERPA_ASSISTANT_ROLE_ID else None
                gen_embed = discord.Embed(
                    title=f"Sherpa Signup — {act}",
                    description=(
                        f"{when_text}\n"
                        f"Open Sherpa slots: **{open_sherpa_slots}** (of {reserved} reserved)\n"
                        f"Please use the **Sherpa signup post** to claim your slot (✅). Extras become **Sherpa Backup**."
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

        # ---- DMs to unchecked queued users who are not already players (ConfirmView) ----
        sent = 0
        pre_slotted_players = {int(uid) for uid in (data.get("players", []) or [])}
        sent_candidate_ids: Set[int] = set()
        for uid in candidates:
            if int(uid) in pre_slotted_players:
                continue
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
                sent_candidate_ids.add(int(uid))
            except Exception as e:
                print("DM failed:", e)

        # DM any pre-slotted players we didn't DM above (info-only)
        pre_dmed = set(sent_candidate_ids)
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
            f"Scheduled **{act}**" + (f" ({selected_difficulty})" if selected_difficulty else "") + ".",
            f"DMed {sent} queued player(s), notified {p_sent} pre-slotted participant(s).",
            f"Sherpa signup posted: {'Yes' if posted_sherpa_signup else 'No'}" + (f" (fallback in <#{sherpa_signup_fallback}>)" if sherpa_signup_fallback else ""),
            (
                "General-sherpa announcement: Skipped (Sherpa slots already filled)."
                if announce_skipped_full
                else f"General-sherpa announcement: {'Yes' if posted_general_announce else 'No'}"
                + (f" (fallback in <#{general_announce_fallback}>)" if general_announce_fallback else "")
            ),
        ]
        if skipped_checked:
            status_lines.append(f"Skipped {skipped_checked} already-checked queue participant(s).")
        if difficulty and not is_raid_dungeon:
            status_lines.append("Difficulty ignored: this option is only used for raids and dungeons.")
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
# /list — standalone repeat-run command (not /schedule)
# ---------------------------

@bot.tree.command(
    name="list",
    description="(Founder) Run an activity on repeat: DM the queue, line people up, pull groups with Next/Done",
)
@founder_only()
@app_commands.describe(
    activity="Activity name",
    datetime_str="Date and time (MM-DD HH:MM, 24h)",
    timezone="Timezone (dropdown)",
    group_size="Players pulled from the line per group (host uses 1 slot)",
    num_sherpas="Sherpas per group (leave empty to auto-fill remaining fireteam slots)",
    max_list_size="Max people allowed in the line (leave empty for unlimited)",
    difficulty="Difficulty (raids/dungeons only)",
    sherpas="User(s) to pre-slot as Sherpa helpers (optional)",
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
    ],
    difficulty=[
        app_commands.Choice(name="Normal", value="Normal"),
        app_commands.Choice(name="Master", value="Master"),
    ],
)
async def list_cmd(
    interaction: discord.Interaction,
    activity: str,
    datetime_str: str,
    timezone: str = "America/New_York",
    group_size: int = 3,
    num_sherpas: Optional[int] = None,
    max_list_size: Optional[int] = None,
    difficulty: Optional[str] = None,
    sherpas: Optional[str] = None,
):
    try:
        await interaction.response.defer(ephemeral=True)
    except Exception:
        pass

    try:
        act, sug = _resolve_activity(activity)
        if not act:
            hint = (" Try: " + ", ".join(sug)) if sug else ""
            await interaction.followup.send(f"Unknown activity.{hint}", ephemeral=True)
            return

        is_raid_dungeon = _is_raid_or_dungeon(act)
        selected_difficulty = difficulty if is_raid_dungeon else None
        channel_id = int(EVENT_SIGNUP_CHANNEL_ID or interaction.channel_id)
        cap = _cap_for_activity(act)
        host_slots = 1
        sherpa_slots_fixed: Optional[int] = None
        if num_sherpas is not None:
            sherpa_slots_fixed = max(0, min(int(num_sherpas), max(0, cap - host_slots - 1)))
        max_group = max(1, cap - host_slots - (sherpa_slots_fixed if sherpa_slots_fixed is not None else 0))
        if sherpa_slots_fixed is None:
            max_group = max(1, cap - host_slots)
        batch_players = max(1, min(int(group_size or 1), max_group))
        list_cap: Optional[int] = None
        if max_list_size is not None:
            try:
                parsed_cap = int(max_list_size)
                if parsed_cap > 0:
                    list_cap = parsed_cap
            except Exception:
                list_cap = None

        try:
            await load_queues()
        except Exception:
            pass

        queue_members = [int(uid) for uid in (QUEUES.get(act, []) or [])]

        try:
            date_part, time_part = datetime_str.strip().split()
            year = datetime.now().year
            date_full = f"{year}-{date_part}"
        except Exception:
            await interaction.followup.send("Invalid datetime format. Use MM-DD HH:MM.", ephemeral=True)
            return

        start_ts = _parse_date_time_to_epoch(date_full, time_part, tz_name=timezone)
        if not start_ts:
            await interaction.followup.send(
                "Could not parse the date/time. Use MM-DD HH:MM (24h).",
                ephemeral=True,
            )
            return
        when_text = f"<t:{start_ts}:F> ({timezone})"

        guild = interaction.guild
        sherpa_ids = _parse_user_ids(sherpas or "", guild) if sherpas else []
        sherpa_pool: List[int] = []
        seen_sherpas: Set[int] = set()
        for sid in sherpa_ids:
            if sid not in seen_sherpas:
                sherpa_pool.append(int(sid))
                seen_sherpas.add(int(sid))

        existing_session = LIST_RUNS_BY_CHANNEL.get(channel_id)
        if existing_session:
            old = _list_session_data(existing_session)
            if old and str(old.get("status")) != "done":
                old["status"] = "done"
                old_mid = _list_control_message_id(old)
                if guild and old_mid:
                    try:
                        ch = guild.get_channel(channel_id) or await guild.fetch_channel(channel_id)
                        if ch:
                            msg = await ch.fetch_message(int(old_mid))
                            emb, _ = await _render_list_embed(guild, old)
                            await msg.edit(embed=emb, view=None)
                    except Exception:
                        pass

        session_id = uuid.uuid4().hex[:12]
        host_id = interaction.user.id
        data: Dict[str, object] = {
            "type": "list_run",
            "session_id": session_id,
            "guild_id": guild.id if guild else None,
            "activity": act,
            "when_text": when_text,
            "start_ts": start_ts,
            "timezone": timezone,
            "difficulty": selected_difficulty,
            "capacity": cap,
            "group_size": batch_players,
            "num_sherpas": sherpa_slots_fixed,
            "max_list_size": list_cap,
            "host_in_fireteam": True,
            "channel_id": channel_id,
            "host_id": host_id,
            "line": [],
            "declined": set(),
            "next_index": 0,
            "completed_batches": [],
            "sherpa_pool": sherpa_pool,
            "sherpa_index": 0,
            "status": "active",
            "batch_number": 0,
            "round_number": 1,
            "sherpa_alert_message_id": None,
            "sherpa_alert_channel_id": None,
        }
        LIST_SESSIONS[session_id] = data
        LIST_RUNS_BY_CHANNEL[channel_id] = session_id

        embed, f = await _render_list_embed(guild, data)
        view = ListControlView(session_id=session_id)
        if f:
            ctrl_msg = await _send_to_channel_id(channel_id, embed=embed, file=f, view=view)
        else:
            ctrl_msg = await _send_to_channel_id(channel_id, embed=embed, view=view)
        if not ctrl_msg:
            LIST_SESSIONS.pop(session_id, None)
            LIST_RUNS_BY_CHANNEL.pop(channel_id, None)
            await interaction.followup.send(
                "Failed to post list control embed — set EVENT_SIGNUP_CHANNEL_ID or run in a channel.",
                ephemeral=True,
            )
            return

        mid = int(ctrl_msg.id)
        data["control_message_id"] = mid
        LIST_RUNS_MSG_TO_SESSION[mid] = session_id
        _register_list_views(session_id)
        await _list_add_join_reaction(ctrl_msg)

        try:
            if ctrl_msg.embeds and ctrl_msg.embeds[0].image and ctrl_msg.embeds[0].image.url:
                url = str(ctrl_msg.embeds[0].image.url)
                if not url.startswith("attachment://"):
                    data["image_url"] = url
                    embed_cdn, _ = await _render_list_embed(guild, data)
                    await ctrl_msg.edit(embed=embed_cdn, view=view, attachments=[])
        except Exception:
            pass

        sherpa_slots = _list_run_sherpa_slots_needed(data, batch_players)
        if RAID_SIGN_UP_CHANNEL_ID and sherpa_slots > 0 and sherpa_slots_fixed is None:
            try:
                sherpa_embed = discord.Embed(
                    title=f"🧭 Sherpa Signup — /list: {act}",
                    description=(
                        f"We're running **{act}** back-to-back at {when_text}.\n"
                        f"React ✅ to join the Sherpa rotation (fills remaining fireteam slots)."
                    ),
                    color=_activity_color(act),
                )
                sherpa_embed.add_field(name="When", value=when_text, inline=True)
                sherpa_embed.add_field(name="List Control", value=f"[Jump to list]({ctrl_msg.jump_url})", inline=False)
                alert = await _send_to_channel_id(int(RAID_SIGN_UP_CHANNEL_ID), embed=sherpa_embed)
                if alert:
                    data["sherpa_alert_message_id"] = str(alert.id)
                    data["sherpa_alert_channel_id"] = str(alert.channel.id)
                    try:
                        await alert.add_reaction("✅")
                    except Exception:
                        pass
            except Exception as e:
                try:
                    print("List sherpa signup post failed:", e)
                except Exception:
                    pass

        sent = 0
        for uid in queue_members:
            try:
                member = guild.get_member(uid) if guild else None
                if not member:
                    continue
                dm = await member.create_dm()
                await dm.send(
                    content=(
                        f"We're running **{act}** over and over today at **{when_text}** "
                        f"in {guild.name if guild else 'the server'}.\n"
                        "Tap **Yes** to get in line — we'll pull groups when it's your turn."
                    ),
                    view=ListConfirmView(session_id=session_id, uid=int(uid)),
                )
                _register_list_views(session_id, [int(uid)])
                sent += 1
            except Exception as e:
                print("List DM failed:", e)

        status_lines = [
            f"**/list** started for **{act}**" + (f" ({selected_difficulty})" if selected_difficulty else "") + ".",
            f"DMed **{sent}** queued player(s). Anyone can also react {LIST_JOIN_EMOJI} on the list post to join.",
            f"List embed posted in <#{channel_id}> (stays at the bottom until **Done**).",
            (
                f"Per group: **1 host** + **{batch_players}** player(s)"
                + (
                    f" + **{sherpa_slots_fixed}** Sherpa(s)"
                    if sherpa_slots_fixed is not None
                    else f" + **{sherpa_slots}** Sherpa(s) auto-fill"
                )
                + f" = **{cap}** fireteam."
            ),
            f"List cap: **{'unlimited' if list_cap is None else list_cap}**.",
        ]
        if difficulty and not is_raid_dungeon:
            status_lines.append("Difficulty ignored: only used for raids and dungeons.")
        await interaction.followup.send("\n".join(status_lines), ephemeral=True)

    except Exception as e:
        print("/list command error:", e)
        try:
            await interaction.followup.send("An error occurred while starting the list run. Check the bot logs.", ephemeral=True)
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

    # Try to persist a CDN-hosted image URL immediately so subsequent edits don't drop the image
    try:
        if ev_msg.embeds and ev_msg.embeds[0].image and ev_msg.embeds[0].image.url:
            url = str(ev_msg.embeds[0].image.url)
            if not url.startswith("attachment://"):
                data["image_url"] = url
                # Re-render without attachment to avoid duplicate preview card
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

    # /list — anyone reacts ✅ on the control embed to join the line
    session_id = LIST_RUNS_MSG_TO_SESSION.get(int(payload.message_id))
    if session_id and emoji_str == LIST_JOIN_EMOJI:
        data = _list_session_data(str(session_id))
        if data and str(data.get("type")) == "list_run" and str(data.get("status")) != "done":
            guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
            if not guild:
                return
            member = guild.get_member(payload.user_id)
            if not member:
                return
            ok, msg = await _list_try_add_to_line(guild, str(session_id), int(member.id))
            if not ok:
                try:
                    dm = await member.create_dm()
                    await dm.send(content=f"Could not join the **/list** line: {msg}")
                except Exception:
                    pass
            else:
                try:
                    dm = await member.create_dm()
                    await dm.send(content=msg)
                except Exception:
                    pass
            return

    # /list Sherpa signup (✅ on sherpa alert — separate from /schedule sherpa posts)
    for session_id, data in list(LIST_SESSIONS.items()):
        if str(data.get("type")) != "list_run" or str(data.get("status")) == "done":
            continue
        alert_id = int(data.get("sherpa_alert_message_id")) if data.get("sherpa_alert_message_id") else None
        alert_ch = int(data.get("sherpa_alert_channel_id")) if data.get("sherpa_alert_channel_id") else None
        if not alert_id or payload.message_id != alert_id:
            continue
        if alert_ch is not None and payload.channel_id != alert_ch:
            continue
        if emoji_str != "✅":
            continue
        guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
        if not guild:
            return
        member = guild.get_member(payload.user_id)
        if not member or not (_is_sherpa(member) or _is_sherpa_assistant(member)):
            return
        pool: List[int] = list(data.get("sherpa_pool") or [])  # type: ignore[arg-type]
        if member.id not in pool:
            pool.append(int(member.id))
            data["sherpa_pool"] = pool
        try:
            dm = await member.create_dm()
            activity = str(data.get("activity") or "Activity")
            when_text = str(data.get("when_text") or "")
            await dm.send(
                content=(
                    f"You're signed up as a Sherpa for the **{activity}** list run"
                    + (f" at **{when_text}**" if when_text else "")
                    + ". You'll be rotated in to fill fireteam slots."
                )
            )
        except Exception:
            pass
        return

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
                # Normalize to sets (defensive: avoids list/set mismatches and keeps counts correct)
                reserved = int(data.get("reserved_sherpas", 0) or 0)
                try:
                    sherpas: Set[int] = set(int(x) for x in (data.get("sherpas") or []))  # type: ignore[arg-type]
                except Exception:
                    sherpas = set()
                try:
                    backup: Set[int] = set(int(x) for x in (data.get("sherpa_backup") or []))  # type: ignore[arg-type]
                except Exception:
                    backup = set()
                if emoji_str == "✅":
                    # Dedup across lists
                    exists = _user_in_any_event_list(data, member.id)
                    if exists in (None, "sherpas"):
                        if len(sherpas) < reserved and member.id not in sherpas:
                            sherpas.add(member.id)
                        else:
                            backup.add(member.id)
                        data["sherpas"] = sherpas
                        data["sherpa_backup"] = backup
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
                        data["sherpa_backup"] = backup
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
        if not (_is_sherpa(member) or _is_sherpa_assistant(member)):
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
            # Sherpas are exempt from player queue cooldowns — do not set cooldowns here
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
                if member and (_is_sherpa(member) or _is_sherpa_assistant(member)):
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
            # Auto-mark check if this user came from the activity's queue.
            try:
                act = str(data.get("activity") or "")
                if act:
                    await _mark_queue_participants_checked(act, [payload.user_id])
                # Set a 24h cooldown only if they were in the queue when scheduled
                if act and payload.user_id in (data.get("candidates", []) or []):
                    start_ts = int(data.get("start_ts") or 0)
                    event_end = start_ts + 3 * 60 * 60 if start_ts else int(datetime.utcnow().timestamp())
                    until = event_end + 24 * 60 * 60
                    m = COOLDOWNS.setdefault(act, {})
                    m[payload.user_id] = max(int(m.get(payload.user_id, 0) or 0), int(until))
                    await persist_cooldowns()
            except Exception:
                pass
        else:
            backups.append(payload.user_id)
        await _update_schedule_message(guild, int(payload.message_id))
        return

    # ❌ on main event message → prompt manual removal instead of auto-removing
    if str(payload.emoji) == "❌":
        data = SCHEDULES.get(payload.message_id)
        if not data:
            return
        # Sherpa-only events are handled by the block above; this branch is for standard events.
        if str(data.get("type")) == "sherpa_only":
            return
        guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
        if guild:
            member = guild.get_member(payload.user_id)
            if member:
                try:
                    dm = await member.create_dm()
                    await dm.send(
                        "Queue spots are managed by the event staff. "
                        "Please contact the promoter or use the /leave command if you need to step out."
                    )
                except Exception:
                    pass
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
        # Manual oversight: do not auto-remove members when ✅ is removed on standard events.
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
            url = str(msg.embeds[0].image.url)
            if not url.startswith("attachment://"):
                data["image_url"] = url
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
# Build of the Week Commands
# ---------------------------

def _subclass_color(subclass: str) -> int:
    """Return embed color based on Destiny 2 subclass."""
    colors = {
        "Arc": 0x7FECFF,
        "Solar": 0xFF6F00,
        "Void": 0x8B00FF,
        "Stasis": 0x4FC3F7,
        "Strand": 0x00E676,
        "Prismatic": 0xFFD700,
    }
    return colors.get(subclass, 0x5865F2)

# Dynamic autocomplete for aspects based on class and subclass
async def _aspect_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    # Try to get class and subclass from the interaction namespace
    guardian_class = getattr(interaction.namespace, "guardian_class", None)
    subclass = getattr(interaction.namespace, "subclass", None)
    
    aspects: List[str] = []
    if guardian_class and subclass:
        class_aspects = DESTINY_DATA.get("aspects", {}).get(guardian_class, {})
        aspects = class_aspects.get(subclass, [])
    
    if not aspects:
        # Fallback: show all aspects for selected class, or all aspects
        if guardian_class:
            class_aspects = DESTINY_DATA.get("aspects", {}).get(guardian_class, {})
            for sub_aspects in class_aspects.values():
                aspects.extend(sub_aspects)
        else:
            for class_data in DESTINY_DATA.get("aspects", {}).values():
                for sub_aspects in class_data.values():
                    aspects.extend(sub_aspects)
        aspects = list(set(aspects))  # Remove duplicates
    
    cur = (current or "").lower()
    out: List[app_commands.Choice[str]] = []
    for aspect in sorted(aspects):
        if not cur or cur in aspect.lower():
            out.append(app_commands.Choice(name=aspect, value=aspect))
            if len(out) >= 25:
                break
    return out

# Dynamic autocomplete for fragments based on subclass
async def _fragment_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    subclass = getattr(interaction.namespace, "subclass", None)
    
    fragments: List[str] = []
    if subclass:
        fragments = DESTINY_DATA.get("fragments", {}).get(subclass, [])
    
    if not fragments:
        # Fallback: show all fragments
        for frag_list in DESTINY_DATA.get("fragments", {}).values():
            fragments.extend(frag_list)
        fragments = list(set(fragments))
    
    cur = (current or "").lower()
    out: List[app_commands.Choice[str]] = []
    for fragment in sorted(fragments):
        if not cur or cur in fragment.lower():
            out.append(app_commands.Choice(name=fragment, value=fragment))
            if len(out) >= 25:
                break
    return out

# Autocomplete for activities
async def _build_activity_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    activities = DESTINY_DATA.get("activities", [])
    cur = (current or "").lower()
    out: List[app_commands.Choice[str]] = []
    for act in activities:
        if not cur or cur in act.lower():
            out.append(app_commands.Choice(name=act, value=act))
            if len(out) >= 25:
                break
    return out


@bot.tree.command(name="build", description="Submit a Destiny 2 build for Build of the Week")
@app_commands.describe(
    activity="What activity is this build for?",
    guardian_class="Your Guardian's class",
    subclass="Your subclass element",
    exotic_armor="Your exotic armor piece",
    kinetic_weapon="Your kinetic weapon",
    energy_weapon="Your energy weapon",
    heavy_weapon="Your heavy/power weapon",
    aspect_one="First equipped aspect",
    aspect_two="Second equipped aspect",
    fragments="Your equipped fragments (comma-separated)",
    mods="Your armor mods (comma-separated, up to 25)",
    description="Describe your build and how it works",
    image="(Optional) Screenshot of your build",
    artifact_perks="(Optional) Seasonal artifact perks",
    dim_link="(Optional) DIM loadout link"
)
@app_commands.choices(
    guardian_class=[
        app_commands.Choice(name="Hunter", value="Hunter"),
        app_commands.Choice(name="Titan", value="Titan"),
        app_commands.Choice(name="Warlock", value="Warlock"),
    ],
    subclass=[
        app_commands.Choice(name="Arc", value="Arc"),
        app_commands.Choice(name="Solar", value="Solar"),
        app_commands.Choice(name="Void", value="Void"),
        app_commands.Choice(name="Stasis", value="Stasis"),
        app_commands.Choice(name="Strand", value="Strand"),
        app_commands.Choice(name="Prismatic", value="Prismatic"),
    ]
)
@app_commands.autocomplete(
    activity=_build_activity_autocomplete,
    aspect_one=_aspect_autocomplete,
    aspect_two=_aspect_autocomplete,
    fragments=_fragment_autocomplete
)
async def build_cmd(
    interaction: discord.Interaction,
    activity: str,
    guardian_class: str,
    subclass: str,
    exotic_armor: str,
    kinetic_weapon: str,
    energy_weapon: str,
    heavy_weapon: str,
    aspect_one: str,
    aspect_two: str,
    fragments: str,
    mods: str,
    description: str,
    image: Optional[discord.Attachment] = None,
    artifact_perks: Optional[str] = None,
    dim_link: Optional[str] = None
):
    await interaction.response.defer(ephemeral=True)
    
    # Validate build of the week channel is configured
    if not BUILD_OF_THE_WEEK_CHANNEL_ID:
        await interaction.followup.send(
            "Build of the Week channel is not configured. Please set BUILD_OF_THE_WEEK_CHANNEL_ID.",
            ephemeral=True
        )
        return
    
    # Get the target channel (forum)
    channel = bot.get_channel(BUILD_OF_THE_WEEK_CHANNEL_ID)
    if not channel:
        try:
            channel = await bot.fetch_channel(BUILD_OF_THE_WEEK_CHANNEL_ID)
        except Exception:
            pass
    
    if not channel:
        await interaction.followup.send(
            "Could not find the Build of the Week channel. Please check configuration.",
            ephemeral=True
        )
        return
    
    # Get current week
    week_start = _get_current_week_start()
    
    # Submitter info
    user = interaction.user
    username = f"{user.name}#{user.discriminator}" if user.discriminator and user.discriminator != "0" else user.name

    # Validate aspect selections
    class_aspects = DESTINY_DATA.get("aspects", {}).get(guardian_class, {})
    available_aspects = class_aspects.get(subclass, [])
    if not available_aspects:
        await interaction.followup.send(
            f"Unable to find any aspects for {guardian_class} {subclass}. Please double-check your selection.",
            ephemeral=True,
        )
        return

    normalized_aspect_one = _normalize_choice(aspect_one, available_aspects)
    normalized_aspect_two = _normalize_choice(aspect_two, available_aspects)
    if not normalized_aspect_one or not normalized_aspect_two:
        await interaction.followup.send(
            "One or both aspects are invalid for this subclass. Please pick from the autocomplete list.",
            ephemeral=True,
        )
        return

    if normalized_aspect_one == normalized_aspect_two:
        await interaction.followup.send(
            "Please select two different aspects.",
            ephemeral=True,
        )
        return

    slot_one = _get_aspect_slot_count(normalized_aspect_one, subclass)
    slot_two = _get_aspect_slot_count(normalized_aspect_two, subclass)
    if slot_one is None or slot_two is None:
        await interaction.followup.send(
            "I couldn't determine the fragment slots for one of those aspects. Please let an admin know so the data can be updated.",
            ephemeral=True,
        )
        return

    subclass_slot_cap = 6 if subclass == "Prismatic" else 4
    fragment_slot_budget = min(slot_one + slot_two, subclass_slot_cap)

    # Validate fragments
    fragment_pool = DESTINY_DATA.get("fragments", {}).get(subclass, [])
    if not fragment_pool:
        await interaction.followup.send(
            f"Unable to find any fragments for the {subclass} subclass.",
            ephemeral=True,
        )
        return

    fragment_inputs = _split_csv_list(fragments)
    if not fragment_inputs:
        await interaction.followup.send(
            "Please provide at least one fragment (comma-separated).",
            ephemeral=True,
        )
        return

    normalized_fragments: List[str] = []
    seen_fragments: Set[str] = set()
    for frag_name in fragment_inputs:
        normalized = _normalize_choice(frag_name, fragment_pool)
        if not normalized:
            await interaction.followup.send(
                f"`{frag_name}` is not a valid fragment for {subclass}. Please use the suggestions shown while typing.",
                ephemeral=True,
            )
            return
        if normalized in seen_fragments:
            await interaction.followup.send(
                f"You listed `{normalized}` more than once. Fragments must be unique.",
                ephemeral=True,
            )
            return
        seen_fragments.add(normalized)
        normalized_fragments.append(normalized)

    if len(normalized_fragments) > fragment_slot_budget:
        await interaction.followup.send(
            f"You selected {len(normalized_fragments)} fragments but your aspects only provide {fragment_slot_budget} slot(s).",
            ephemeral=True,
        )
        return

    aspects_text = ", ".join([normalized_aspect_one, normalized_aspect_two])
    fragments_text = ", ".join(normalized_fragments)
    
    # Build the embed
    embed = discord.Embed(
        title="🔨 BUILD DETAILS",
        color=_subclass_color(subclass)
    )
    
    embed.add_field(name="👤 Submitted by", value=f"<@{user.id}>", inline=False)
    
    # Activity and Class info
    embed.add_field(name="🎯 Activity", value=activity, inline=True)
    embed.add_field(name="🧙 Class", value=guardian_class, inline=True)
    embed.add_field(name="⚡ Subclass", value=subclass, inline=True)
    
    # Exotic armor
    embed.add_field(name="🛡️ Exotic Armor", value=exotic_armor, inline=False)
    
    # Weapons section
    embed.add_field(name="🔫 Kinetic", value=kinetic_weapon, inline=True)
    embed.add_field(name="⚡ Energy", value=energy_weapon, inline=True)
    embed.add_field(name="💥 Heavy", value=heavy_weapon, inline=True)
    
    # Abilities section
    embed.add_field(name="🔷 Aspects", value=aspects_text, inline=False)
    embed.add_field(name="🔹 Fragments", value=fragments_text, inline=False)
    
    # Mods
    mods_entries = _split_csv_list(mods)
    if mods_entries and len(mods_entries) > 25:
        await interaction.followup.send(
            "Please limit your mods list to 25 entries.",
            ephemeral=True,
        )
        return
    mods_text = ", ".join(mods_entries) if mods_entries else mods
    embed.add_field(name="🧩 Mods", value=mods_text, inline=False)
    
    # Optional fields
    if artifact_perks:
        embed.add_field(name="🏺 Artifact Perks", value=artifact_perks, inline=False)
    
    if dim_link:
        embed.add_field(name="🔗 DIM Link", value=dim_link, inline=False)
    
    # Description
    embed.add_field(name="📝 Description", value=description, inline=False)
    
    # Footer with week info
    embed.set_footer(text=f"Vote with 👍 if you like this build! • Week of {week_start}")
    
    # Add user avatar as thumbnail
    try:
        if user.avatar:
            embed.set_thumbnail(url=user.avatar.url)
    except Exception:
        pass
    
    # Prepare the image file if provided
    file_to_send = None
    if image:
        try:
            # Validate it's an image
            if image.content_type and image.content_type.startswith("image/"):
                file_to_send = await image.to_file()
                # Set the image in embed
                embed.set_image(url=f"attachment://{image.filename}")
            else:
                await interaction.followup.send(
                    "The attached file is not a valid image. Please attach a PNG, JPG, or GIF.",
                    ephemeral=True
                )
                return
        except Exception as e:
            await interaction.followup.send(f"Failed to process image: {e}", ephemeral=True)
            return
    
    # Create forum thread title
    thread_title = f"{guardian_class} {subclass} - {activity} | {user.display_name}"
    # Discord limits thread names to 100 characters
    if len(thread_title) > 100:
        thread_title = thread_title[:97] + "..."
    build_title = thread_title
    
    # Check if channel is a forum
    if isinstance(channel, discord.ForumChannel):
        # Create a forum post (thread)
        try:
            if file_to_send:
                thread_with_message = await channel.create_thread(
                    name=thread_title,
                    embed=embed,
                    file=file_to_send
                )
            else:
                thread_with_message = await channel.create_thread(
                    name=thread_title,
                    embed=embed
                )
            thread = thread_with_message.thread
            msg = thread_with_message.message
        except Exception as e:
            await interaction.followup.send(f"Failed to create forum post: {e}", ephemeral=True)
            return
        
        # Add thumbs up reaction for voting
        try:
            await msg.add_reaction("👍")
        except Exception:
            pass
        
        # Store the build data
        build_data = {
            "id": str(thread.id),
            "message_id": msg.id,
            "thread_id": thread.id,
            "channel_id": channel.id,
            "user_id": user.id,
            "username": username,
            "build_title": build_title,
            "submitted_at": int(datetime.now().timestamp()),
            "week_of": week_start,
            "activity": activity,
            "guardian_class": guardian_class,
            "subclass": subclass,
            "exotic_armor": exotic_armor,
            "kinetic_weapon": kinetic_weapon,
            "energy_weapon": energy_weapon,
            "heavy_weapon": heavy_weapon,
            "aspects": aspects_text,
            "fragments": fragments_text,
            "mods": mods_text,
            "artifact_perks": artifact_perks,
            "dim_link": dim_link,
            "description": description,
            "has_image": image is not None,
        }
        
        await add_build(build_data)
        
        # Confirm to user
        await interaction.followup.send(
            f"✅ Your build has been submitted! Check it out here: {thread.jump_url}\n"
            f"📊 Community members can now vote with 👍 on your build.",
            ephemeral=True
        )
    
    else:
        # Fallback for regular text channel
        try:
            if file_to_send:
                msg = await channel.send(embed=embed, file=file_to_send)
            else:
                msg = await channel.send(embed=embed)
        except Exception as e:
            await interaction.followup.send(f"Failed to post build: {e}", ephemeral=True)
            return
        
        # Add thumbs up reaction for voting
        try:
            await msg.add_reaction("👍")
        except Exception:
            pass
        
        # Store the build data
        build_data = {
            "id": str(msg.id),
            "message_id": msg.id,
            "channel_id": channel.id,
            "user_id": user.id,
            "username": username,
            "build_title": build_title,
            "submitted_at": int(datetime.now().timestamp()),
            "week_of": week_start,
            "activity": activity,
            "guardian_class": guardian_class,
            "subclass": subclass,
            "exotic_armor": exotic_armor,
            "kinetic_weapon": kinetic_weapon,
            "energy_weapon": energy_weapon,
            "heavy_weapon": heavy_weapon,
            "aspects": aspects_text,
            "fragments": fragments_text,
            "mods": mods_text,
            "artifact_perks": artifact_perks,
            "dim_link": dim_link,
            "description": description,
            "has_image": image is not None,
        }
        
        await add_build(build_data)
        
        # Confirm to user
        await interaction.followup.send(
            f"✅ Your build has been submitted! Check it out in <#{channel.id}>.\n"
            f"📊 Community members can now vote with 👍 on your build.",
            ephemeral=True
        )


@bot.tree.command(name="buildwinner", description="(Admin) Announce the Build of the Week winner")
@founder_only()
@app_commands.describe(
    announce="Post winner announcement in channel (default: True)",
    week="(Optional) Any date in the target week (YYYY-MM-DD). Default: current week"
)
async def buildwinner_cmd(
    interaction: discord.Interaction,
    announce: Optional[bool] = True,
    week: Optional[str] = None
):
    await interaction.response.defer(ephemeral=True)
    
    # Validate channel is configured
    if not BUILD_OF_THE_WEEK_CHANNEL_ID:
        await interaction.followup.send(
            "Build of the Week channel is not configured. Please set BUILD_OF_THE_WEEK_CHANNEL_ID.",
            ephemeral=True
        )
        return
    
    # Get channel
    channel = bot.get_channel(BUILD_OF_THE_WEEK_CHANNEL_ID)
    if not channel:
        try:
            channel = await bot.fetch_channel(BUILD_OF_THE_WEEK_CHANNEL_ID)
        except Exception:
            pass
    
    if not channel:
        await interaction.followup.send(
            "Could not find the Build of the Week channel.",
            ephemeral=True
        )
        return
    
    # Determine which week to evaluate (defaults to current week)
    parsed_week_start = _parse_week_start_from_input(week)
    if week and not parsed_week_start:
        await interaction.followup.send(
            "Invalid week/date. Try `YYYY-MM-DD` (example: 2025-12-08) or `M-D-YYYY` / `M/D/YYYY` (example: 12-8-2025).",
            ephemeral=True
        )
        return
    week_start = parsed_week_start or _get_current_week_start()

    # Get builds for selected week (persistent storage), and also scan the forum if applicable.
    # This prevents "no builds found" when posts exist but were not persisted (or were posted manually).
    builds: List[Dict[str, object]] = []
    try:
        builds = await get_builds_for_week(week_start)
    except Exception:
        builds = []

    try:
        if isinstance(channel, discord.ForumChannel):
            scanned = await _scan_builds_in_forum_for_week(channel, week_start)
            if scanned:
                # Merge + dedupe (prefer persisted entries when available)
                by_key: Dict[Tuple[Optional[int], Optional[int]], Dict[str, object]] = {}
                for b in scanned:
                    try:
                        by_key[(b.get("thread_id"), b.get("message_id"))] = b
                    except Exception:
                        continue
                for b in (builds or []):
                    try:
                        by_key[(b.get("thread_id"), b.get("message_id"))] = b
                    except Exception:
                        continue
                builds = list(by_key.values())
    except Exception:
        pass
    
    if not builds:
        await interaction.followup.send(
            f"No builds have been submitted for the week of {week_start}.",
            ephemeral=True
        )
        return
    
    # Count votes for each build
    build_votes: List[Tuple[Dict[str, object], int]] = []
    is_forum = isinstance(channel, discord.ForumChannel)
    
    for build in builds:
        message_id = build.get("message_id")
        thread_id = build.get("thread_id")
        if not message_id:
            continue
        
        try:
            msg = None
            
            if is_forum and thread_id:
                # For forum channels, fetch the thread first, then the message
                try:
                    thread = channel.get_thread(int(thread_id))
                    if not thread:
                        thread = await bot.fetch_channel(int(thread_id))
                    if thread:
                        msg = await thread.fetch_message(int(message_id))
                except Exception:
                    msg = None
                    # Fallback: try to resolve the starter message robustly
                    try:
                        if thread and isinstance(thread, discord.Thread):
                            msg = await _fetch_thread_starter_message(thread)
                    except Exception:
                        msg = None
            else:
                # Regular text channel
                msg = await channel.fetch_message(int(message_id))
            
            if not msg:
                continue
                
            vote_count = 0
            
            # Find the thumbs up reaction
            for reaction in msg.reactions:
                if str(reaction.emoji) == "👍":
                    # Subtract 1 if the bot reacted.
                    try:
                        vote_count = int(reaction.count) - (1 if getattr(reaction, "me", False) else 0)
                    except Exception:
                        vote_count = max(0, int(getattr(reaction, "count", 0) or 0))
                    break
            
            build_votes.append((build, max(0, vote_count)))
        except discord.NotFound:
            # Message was deleted
            continue
        except Exception as e:
            print(f"Error fetching build message {message_id}: {e}")
            continue
    
    if not build_votes:
        await interaction.followup.send(
            "Could not find any valid build submissions to count votes.",
            ephemeral=True
        )
        return
    
    # Sort by vote count (descending), then by submission time (ascending for tiebreaker)
    build_votes.sort(key=lambda x: (-x[1], x[0].get("submitted_at", 0)))
    
    # Get winner
    winner_build, winner_votes = build_votes[0]
    winner_user_id = winner_build.get("user_id")
    winner_class = winner_build.get("guardian_class", "Unknown")
    winner_subclass = winner_build.get("subclass", "Unknown")
    winner_activity = winner_build.get("activity", "Unknown")
    winner_message_id = winner_build.get("message_id")
    winner_thread_id = winner_build.get("thread_id")
    winner_build_title = winner_build.get("build_title")
    if not winner_build_title:
        # Back-compat for older stored builds
        winner_build_title = f"{winner_class} {winner_subclass} - {winner_activity}"
    
    # Build results summary for admin
    summary_lines = ["**Vote Results:**"]
    for i, (build, votes) in enumerate(build_votes[:10], 1):
        user_id = build.get("user_id")
        activity = build.get("activity", "Unknown")
        medal = "🥇" if i == 1 else ("🥈" if i == 2 else ("🥉" if i == 3 else f"{i}."))
        summary_lines.append(f"{medal} <@{user_id}> — {activity} — **{votes}** votes")
    
    summary = "\n".join(summary_lines)
    
    # Announce winner if requested
    if announce:
        # Create winner announcement embed
        winner_embed = discord.Embed(
            title="🏆 BUILD OF THE WEEK WINNER! 🏆",
            color=0xFFD700  # Gold
        )

        winner_allowed_mentions = discord.AllowedMentions(everyone=True, users=True, roles=False)

        # Human-readable announcement content (requested template)
        # Note: @everyone is only prefixed for the GENERAL channel post.
        announcement_text = (
            "🏆 BUILD OF THE WEEK WINNER 🏆\n"
            f"Congratulations to <@{winner_user_id}> on taking Build of the Week!\n"
            f"Your build **{winner_build_title}** earned the most votes and stood out from the rest — well deserved 👏\n\n"
            "🎁 What you won:\n"
            "• $10 per video explaining your build\n"
            "• Your build featured in Monday’s raid\n"
            "• A dedicated video will be made breaking down your build\n\n"
            "We’ll reach out with details on the video and raid feature.\n"
            "Big respect to everyone who submitted — new week, new builds 🔥"
        )
        
        winner_embed.add_field(
            name="🎉 Congratulations!",
            value=f"<@{winner_user_id}>",
            inline=False
        )

        winner_embed.add_field(
            name="🏷️ Build Title",
            value=f"**{winner_build_title}**",
            inline=False
        )
        
        winner_embed.add_field(
            name="🔨 Winning Build",
            value=f"**{winner_class} {winner_subclass}** for **{winner_activity}**",
            inline=False
        )
        
        winner_embed.add_field(
            name="📊 Votes",
            value=f"**{winner_votes}** votes",
            inline=True
        )
        
        # Link to winning build
        # For forum posts: channel_id should be the thread id, and message_id should be the starter message id.
        jump_url = None
        try:
            if interaction.guild_id and winner_thread_id and winner_message_id:
                jump_url = f"https://discord.com/channels/{interaction.guild_id}/{int(winner_thread_id)}/{int(winner_message_id)}"
            elif interaction.guild_id and BUILD_OF_THE_WEEK_CHANNEL_ID and winner_message_id:
                jump_url = f"https://discord.com/channels/{interaction.guild_id}/{int(BUILD_OF_THE_WEEK_CHANNEL_ID)}/{int(winner_message_id)}"
        except Exception:
            jump_url = None

        if jump_url:
            announcement_text = f"{announcement_text}\n\n🔗 View the winning build: {jump_url}"

        if jump_url:
            try:
                winner_embed.add_field(
                    name="🔗 View Build",
                    value=f"[Jump to Build]({jump_url})",
                    inline=True
                )
            except Exception:
                pass
        
        # Week info in footer
        week_end = datetime.strptime(week_start, "%Y-%m-%d") + timedelta(days=6)
        week_end_str = week_end.strftime("%Y-%m-%d")
        winner_embed.set_footer(text=f"Week of {week_start} to {week_end_str}")
        
        try:
            # Post in the Build of the Week channel (forum => create a new post)
            if isinstance(channel, discord.ForumChannel):
                post_name = f"🏆 Winner — Week of {week_start}"
                if len(post_name) > 100:
                    post_name = post_name[:97] + "..."
                await channel.create_thread(name=post_name, content=announcement_text, embed=winner_embed)
            else:
                await channel.send(content=announcement_text, embed=winner_embed, allowed_mentions=winner_allowed_mentions)
        except Exception as e:
            await interaction.followup.send(
                f"Failed to post winner announcement: {e}\n\n{summary}",
                ephemeral=True
            )
            return

        # Also announce in #general (if configured and different from the BoTW channel)
        try:
            if GENERAL_CHANNEL_ID and int(GENERAL_CHANNEL_ID) != int(BUILD_OF_THE_WEEK_CHANNEL_ID):
                await _send_to_channel_id(
                    int(GENERAL_CHANNEL_ID),
                    content=f"@everyone\n\n{announcement_text}",
                    embed=winner_embed,
                    allowed_mentions=winner_allowed_mentions,
                )
        except Exception:
            pass

        # DM the winner with the announcement (no @everyone)
        dm_sent = False
        dm_error = None
        try:
            winner_user = None
            try:
                if interaction.guild:
                    winner_user = interaction.guild.get_member(int(winner_user_id))
            except Exception:
                winner_user = None

            if not winner_user:
                try:
                    winner_user = bot.get_user(int(winner_user_id))
                except Exception:
                    winner_user = None

            if not winner_user:
                try:
                    winner_user = await bot.fetch_user(int(winner_user_id))
                except Exception:
                    winner_user = None

            if winner_user:
                await winner_user.send(content=announcement_text, embed=winner_embed)
                dm_sent = True
        except Exception as e:
            dm_error = str(e)
        
        # Store winner record
        winner_record = {
            "week_of": week_start,
            "build_id": winner_build.get("id"),
            "message_id": winner_message_id,
            "user_id": winner_user_id,
            "vote_count": winner_votes,
            "announced_at": int(datetime.now().timestamp()),
        }
        await add_winner(winner_record)
    
    # Send summary to admin
    dm_note = ""
    try:
        if announce:
            if dm_sent:
                dm_note = "\n\n📩 Winner was also DM’d."
            elif dm_error:
                dm_note = f"\n\n⚠️ Could not DM winner: {dm_error}"
            else:
                dm_note = "\n\n⚠️ Could not DM winner (unknown reason)."
    except Exception:
        dm_note = ""
    await interaction.followup.send(
        f"✅ Build of the Week winner determined!\n\n{summary}{dm_note}",
        ephemeral=True
    )


@bot.tree.command(name="deletebuild", description="(Admin) Delete a build submission")
@founder_only()
@app_commands.describe(
    thread_or_message_id="The thread ID (for forum) or message ID of the build to delete"
)
async def deletebuild_cmd(
    interaction: discord.Interaction,
    thread_or_message_id: str
):
    await interaction.response.defer(ephemeral=True)
    
    # Parse ID
    try:
        target_id = int(thread_or_message_id.strip())
    except ValueError:
        await interaction.followup.send(
            "Invalid ID. Please provide a valid numeric thread or message ID.",
            ephemeral=True
        )
        return
    
    # Validate channel is configured
    if not BUILD_OF_THE_WEEK_CHANNEL_ID:
        await interaction.followup.send(
            "Build of the Week channel is not configured.",
            ephemeral=True
        )
        return
    
    # Get channel
    channel = bot.get_channel(BUILD_OF_THE_WEEK_CHANNEL_ID)
    if not channel:
        try:
            channel = await bot.fetch_channel(BUILD_OF_THE_WEEK_CHANNEL_ID)
        except Exception:
            pass
    
    if not channel:
        await interaction.followup.send(
            "Could not find the Build of the Week channel.",
            ephemeral=True
        )
        return
    
    # Try to delete - handle both forum threads and regular messages
    message_deleted = False
    thread_deleted = False
    
    # First, try to find and delete as a forum thread
    if isinstance(channel, discord.ForumChannel):
        try:
            thread = channel.get_thread(target_id)
            if not thread:
                thread = await bot.fetch_channel(target_id)
            if thread and isinstance(thread, discord.Thread):
                await thread.delete()
                thread_deleted = True
        except discord.NotFound:
            pass
        except discord.Forbidden:
            await interaction.followup.send(
                "Bot lacks permission to delete threads in that channel.",
                ephemeral=True
            )
            return
        except Exception:
            pass  # Try message deletion as fallback
    
    # If not a thread or thread deletion failed, try as a message
    if not thread_deleted:
        try:
            # For forum channels, we need to search in threads
            if isinstance(channel, discord.ForumChannel):
                # Try to find the message in archived or active threads
                msg = None
                async for thread in channel.archived_threads():
                    try:
                        msg = await thread.fetch_message(target_id)
                        await msg.delete()
                        message_deleted = True
                        break
                    except discord.NotFound:
                        continue
                if not message_deleted:
                    for thread in channel.threads:
                        try:
                            msg = await thread.fetch_message(target_id)
                            await msg.delete()
                            message_deleted = True
                            break
                        except discord.NotFound:
                            continue
            else:
                msg = await channel.fetch_message(target_id)
                await msg.delete()
                message_deleted = True
        except discord.NotFound:
            pass  # Message already deleted, continue to remove from storage
        except discord.Forbidden:
            await interaction.followup.send(
                "Bot lacks permission to delete messages in that channel.",
                ephemeral=True
            )
            return
        except Exception as e:
            # Continue to try removing from storage
            pass
    
    # Remove from storage - try both thread_id and message_id
    deleted_build = await delete_build(target_id)
    if not deleted_build:
        # Also try looking up by thread_id in the build data
        deleted_build = await delete_build_by_thread(target_id)
    
    if deleted_build:
        user_id = deleted_build.get("user_id")
        activity = deleted_build.get("activity", "Unknown")
        guardian_class = deleted_build.get("guardian_class", "Unknown")
        await interaction.followup.send(
            f"✅ Build deleted!\n"
            f"**Submitter:** <@{user_id}>\n"
            f"**Build:** {guardian_class} — {activity}\n"
            f"**Message:** {'Deleted from channel' if message_deleted else 'Already removed from channel'}",
            ephemeral=True
        )
    elif message_deleted:
        await interaction.followup.send(
            f"✅ Message deleted from channel, but no matching build was found in storage.",
            ephemeral=True
        )
    else:
        await interaction.followup.send(
            f"No build found with message ID `{msg_id}`.",
            ephemeral=True
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
