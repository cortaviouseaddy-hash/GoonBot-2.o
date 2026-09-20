"""TikFinity Event API client: detect a new TikTok LIVE and announce it once."""

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Set

import aiohttp
import discord


DEFAULT_WS_URL = "ws://localhost:21213/"
DEFAULT_TIKTOK_URL = "https://www.tiktok.com/@GFerryGoon"
DEFAULT_TWITCH_URL = "https://www.twitch.tv/gferrygoon"
STATE_FILENAME = "tikfinity_live_state.json"

# Handshake / socket status only. These prove the bot reached TikFinity,
# not that a TikTok LIVE session started. Never announce from these.
CONNECTION_EVENTS = frozenset(
    {
        "connected",
        "disconnected",
        "websocketconnected",
        "websocket_connected",
        "websocketdisconnected",
        "websocket_disconnected",
        "ready",
        "welcome",
        "handshake",
        "hello",
        "ping",
        "pong",
        "error",
    }
)

# Empty on purpose until a real LIVE-start event is identified from logs.
# Set TIKFINITY_LIVE_START_EVENTS after a test stream (do not include "connected").
DEFAULT_LIVE_START_EVENTS = frozenset()

DEFAULT_LIVE_END_EVENTS = frozenset(
    {
        "streamend",
        "stream_end",
        "streamended",
        "livestreamend",
        "livestream_end",
        "liveend",
        "live_end",
        "roomend",
        "room_end",
    }
)

# Viewer interaction noise — log the names, never announce from these.
INTERACTION_EVENTS = frozenset(
    {
        "gift",
        "like",
        "likes",
        "chat",
        "comment",
        "comments",
        "follow",
        "share",
        "subscribe",
        "subscription",
        "member",
        "join",
        "roomuser",
        "room_user",
        "social",
        "emote",
        "envelope",
        "questionnew",
        "question_new",
        "liveintro",
        "live_intro",
        "linkmicbattle",
        "linkmicarmies",
        "linkmic",
    }
)


def _norm_event(name: Optional[str]) -> str:
    return "".join(ch for ch in str(name or "").strip().lower() if ch.isalnum() or ch in ("_", "-"))


def _env_int(name: str) -> Optional[int]:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return int(str(raw).strip())
    except Exception:
        return None


def _csv_events(raw: Optional[str]) -> Set[str]:
    out: Set[str] = set()
    if not raw:
        return out
    for part in str(raw).split(","):
        n = _norm_event(part)
        if n:
            out.add(n)
    return out


def extract_event_name(payload: Any) -> Optional[str]:
    """Best-effort event name from TikFinity / TikTok-Live-Connector JSON shapes."""
    if isinstance(payload, str):
        n = payload.strip()
        return n or None
    if not isinstance(payload, dict):
        return None
    for key in ("event", "eventName", "event_name", "eventType", "event_type", "type", "name", "status"):
        val = payload.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
        if isinstance(val, dict):
            nested = extract_event_name(val)
            if nested:
                return nested
    data = payload.get("data")
    if isinstance(data, dict):
        nested = extract_event_name(data)
        if nested:
            return nested
    return None


def dump_payload(payload: Any, *, limit: int = 2000) -> str:
    try:
        text = json.dumps(payload, default=str, ensure_ascii=False, sort_keys=True)
    except Exception:
        text = str(payload)
    if len(text) > limit:
        return text[:limit] + "...(truncated)"
    return text


def extract_room_id(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    for key in ("roomId", "room_id", "roomid", "liveRoomId", "live_room_id"):
        val = payload.get(key)
        if val is not None and str(val).strip() != "":
            return str(val).strip()
    data = payload.get("data")
    if isinstance(data, dict):
        found = extract_room_id(data)
        if found:
            return found
        info = data.get("roomInfo") or data.get("room_info")
        if isinstance(info, dict):
            found = extract_room_id(info)
            if found:
                return found
    info = payload.get("roomInfo") or payload.get("room_info")
    if isinstance(info, dict):
        found = extract_room_id(info)
        if found:
            return found
    return None


def parse_tikfinity_messages(raw: str) -> List[Dict[str, Any]]:
    """Turn a WebSocket text frame into a list of event dicts."""
    text = (raw or "").strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except Exception:
        return [{"event": text, "_unparsed": True}]

    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict) or isinstance(item, str)]

    if isinstance(payload, dict):
        for key in ("messages", "events", "payloads"):
            items = payload.get(key)
            if isinstance(items, list) and items:
                return [item for item in items if isinstance(item, (dict, str))]
        return [payload]

    if isinstance(payload, str):
        return [{"event": payload}]
    return []


def build_live_announcement_text(tiktok_url: str, twitch_url: str) -> str:
    return (
        "🔴 @everyone I'M LIVE!\n"
        "\n"
        "Come hang out with me!\n"
        "\n"
        f"🎵 TikTok:\n{tiktok_url}\n"
        "\n"
        f"🟣 Twitch:\n{twitch_url}"
    )


class LiveLinksView(discord.ui.View):
    """Persistent-looking link buttons; Discord stores the URLs on the message."""

    def __init__(self, tiktok_url: str, twitch_url: str):
        super().__init__(timeout=None)
        if tiktok_url:
            self.add_item(
                discord.ui.Button(
                    label="🎵 TikTok",
                    style=discord.ButtonStyle.link,
                    url=tiktok_url,
                )
            )
        if twitch_url:
            self.add_item(
                discord.ui.Button(
                    label="🟣 Twitch",
                    style=discord.ButtonStyle.link,
                    url=twitch_url,
                )
            )


class TikFinityLiveMonitor:
    def __init__(
        self,
        *,
        ws_url: str = DEFAULT_WS_URL,
        live_start_events: Optional[Iterable[str]] = None,
        live_end_events: Optional[Iterable[str]] = None,
        data_dir: Optional[str] = None,
        on_announce: Optional[Callable[[], Awaitable[None]]] = None,
        reconnect_initial: float = 2.0,
        reconnect_max: float = 30.0,
        log: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.ws_url = (ws_url or DEFAULT_WS_URL).strip() or DEFAULT_WS_URL
        extra_start = {_norm_event(n) for n in (live_start_events or []) if _norm_event(n)}
        extra_end = {_norm_event(n) for n in (live_end_events or []) if _norm_event(n)}
        # Connection/handshake names can never be used as LIVE-start, even via env.
        self.live_start_events = (set(DEFAULT_LIVE_START_EVENTS) | extra_start) - CONNECTION_EVENTS
        self.live_end_events = set(DEFAULT_LIVE_END_EVENTS) | extra_end
        self.data_dir = data_dir
        self.on_announce = on_announce
        self.reconnect_initial = reconnect_initial
        self.reconnect_max = reconnect_max
        self._log = log or (lambda msg: print(msg, flush=True))
        self._stop = asyncio.Event()
        self.announced_room_id: Optional[str] = None
        self.session_announced: bool = False
        self.last_announced_at: float = 0.0
        self._load_state()

    def stop(self) -> None:
        self._stop.set()

    def _state_path(self) -> Optional[str]:
        if not self.data_dir:
            return None
        return os.path.join(self.data_dir, STATE_FILENAME)

    def _load_state(self) -> None:
        path = self._state_path()
        if not path or not os.path.isfile(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            room = data.get("announced_room_id")
            self.announced_room_id = str(room).strip() if room else None
            self.session_announced = bool(data.get("session_announced"))
            self.last_announced_at = float(data.get("last_announced_at") or 0.0)
        except Exception as exc:
            self._log(f"[tikfinity] could not load live state: {exc}")

    def _save_state(self) -> None:
        path = self._state_path()
        if not path:
            return
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as handle:
                json.dump(
                    {
                        "announced_room_id": self.announced_room_id,
                        "session_announced": self.session_announced,
                        "last_announced_at": self.last_announced_at,
                    },
                    handle,
                )
            os.replace(tmp, path)
        except Exception as exc:
            self._log(f"[tikfinity] could not save live state: {exc}")

    def _reset_session(self) -> None:
        self.announced_room_id = None
        self.session_announced = False
        self._save_state()

    def is_connection_status(self, event_name: Optional[str]) -> bool:
        return _norm_event(event_name) in CONNECTION_EVENTS

    def is_live_start(self, event_name: Optional[str]) -> bool:
        n = _norm_event(event_name)
        if not n or n in CONNECTION_EVENTS:
            return False
        return n in self.live_start_events

    def is_live_end(self, event_name: Optional[str]) -> bool:
        n = _norm_event(event_name)
        if not n or n in CONNECTION_EVENTS:
            return False
        return n in self.live_end_events

    def is_interaction(self, event_name: Optional[str]) -> bool:
        return _norm_event(event_name) in INTERACTION_EVENTS

    def evaluate_payload(self, payload: Any) -> str:
        """
        Update session state for one event object.

        Returns one of: announce, end, ignore, unknown
        """
        event_name = extract_event_name(payload)
        room_id = extract_room_id(payload)
        dumped = dump_payload(payload)
        display = event_name or "<unnamed>"

        kind = "logged"
        action = "ignore"
        if not event_name:
            kind = "unnamed"
            action = "unknown"
        elif self.is_connection_status(event_name):
            kind = "TikFinity/socket status — not a LIVE"
            action = "ignore"
        elif self.is_live_end(event_name):
            kind = "live end"
            action = "end"
        elif self.is_live_start(event_name):
            kind = "live start"
            if self.session_announced and (
                not room_id or not self.announced_room_id or room_id == self.announced_room_id
            ):
                kind = "live start, already announced for this session"
                action = "ignore"
            elif room_id and self.announced_room_id == room_id and self.session_announced:
                kind = "live start, already announced for this session"
                action = "ignore"
            else:
                action = "announce"
        elif self.is_interaction(event_name):
            kind = "LIVE data (logged, no Discord ping)"
            action = "ignore"

        extra = f" roomId={room_id}" if room_id else ""
        self._log(f"[tikfinity] event: {display} ({kind}){extra} payload={dumped}")

        if action == "end":
            self._reset_session()
            return "end"

        if action == "announce":
            self.session_announced = True
            self.announced_room_id = room_id or self.announced_room_id
            self.last_announced_at = time.time()
            self._save_state()
            return "announce"

        return action if action in ("ignore", "unknown") else "ignore"

    def evaluate_raw_message(self, raw: str) -> List[str]:
        items = parse_tikfinity_messages(raw)
        if not items:
            preview = (raw or "").strip()
            if preview:
                self._log(f"[tikfinity] event: <raw> payload={preview[:2000]}")
                return ["unknown"]
            return []
        actions: List[str] = []
        for item in items:
            if isinstance(item, str):
                item = {"event": item}
            actions.append(self.evaluate_payload(item))
        return actions

    async def handle_raw_message(self, raw: str) -> List[str]:
        actions = self.evaluate_raw_message(raw)
        if "announce" in actions and self.on_announce is not None:
            self._log("[tikfinity] new LIVE session detected; sending Discord announcement")
            await self.on_announce()
        return actions

    async def run_forever(self) -> None:
        delay = self.reconnect_initial
        failures = 0
        while not self._stop.is_set():
            try:
                self._log(f"[tikfinity] connecting to {self.ws_url}")
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=None)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(
                        self.ws_url,
                        heartbeat=20,
                        autoclose=True,
                        autoping=True,
                    ) as ws:
                        self._log(
                            "[tikfinity] socket connected to TikFinity Event API — "
                            "this is NOT a TikTok LIVE and will not ping Discord"
                        )
                        delay = self.reconnect_initial
                        failures = 0
                        async for msg in ws:
                            if self._stop.is_set():
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self.handle_raw_message(msg.data)
                            elif msg.type == aiohttp.WSMsgType.BINARY:
                                await self.handle_raw_message(msg.data.decode("utf-8", "replace"))
                            elif msg.type in (
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.CLOSE,
                                aiohttp.WSMsgType.ERROR,
                            ):
                                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failures += 1
                if failures <= 3 or failures % 10 == 0:
                    self._log(f"[tikfinity] disconnected ({exc}); reconnecting in {delay:.0f}s")
            else:
                if not self._stop.is_set():
                    failures += 1
                    if failures <= 3 or failures % 10 == 0:
                        self._log(f"[tikfinity] disconnected; reconnecting in {delay:.0f}s")
            if self._stop.is_set():
                break
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            delay = min(max(delay, 0.1) * 2, self.reconnect_max)


def load_tikfinity_config(
    *,
    general_channel_id: Optional[int] = None,
    live_channel_id: Optional[int] = None,
    data_dir: Optional[str] = None,
) -> Dict[str, Any]:
    ws_url = (os.getenv("TIKFINITY_WS_URL") or DEFAULT_WS_URL).strip() or DEFAULT_WS_URL
    channel_id = _env_int("DISCORD_LIVE_CHANNEL_ID") or live_channel_id or general_channel_id
    tiktok_url = (os.getenv("TIKTOK_LIVE_URL") or DEFAULT_TIKTOK_URL).strip() or DEFAULT_TIKTOK_URL
    twitch_url = (os.getenv("TWITCH_LIVE_URL") or DEFAULT_TWITCH_URL).strip() or DEFAULT_TWITCH_URL
    extra_start = _csv_events(os.getenv("TIKFINITY_LIVE_START_EVENTS")) - CONNECTION_EVENTS
    extra_end = _csv_events(os.getenv("TIKFINITY_LIVE_END_EVENTS"))
    return {
        "ws_url": ws_url,
        "channel_id": channel_id,
        "tiktok_url": tiktok_url,
        "twitch_url": twitch_url,
        "live_start_events": extra_start,
        "live_end_events": extra_end,
        "data_dir": data_dir,
    }


async def announce_live_to_discord(
    bot: discord.Client,
    *,
    channel_id: Optional[int],
    tiktok_url: str,
    twitch_url: str,
    send_func: Optional[Callable[..., Awaitable[Any]]] = None,
) -> None:
    if not channel_id:
        print("[tikfinity] DISCORD_LIVE_CHANNEL_ID / GENERAL_CHANNEL_ID is not set; cannot announce")
        return
    content = build_live_announcement_text(tiktok_url, twitch_url)
    view = LiveLinksView(tiktok_url, twitch_url)
    mentions = discord.AllowedMentions(everyone=True)
    if send_func is not None:
        await send_func(
            int(channel_id),
            content=content,
            allowed_mentions=mentions,
            view=view,
            suppress_embeds=True,
        )
        return
    channel = bot.get_channel(int(channel_id)) or await bot.fetch_channel(int(channel_id))
    await channel.send(  # type: ignore[union-attr]
        content=content,
        allowed_mentions=mentions,
        view=view,
        suppress_embeds=True,
    )


def start_tikfinity_monitor(
    bot: discord.Client,
    *,
    general_channel_id: Optional[int],
    live_channel_id: Optional[int],
    data_dir: str,
    send_func: Optional[Callable[..., Awaitable[Any]]] = None,
) -> asyncio.Task:
    cfg = load_tikfinity_config(
        general_channel_id=general_channel_id,
        live_channel_id=live_channel_id,
        data_dir=data_dir,
    )

    async def _on_announce() -> None:
        await announce_live_to_discord(
            bot,
            channel_id=cfg["channel_id"],
            tiktok_url=cfg["tiktok_url"],
            twitch_url=cfg["twitch_url"],
            send_func=send_func,
        )

    monitor = TikFinityLiveMonitor(
        ws_url=cfg["ws_url"],
        live_start_events=cfg["live_start_events"],
        live_end_events=cfg["live_end_events"],
        data_dir=cfg["data_dir"],
        on_announce=_on_announce,
    )
    bot._tikfinity_monitor = monitor  # type: ignore[attr-defined]
    start_names = ",".join(sorted(monitor.live_start_events)) or "(none yet — logging only)"
    print(
        f"[tikfinity] LIVE monitor enabled → {cfg['ws_url']} "
        f"(announce channel={cfg['channel_id']}; live-start events={start_names})",
        flush=True,
    )
    print(
        "[tikfinity] connecting to TikFinity will not send @everyone. "
        "Every incoming event is logged so a real LIVE-start event can be identified.",
        flush=True,
    )
    return bot.loop.create_task(monitor.run_forever(), name="tikfinity-live-monitor")
