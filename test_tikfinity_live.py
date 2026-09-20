import asyncio
import json
import os
import tempfile
import unittest
from typing import List
from unittest.mock import patch

from aiohttp import web

from tikfinity_live import (
    DEFAULT_TIKTOK_URL,
    DEFAULT_TWITCH_URL,
    INTERACTION_EVENTS,
    LiveLinksView,
    TikFinityLiveMonitor,
    build_live_announcement_text,
    extract_event_name,
    extract_room_id,
    load_tikfinity_config,
    parse_tikfinity_messages,
)


class EventParseTests(unittest.TestCase):
    def test_event_and_data_shape(self) -> None:
        payload = {"event": "gift", "data": {"giftName": "Rose"}}
        self.assertEqual(extract_event_name(payload), "gift")

    def test_event_name_field(self) -> None:
        self.assertEqual(extract_event_name({"eventName": "chat"}), "chat")

    def test_type_field(self) -> None:
        self.assertEqual(extract_event_name({"type": "like"}), "like")

    def test_nested_event_object(self) -> None:
        self.assertEqual(extract_event_name({"event": {"name": "follow"}}), "follow")

    def test_room_id_nested(self) -> None:
        payload = {"event": "connected", "data": {"roomId": "7137682087200557829"}}
        self.assertEqual(extract_room_id(payload), "7137682087200557829")

    def test_parse_message_list(self) -> None:
        raw = json.dumps({"messages": [{"event": "gift"}, {"event": "like"}]})
        items = parse_tikfinity_messages(raw)
        self.assertEqual(len(items), 2)


class LiveDetectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.logs: List[str] = []
        self.monitor = TikFinityLiveMonitor(log=self.logs.append)

    def test_announcement_text(self) -> None:
        text = build_live_announcement_text(DEFAULT_TIKTOK_URL, DEFAULT_TWITCH_URL)
        self.assertEqual(
            text,
            "🔴 @everyone I'M LIVE!\n"
            "\n"
            "Come hang out with me!\n"
            "\n"
            "🎵 TikTok:\nhttps://www.tiktok.com/@GFerryGoon\n"
            "\n"
            "🟣 Twitch:\nhttps://www.twitch.tv/gferrygoon",
        )

    def test_connected_announces_once(self) -> None:
        raw = json.dumps({"event": "connected", "data": {"roomId": "room-1"}})
        self.assertEqual(self.monitor.evaluate_raw_message(raw), ["announce"])
        self.assertEqual(self.monitor.evaluate_raw_message(raw), ["ignore"])

    def test_stream_start_alias(self) -> None:
        raw = json.dumps({"event": "streamStart", "data": {"roomId": "abc"}})
        self.assertEqual(self.monitor.evaluate_raw_message(raw), ["announce"])

    def test_interactions_never_announce(self) -> None:
        self.monitor.evaluate_raw_message(json.dumps({"event": "connected", "data": {"roomId": "r"}}))
        for name in ("like", "chat", "comment", "gift", "follow", "share", "subscribe", "member", "roomUser"):
            actions = self.monitor.evaluate_raw_message(json.dumps({"event": name, "data": {}}))
            self.assertEqual(actions, ["ignore"], name)
            self.assertTrue(any(f"event: {name}" in line for line in self.logs), name)

    def test_first_interaction_does_not_announce(self) -> None:
        for name in ("like", "gift", "comment", "follow"):
            monitor = TikFinityLiveMonitor(log=lambda _m: None)
            actions = monitor.evaluate_raw_message(json.dumps({"event": name}))
            self.assertEqual(actions, ["ignore"], name)
            self.assertFalse(monitor.session_announced)

    def test_new_room_is_a_new_session(self) -> None:
        self.monitor.evaluate_raw_message(json.dumps({"event": "connected", "data": {"roomId": "a"}}))
        actions = self.monitor.evaluate_raw_message(json.dumps({"event": "connected", "data": {"roomId": "b"}}))
        self.assertEqual(actions, ["announce"])

    def test_stream_end_allows_next_live(self) -> None:
        self.monitor.evaluate_raw_message(json.dumps({"event": "connected", "data": {"roomId": "a"}}))
        self.assertEqual(self.monitor.evaluate_raw_message(json.dumps({"event": "streamEnd"})), ["end"])
        actions = self.monitor.evaluate_raw_message(json.dumps({"event": "connected", "data": {"roomId": "a"}}))
        self.assertEqual(actions, ["announce"])

    def test_event_names_are_logged(self) -> None:
        self.monitor.evaluate_raw_message(json.dumps({"event": "gift"}))
        self.assertTrue(any("event: gift" in line for line in self.logs))

    def test_custom_start_event(self) -> None:
        monitor = TikFinityLiveMonitor(live_start_events=["myLivePing"], log=lambda _m: None)
        actions = monitor.evaluate_raw_message(json.dumps({"event": "myLivePing", "data": {"roomId": "z"}}))
        self.assertEqual(actions, ["announce"])

    def test_state_survives_reload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            first = TikFinityLiveMonitor(data_dir=tmp, log=lambda _m: None)
            first.evaluate_raw_message(json.dumps({"event": "connected", "data": {"roomId": "keep"}}))
            second = TikFinityLiveMonitor(data_dir=tmp, log=lambda _m: None)
            actions = second.evaluate_raw_message(json.dumps({"event": "connected", "data": {"roomId": "keep"}}))
            self.assertEqual(actions, ["ignore"])

    def test_interaction_constants_cover_common_noise(self) -> None:
        for name in ("gift", "like", "chat", "comment", "follow"):
            self.assertIn(name, INTERACTION_EVENTS)

    def test_link_buttons(self) -> None:
        view = LiveLinksView(DEFAULT_TIKTOK_URL, DEFAULT_TWITCH_URL)
        labels = [item.label for item in view.children]
        self.assertEqual(labels, ["🎵 TikTok", "🟣 Twitch"])
        urls = [item.url for item in view.children]
        self.assertEqual(urls, [DEFAULT_TIKTOK_URL, DEFAULT_TWITCH_URL])


class ConfigTests(unittest.TestCase):
    def test_load_config_defaults(self) -> None:
        env = {
            "TIKFINITY_WS_URL": "ws://localhost:21213/",
            "DISCORD_LIVE_CHANNEL_ID": "123",
            "TIKTOK_LIVE_URL": "https://www.tiktok.com/@GFerryGoon",
            "TWITCH_LIVE_URL": "https://www.twitch.tv/gferrygoon",
        }
        with patch.dict(os.environ, env, clear=False):
            cfg = load_tikfinity_config(general_channel_id=999)
        self.assertEqual(cfg["ws_url"], "ws://localhost:21213/")
        self.assertEqual(cfg["channel_id"], 123)
        self.assertEqual(cfg["tiktok_url"], "https://www.tiktok.com/@GFerryGoon")
        self.assertEqual(cfg["twitch_url"], "https://www.twitch.tv/gferrygoon")


class WebSocketReconnectTests(unittest.IsolatedAsyncioTestCase):
    async def test_reconnect_does_not_reannounce_same_room(self) -> None:
        announced: List[int] = []
        logs: List[str] = []
        release_second = asyncio.Event()
        connections = {"count": 0}

        async def ws_handler(request: web.Request) -> web.WebSocketResponse:
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            connections["count"] += 1
            await ws.send_json({"event": "connected", "data": {"roomId": "same-live"}})
            await ws.send_json({"event": "gift", "data": {"giftName": "Rose"}})
            await ws.send_json({"event": "like", "data": {}})
            if connections["count"] == 1:
                await ws.close()
            else:
                await release_second.wait()
                await ws.close()
            return ws

        app = web.Application()
        app.router.add_get("/", ws_handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]

        async def on_announce() -> None:
            announced.append(1)

        monitor = TikFinityLiveMonitor(
            ws_url=f"ws://127.0.0.1:{port}/",
            on_announce=on_announce,
            reconnect_initial=0.05,
            reconnect_max=0.1,
            log=logs.append,
        )
        task = asyncio.create_task(monitor.run_forever())
        try:
            for _ in range(80):
                if connections["count"] >= 2 and announced:
                    break
                await asyncio.sleep(0.05)
            self.assertGreaterEqual(connections["count"], 2)
            self.assertEqual(sum(announced), 1)
            self.assertTrue(any("event: gift" in line for line in logs))
            self.assertTrue(any("event: like" in line for line in logs))
            self.assertTrue(any("event: connected" in line for line in logs))
        finally:
            release_second.set()
            monitor.stop()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            await runner.cleanup()


if __name__ == "__main__":
    unittest.main()
