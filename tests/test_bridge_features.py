"""NDFC-NFC bridge 的回复、习惯和状态扩展测试。"""

from __future__ import annotations

from contextlib import asynccontextmanager
from importlib import import_module
from types import SimpleNamespace
from typing import AsyncIterator

import pytest

actions = import_module("plugins.neo-ndfc-nfc-bridge.actions")
handlers = import_module("plugins.neo-ndfc-nfc-bridge.handlers")
state = import_module("plugins.neo-ndfc-nfc-bridge.state")


class _SessionStore:
    """提供 bridge Action 所需的最小会话存储。"""

    def __init__(self, session: object) -> None:
        self.session = session
        self.saved = 0

    @asynccontextmanager
    async def lock(self, stream_id: str) -> AsyncIterator[None]:
        """提供无竞争的 per-stream 锁。"""
        assert stream_id == self.session.stream_id
        yield

    async def get_or_create(self, stream_id: str) -> object:
        """返回唯一测试会话。"""
        assert stream_id == self.session.stream_id
        return self.session

    async def peek(self, stream_id: str) -> object:
        """返回唯一测试会话。"""
        assert stream_id == self.session.stream_id
        return self.session

    async def save(self, session: object) -> None:
        """记录持久化次数。"""
        assert session is self.session
        self.saved += 1


def _plugin(session: object) -> SimpleNamespace:
    """构造私聊 bridge Action 所需的插件替身。"""
    return SimpleNamespace(
        config=SimpleNamespace(
            bridge=SimpleNamespace(
                enabled=True,
                private_only=True,
                use_nfc_history=True,
                persist_mental_state=True,
                use_nfc_waiting=False,
            ),
            reply=SimpleNamespace(segment_delay_min=0.0, segment_delay_max=0.0),
            proactive=SimpleNamespace(enabled=True),
            prompt=SimpleNamespace(
                request_snapshot_enabled=True,
                summary_enabled=False,
                compress_every_n_rounds=50,
                min_compress_interval_minutes=120.0,
            ),
        ),
        session_store=_SessionStore(session),
    )


@pytest.mark.asyncio
async def test_reply_action_uses_reply_to_for_first_segment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """bridge 的首段回复应经宿主标准 send_api 携带 reply_to。"""
    session = state.BridgeSession(user_id="user", stream_id="private-1")
    plugin = _plugin(session)
    chat_stream = SimpleNamespace(stream_id="private-1", chat_type="private")
    action = actions.NDFCReplyAction(chat_stream=chat_stream, plugin=plugin)
    reply_calls: list[tuple[str, str, str]] = []
    normal_calls: list[str] = []

    async def send_reply(content: str, stream_id: str, reply_to: str) -> bool:
        reply_calls.append((content, stream_id, reply_to))
        return True

    async def send_normal(content: str) -> bool:
        normal_calls.append(content)
        return True

    monkeypatch.setattr(actions.send_api, "send_text", send_reply)
    monkeypatch.setattr(action, "_send_to_stream", send_normal)

    success, message = await action.execute(
        content=["第一句", "第二句"],
        reply_to="message-1",
    )

    assert success is True
    assert message == "已发送 2 条消息"
    assert reply_calls == [("第一句", "private-1", "message-1")]
    assert normal_calls == ["第二句"]


@pytest.mark.asyncio
async def test_habit_actions_can_query_update_and_remove() -> None:
    """bridge 应可查询带 ID 的习惯，再更正或删除。"""
    session = state.BridgeSession(user_id="user", stream_id="private-1")
    session.add_habit("通常 23 点睡觉", "sleep")
    plugin = _plugin(session)
    chat_stream = SimpleNamespace(stream_id="private-1", chat_type="private")

    query_action = actions.NDFCQueryUserHabitsAction(
        chat_stream=chat_stream,
        plugin=plugin,
    )
    success, listing = await query_action.execute()
    habit_id = session.user_habits[0]["id"]

    assert success is True
    assert habit_id in listing

    success, _ = await actions.NDFCUpdateUserHabitAction(
        chat_stream=chat_stream,
        plugin=plugin,
    ).execute(habit_id=habit_id, habit_text="通常 24 点睡觉")
    assert success is True
    assert session.user_habits[0]["habit_text"] == "通常 24 点睡觉"

    success, _ = await actions.NDFCRemoveUserHabitAction(
        chat_stream=chat_stream,
        plugin=plugin,
    ).execute(habit_id=habit_id)
    assert success is True
    assert session.user_habits == []
    assert plugin.session_store.saved == 2


def test_session_persists_summary_counter_habits_and_request_snapshot() -> None:
    """bridge session 应往返保存新增的长期状态。"""
    session = state.BridgeSession(user_id="user", stream_id="private-1")
    session.add_habit("喜欢夜跑", "hobby")
    session.history_summary = "最近聊了夜跑。"
    session.last_compress_at = 100.0
    session.compress_round_count = 3
    session.request_snapshot = {"stream_id": "private-1", "entries": []}

    restored = state.BridgeSession.from_dict(session.to_dict())

    assert restored.user_habits[0]["id"] == session.user_habits[0]["id"]
    assert restored.history_summary == "最近聊了夜跑。"
    assert restored.last_compress_at == 100.0
    assert restored.compress_round_count == 3
    assert restored.request_snapshot == session.request_snapshot


def test_snapshot_discards_entire_interrupted_tool_turn() -> None:
    """bridge 恢复时不得保留含未闭合工具调用的当前回合。"""
    from src.kernel.llm import LLMPayload, ROLE, Text, ToolCall

    snapshot = import_module("plugins.neo-ndfc-nfc-bridge.snapshot")
    captured = snapshot.capture_payload_snapshot(
        "private-1",
        [
            LLMPayload(ROLE.USER, Text("已完成用户消息")),
            LLMPayload(ROLE.ASSISTANT, Text("已完成助手回复")),
            LLMPayload(ROLE.USER, Text("中断用户消息")),
            LLMPayload(
                ROLE.ASSISTANT,
                ToolCall("call-incomplete", "action-nfc_reply", {"content": "草稿"}),
            ),
        ],
    )

    assert captured is not None
    restored = snapshot.restore_payload_snapshot(captured)

    assert [payload.role for payload in restored] == [ROLE.USER, ROLE.ASSISTANT]
    assert restored[0].content == [Text("已完成用户消息")]
    assert restored[1].content == [Text("已完成助手回复")]


@pytest.mark.asyncio
async def test_request_snapshot_restores_once_and_suppresses_duplicate_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """bridge 冷启动首个 NDFC 请求应恢复快照而非重复默认历史。"""
    from src.core.components.types import EventType
    from src.kernel.llm import LLMPayload, ROLE, Text, ToolCall, ToolResult

    snapshot = import_module("plugins.neo-ndfc-nfc-bridge.snapshot")

    stream_id = "private-1"
    session = state.BridgeSession(user_id="user", stream_id=stream_id)
    session.request_snapshot = snapshot.capture_payload_snapshot(
        stream_id,
        [
            LLMPayload(ROLE.USER, Text("旧消息")),
            LLMPayload(
                ROLE.ASSISTANT,
                ToolCall("call-1", "action-nfc_reply", {"content": "旧回复"}),
            ),
            LLMPayload(ROLE.TOOL_RESULT, ToolResult("已发送", call_id="call-1")),
        ],
    ).to_dict()
    plugin = _plugin(session)
    plugin.config.prompt = SimpleNamespace(request_snapshot_enabled=True)
    chat_stream = SimpleNamespace(stream_id=stream_id, chat_type="private")
    snapshot_handler = handlers.NdfcNfcRequestSnapshotHandler(plugin=plugin)
    bridge_handler = handlers.NdfcNfcBridgeHandler(plugin=plugin)
    snapshot_handler._restored_streams.clear()

    monkeypatch.setattr(
        handlers.stream_api,
        "get_stream",
        lambda _stream_id: _async_value(chat_stream),
    )

    _, history_params = await bridge_handler.execute(
        "neo_default_chatter:build_history_text",
        {"stream_id": stream_id, "chat_stream": chat_stream, "lines": []},
    )
    assert history_params["lines"] == []

    _, first = await snapshot_handler.execute(
        EventType.BEFORE_LLM_REQUEST.value,
        {
            "request_name": "neo_default_chatter",
            "meta_data": {"stream_id": stream_id},
            "payloads": [
                LLMPayload(ROLE.SYSTEM, Text("当前系统")),
                LLMPayload(ROLE.TOOL, []),
                LLMPayload(ROLE.USER, Text("本轮消息")),
            ],
        },
    )
    assert [payload.role for payload in first["payloads"]] == [
        ROLE.SYSTEM,
        ROLE.TOOL,
        ROLE.USER,
        ROLE.ASSISTANT,
        ROLE.TOOL_RESULT,
        ROLE.USER,
    ]

    _, second = await snapshot_handler.execute(
        EventType.BEFORE_LLM_REQUEST.value,
        {
            "request_name": "neo_default_chatter",
            "meta_data": {"stream_id": stream_id},
            "payloads": [LLMPayload(ROLE.USER, Text("续轮"))],
        },
    )
    assert second["payloads"] == [LLMPayload(ROLE.USER, Text("续轮"))]


def test_summary_schedule_requires_round_threshold_and_interval() -> None:
    """bridge 摘要仅在启用、轮数和冷却时间都满足时调度。"""
    summary = import_module("plugins.neo-ndfc-nfc-bridge.summary")
    session = state.BridgeSession(user_id="user", stream_id="private-1")
    config = SimpleNamespace(
        prompt=SimpleNamespace(
            summary_enabled=True,
            compress_every_n_rounds=2,
            min_compress_interval_minutes=30.0,
        )
    )

    session.compress_round_count = 1
    assert summary.BridgeSummaryService.should_schedule(session, config, now=10_000.0) is False

    session.compress_round_count = 2
    session.last_compress_at = 9_000.0
    assert summary.BridgeSummaryService.should_schedule(session, config, now=10_000.0) is False

    session.last_compress_at = 8_000.0
    assert summary.BridgeSummaryService.should_schedule(session, config, now=10_000.0) is True


@pytest.mark.asyncio
async def test_observer_records_reply_under_stable_action_name() -> None:
    """bridge 超时上下文应能读取观察器记录的最近一次实际回复。"""
    session = state.BridgeSession(user_id="user", stream_id="private-1")
    plugin = _plugin(session)
    observer = handlers.NdfcNfcBridgeObserver(plugin=plugin)

    await observer.execute(
        "neo_default_chatter:run_tool_call",
        {
            "stream_id": "private-1",
            "chat_stream": SimpleNamespace(chat_type="private"),
            "calls": [
                SimpleNamespace(
                    name="action-nfc_reply",
                    args={"content": ["上一条回复"]},
                )
            ],
            "results": [(None, True)],
        },
    )

    assert session.mental_log.entries[-1].actions[0]["type"] == "nfc_reply"
    assert session.mental_log.get_last_bot_reply_content() == "上一条回复"


async def _async_value(value: object) -> object:
    """把普通值包装成可等待结果。"""
    return value