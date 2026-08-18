"""主动发起调度的多 Chatter 共存回归测试。"""

from __future__ import annotations

from contextlib import asynccontextmanager
from importlib import import_module
from types import SimpleNamespace
from typing import AsyncIterator

import pytest

from src.app.plugin_system.base import Wait

actions = import_module("plugins.neo-ndfc-nfc-bridge.actions")
proactive = import_module("plugins.neo-ndfc-nfc-bridge.proactive")
state = import_module("plugins.neo-ndfc-nfc-bridge.state")


class _SessionStore:
    """提供调度测试所需的最小会话存储。"""

    def __init__(self, session: object) -> None:
        self.session = session
        self.saved = 0

    async def list_all_stream_ids(self) -> list[str]:
        """返回唯一测试流。"""
        return [self.session.stream_id]

    async def peek(self, stream_id: str) -> object:
        """读取测试会话。"""
        assert stream_id == self.session.stream_id
        return self.session

    async def get_or_create(self, stream_id: str) -> object:
        """返回测试会话。"""
        assert stream_id == self.session.stream_id
        return self.session

    async def save(self, session: object) -> None:
        """记录保存次数。"""
        assert session is self.session
        self.saved += 1

    @asynccontextmanager
    async def lock(self, stream_id: str) -> AsyncIterator[None]:
        """提供无竞争的测试锁。"""
        assert stream_id == self.session.stream_id
        yield


def _plugin(session: object, *, private_only: bool = True) -> SimpleNamespace:
    """构造调度器需要的插件替身。"""
    return SimpleNamespace(
        config=SimpleNamespace(
            bridge=SimpleNamespace(enabled=True, private_only=private_only),
            proactive=SimpleNamespace(
                enabled=True,
                check_interval=60,
                silence_threshold=7200,
                min_interval=1800,
                quiet_hours_start="23:00",
                quiet_hours_end="07:00",
                trigger_probability=0.3,
            ),
        ),
        ndfc_plugin=object(),
        session_store=_SessionStore(session),
    )


@pytest.mark.asyncio
async def test_wake_uses_ndfc_service_without_chatter_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """主动决策应走独立 Service 会话，不读取或覆盖同流 Chatter。"""
    stream_id = "stream-private"
    received_events: list[object] = []

    async def session_runner():
        resume_event = yield Wait()
        received_events.append(resume_event)
        yield Wait()

    service = SimpleNamespace(
        create_session=lambda **_kwargs: SimpleNamespace(execute=session_runner)
    )
    chat_stream = SimpleNamespace(
        chat_type="private",
        context=SimpleNamespace(
            unread_messages=[],
            message_cache=[],
            is_chatter_processing=False,
        ),
    )
    session = state.BridgeSession(user_id="user", stream_id=stream_id)
    plugin = _plugin(session)

    monkeypatch.setattr(
        proactive.stream_api,
        "activate_stream",
        lambda _stream_id: _async_value(chat_stream),
    )
    monkeypatch.setattr(
        proactive.service_api, "get_service", lambda _signature: service
    )

    outcome = await proactive.ProactiveScheduler(plugin)._wake_ndfc(
        stream_id,
        "主动提示",
    )

    assert outcome is proactive._WakeOutcome.WOKEN
    assert len(received_events) == 1
    assert received_events[0].source == "neo_ndfc_nfc_bridge.proactive"
    assert received_events[0].extra == {"resume_prompt": "主动提示"}


@pytest.mark.asyncio
async def test_scheduled_group_skip_consumes_obsolete_appointment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """private_only 下的旧群聊预约应一次性清理，不再反复告警。"""
    session = state.BridgeSession(user_id="user", stream_id="stream-group")
    session.set_scheduled_proactive(1.0, "旧群聊预约")
    plugin = _plugin(session)
    scheduler = proactive.ProactiveScheduler(plugin)

    async def skip_wake(
        _stream_id: str,
        _prompt: str,
        recipient_user_id: str = "",
    ):
        del recipient_user_id
        return proactive._WakeOutcome.SKIPPED

    monkeypatch.setattr(scheduler, "_wake_ndfc", skip_wake)

    await scheduler._check_sessions()

    assert session.scheduled_proactive_at is None
    assert plugin.session_store.saved == 1


@pytest.mark.asyncio
async def test_retry_keeps_scheduled_appointment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """服务临时不可用时应保留预约供下一轮重试。"""
    session = state.BridgeSession(user_id="user", stream_id="stream-retry")
    session.set_scheduled_proactive(1.0, "稍后重试")
    plugin = _plugin(session)
    scheduler = proactive.ProactiveScheduler(plugin)

    async def retry_wake(
        _stream_id: str,
        _prompt: str,
        recipient_user_id: str = "",
    ):
        del recipient_user_id
        return proactive._WakeOutcome.RETRY

    monkeypatch.setattr(scheduler, "_wake_ndfc", retry_wake)

    await scheduler._check_sessions()

    assert session.scheduled_proactive_at == 1.0
    assert plugin.session_store.saved == 0


@pytest.mark.asyncio
async def test_private_whitelist_change_blocks_existing_proactive_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OneBot 白名单更新后，已有私聊会话不得继续被主动唤醒。"""
    stream_id = "stream-private"
    session = state.BridgeSession(user_id="blocked-user", stream_id=stream_id)
    plugin = _plugin(session)
    chat_stream = SimpleNamespace(
        platform="qq",
        chat_type="private",
        context=SimpleNamespace(
            unread_messages=[],
            message_cache=[],
            is_chatter_processing=False,
        ),
    )
    monkeypatch.setattr(
        proactive.stream_api,
        "activate_stream",
        lambda _stream_id: _async_value(chat_stream),
    )
    monkeypatch.setattr(
        proactive.stream_api,
        "get_stream_info",
        lambda _stream_id: _async_value({"chat_type": "private"}),
    )
    monkeypatch.setattr(
        proactive.config_api,
        "get_config",
        lambda _plugin_name: SimpleNamespace(
            features=SimpleNamespace(
                private_list_type="whitelist",
                private_list=["allowed-user"],
                group_list_type="blacklist",
                group_list=[],
                ban_user_id=[],
            )
        ),
    )
    monkeypatch.setattr(
        proactive.service_api,
        "get_service",
        lambda _signature: pytest.fail("白名单外流不应创建 NDFC 会话"),
    )

    outcome = await proactive.ProactiveScheduler(plugin)._wake_ndfc(
        stream_id,
        "主动提示",
        recipient_user_id=session.user_id,
    )

    assert outcome is proactive._WakeOutcome.SKIPPED


@pytest.mark.asyncio
async def test_schedule_action_rejects_group_when_private_only() -> None:
    """private_only 下群聊不暴露桥接 Action，也不能创建主动预约。"""
    plugin = SimpleNamespace(
        config=SimpleNamespace(
            bridge=SimpleNamespace(enabled=True, private_only=True),
            proactive=SimpleNamespace(enabled=True),
        )
    )
    chat_stream = SimpleNamespace(stream_id="stream-group", chat_type="group")
    reply_action = actions.NDFCReplyAction(
        chat_stream=chat_stream,
        plugin=plugin,
    )
    schedule_action = actions.NDFCScheduleProactiveAction(
        chat_stream=chat_stream,
        plugin=plugin,
    )

    assert await reply_action.go_activate() is False
    assert await schedule_action.go_activate() is False
    success, message = await schedule_action.execute(delay_minutes=30, reason="测试")

    assert success is False
    assert message == "当前配置仅允许在私聊中预约主动发起"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action_class", "kwargs", "expected_message"),
    [
        (actions.NDFCReplyAction, {"content": "不应发送"}, "当前配置仅允许在私聊中执行桥接 Action"),
        (actions.NDFCDoNothingAction, {}, "当前配置仅允许在私聊中执行桥接 Action"),
        (actions.NDFCUpdateMoodStateAction, {"mood": "平静"}, "当前配置仅允许在私聊中执行桥接 Action"),
        (actions.NDFCRecordUserHabitAction, {"habit_text": "早睡"}, "当前配置仅允许在私聊中执行桥接 Action"),
        (actions.NDFCQueryActivityPatternAction, {}, "当前配置仅允许在私聊中执行桥接 Action"),
        (actions.NDFCScheduleProactiveAction, {}, "当前配置仅允许在私聊中预约主动发起"),
    ],
)
async def test_direct_action_execution_rejects_group_when_private_only(
    action_class: type,
    kwargs: dict[str, object],
    expected_message: str,
) -> None:
    """绕过 go_activate 直接执行时也不得让桥接 Action 作用于群聊。"""
    plugin = SimpleNamespace(
        config=SimpleNamespace(
            bridge=SimpleNamespace(enabled=True, private_only=True),
            proactive=SimpleNamespace(enabled=True),
        )
    )
    chat_stream = SimpleNamespace(stream_id="stream-group", chat_type="group")
    action = action_class(chat_stream=chat_stream, plugin=plugin)

    success, message = await action.execute(**kwargs)

    assert success is False
    assert message == expected_message


async def _async_value(value: object) -> object:
    """把普通值包装为可等待结果。"""
    return value
