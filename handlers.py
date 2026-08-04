"""把 NFC 能力注入 NDFC 事件链的处理器。"""

from __future__ import annotations

import time
from typing import Any, cast

from plugins.neo_default_chatter.utils.event_publisher import NdfcEvent
from src.app.plugin_system.api import prompt_api, stream_api
from src.app.plugin_system.base import BaseEventHandler
from src.kernel.event import EventDecision

from .capabilities import (
    BridgePromptBuilder,
    extract_media_from_messages,
    format_unread_message,
)
from .config import NdfcNfcBridgeConfig
from .state import WaitingConfig


class NdfcNfcBridgeHandler(BaseEventHandler):
    """通过 NDFC 的全部公开事件切面注入桥内 NFC 能力。"""

    name = "ndfc_nfc_bridge"
    description = "把内置的 NFC 消息格式、心理历史、多模态和 Actions 注入 NDFC"
    weight = 100
    timeout: float | None = 0
    init_subscribe = list(NdfcEvent)

    def _config(self) -> NdfcNfcBridgeConfig:
        """返回桥接配置。"""
        return cast(NdfcNfcBridgeConfig, self.plugin.config)

    @staticmethod
    def _chat_type(params: dict[str, Any]) -> str:
        """从事件参数中读取聊天类型。"""
        chat_stream = params.get("chat_stream")
        if chat_stream is not None:
            return str(getattr(chat_stream, "chat_type", "") or "")
        message = params.get("message")
        if message is not None:
            return str(getattr(message, "chat_type", "") or "")
        unreads = params.get("unread_msgs") or params.get("unreads") or []
        if unreads:
            return str(getattr(unreads[-1], "chat_type", "") or "")
        return str(params.get("chat_type") or "")

    async def _is_enabled(self, params: dict[str, Any]) -> bool:
        """判断当前事件是否在桥接作用域内。"""
        config = self._config().bridge
        if not config.enabled:
            return False
        chat_type = self._chat_type(params)
        if not config.private_only:
            return True
        if not chat_type:
            stream_id = str(params.get("stream_id") or "")
            chat_stream = await stream_api.get_stream(stream_id) if stream_id else None
            chat_type = str(getattr(chat_stream, "chat_type", "") or "")
        return chat_type == "private"

    async def _get_session(self, stream_id: str) -> Any:
        """从桥独立 Store 获取当前会话。"""
        session_store = self.plugin.session_store
        async with session_store.lock(stream_id):
            return await session_store.get_or_create(stream_id)

    async def execute(
        self, event_name: str, params: dict[str, Any]
    ) -> tuple[EventDecision, dict[str, Any]]:
        """按 NDFC 事件名分派桥接行为。

        Args:
            event_name: NDFC 发布的事件名。
            params: 事件的稳定键集合。

        Returns:
            事件传播决策和原键集合参数。
        """
        if not await self._is_enabled(params):
            return EventDecision.PASS, params

        event = str(event_name)
        config = self._config().bridge

        if event == NdfcEvent.FORMAT_UNREAD_LINE and config.use_nfc_message_format:
            params["formatted_line"] = format_unread_message(
                params["message"], params.get("time_format") or "%H:%M"
            )
            return EventDecision.STOP, params

        if event == NdfcEvent.BUILD_HISTORY_TEXT and config.use_nfc_history:
            session = await self._get_session(params["stream_id"])
            narrative = BridgePromptBuilder().build_fused_narrative(
                params["chat_stream"],
                session,
            )
            params["lines"] = narrative.splitlines() if narrative else []
            return EventDecision.STOP, params

        if event == NdfcEvent.INJECT_UNREAD_PAYLOAD and config.use_nfc_multimodal:
            session = await self._get_session(params["stream_id"])
            bridge_config = self._config()
            max_images = int(bridge_config.model.max_images_per_payload)
            media_items = extract_media_from_messages(
                params.get("unread_msgs") or [], max_items=max_images
            )
            payload, extra_payload = await BridgePromptBuilder().build_user_payload(
                params.get("formatted_text") or "",
                media_items=media_items,
                stream_id=params["stream_id"],
                session=session,
                config=bridge_config,
            )
            params["response"].add_payload(payload)
            if extra_payload is not None:
                params["response"].add_payload(extra_payload)
            params["skip"] = True
            return EventDecision.STOP, params

        if event == NdfcEvent.INJECT_USABLES and config.expose_nfc_actions:
            return EventDecision.PASS, params

        if event == NdfcEvent.CREATE_REQUEST:
            params["task_name"] = self._config().model.model_task
            return EventDecision.SUCCESS, params

        if (
            event == NdfcEvent.BUILD_RESUME_PROMPT
            and params.get("source") == "timer"
            and config.use_nfc_waiting
        ):
            await self._build_timeout_resume(params)
            return EventDecision.STOP, params

        if event == NdfcEvent.COMPUTE_COOLDOWN and config.use_nfc_waiting:
            session = await self._get_session(params["stream_id"])
            raw_seconds = float(params.get("minutes") or 0.0) * 60
            cooldown_seconds = self._config().wait.apply_rules(
                raw_seconds,
                session.consecutive_timeout_count,
            )
            params["cooldown_seconds"] = int(cooldown_seconds)
            return EventDecision.STOP, params

        if event == NdfcEvent.COMPUTE_STOP_WAKE and config.use_nfc_waiting:
            session = await self._get_session(params["stream_id"])
            waiting = session.waiting_config
            if (
                self._config().wait.suppress_early_wake
                and waiting.is_active()
                and not waiting.is_timeout()
            ):
                params["probability"] = 0.0
                return EventDecision.STOP, params

        if event == NdfcEvent.PREPROCESS:
            if config.persist_mental_state:
                await self._record_unreads(params)
            await self._inject_nfc_context(params)
            return EventDecision.SUCCESS, params

        if event == NdfcEvent.SESSION_TRANSITION and config.persist_mental_state:
            session_store = self.plugin.session_store
            async with session_store.lock(params["stream_id"]):
                session = await session_store.get_or_create(params["stream_id"])
                session.last_activity_at = time.time()
                await session_store.save(session)
            return EventDecision.PASS, params

        return EventDecision.PASS, params

    async def _inject_nfc_context(self, params: dict[str, Any]) -> None:
        """把桥内 system prompt 和动态会话状态注入 NDFC 本轮上下文。"""
        config = self._config().bridge
        if not config.use_nfc_system_prompt:
            return

        session = await self._get_session(params["stream_id"])
        chat_stream = params["chat_stream"]
        system_prompt = await BridgePromptBuilder().build_system_prompt(
            chat_stream,
            session,
            self._config(),
        )
        if system_prompt:
            prompt_api.add_stream_reminder(
                stream_id=params["stream_id"],
                bucket="actor",
                name="neo_ndfc_nfc_bridge.system_behavior",
                content=system_prompt,
            )

        dynamic_parts: list[str] = []
        if session.history_summary:
            dynamic_parts.append(f"【近期记忆】\n{session.history_summary}")
        if session.scheduled_proactive_at is not None:
            schedule_time = time.strftime(
                "%Y-%m-%d %H:%M",
                time.localtime(session.scheduled_proactive_at),
            )
            dynamic_parts.append(
                f"【主动发起预约】{schedule_time}："
                f"{session.scheduled_proactive_reason or '未说明理由'}"
            )
        if dynamic_parts:
            existing = str(params.get("mutations") or "").strip()
            bridge_context = "\n\n".join(dynamic_parts)
            params["mutations"] = (
                f"{existing}\n\n{bridge_context}" if existing else bridge_context
            )

    async def _build_timeout_resume(self, params: dict[str, Any]) -> None:
        """构造 NFC 风格的等待超时心理提示并更新超时计数。"""
        session_store = self.plugin.session_store
        stream_id = params["stream_id"]
        async with session_store.lock(stream_id):
            session = await session_store.get_or_create(stream_id)
            session.consecutive_timeout_count += 1
            elapsed = session.waiting_config.get_elapsed_seconds()
            resume_event = params.get("resume_event")
            wait_time = getattr(resume_event, "wait_time", None)
            if isinstance(wait_time, (int, float)):
                elapsed = float(wait_time)
            timeout_payload = BridgePromptBuilder.build_timeout_payload(
                elapsed_seconds=elapsed,
                expected_reaction=session.waiting_config.expected_reaction,
                consecutive_timeouts=session.consecutive_timeout_count,
                last_bot_message=session.mental_log.get_last_bot_reply_content(),
                max_consecutive_timeouts=self._config().wait.max_consecutive_timeouts,
            )
            params["prompt"] = str(timeout_payload.content[0].text)
            await session_store.save(session)

    async def _record_unreads(self, params: dict[str, Any]) -> None:
        """把 NDFC 收到的消息同步到 NFC 心理活动流。"""
        unreads = params.get("unreads") or []
        if not unreads:
            return
        session_store = self.plugin.session_store
        stream_id = params["stream_id"]
        async with session_store.lock(stream_id):
            session = await session_store.get_or_create(stream_id)
            for message in unreads:
                session.add_user_message(
                    content=getattr(message, "processed_plain_text", "")
                    or str(getattr(message, "content", "")),
                    user_name=getattr(message, "sender_name", "") or "未知用户",
                    user_id=getattr(message, "sender_id", "") or "",
                    timestamp=getattr(message, "time", None),
                    message_id=getattr(message, "message_id", "") or "",
                )
            chat_stream = params.get("chat_stream")
            if chat_stream is not None:
                session.platform = getattr(chat_stream, "platform", "") or ""
                session.user_id = (
                    getattr(unreads[-1], "sender_id", "") or session.user_id
                )
            await session_store.save(session)


class NdfcNfcBridgeObserver(BaseEventHandler):
    """在 NDFC 默认工具执行后持久化 NFC 心理与等待状态。"""

    name = "ndfc_nfc_bridge_observer"
    description = "记录 NDFC 工具调用产生的 NFC 心理、情绪和等待状态"
    weight = -100
    timeout: float | None = 0
    init_subscribe = [NdfcEvent.RUN_TOOL_CALL]

    async def _is_enabled(self, params: dict[str, Any]) -> bool:
        """判断后置状态同步是否在桥接作用域内。"""
        config = cast(NdfcNfcBridgeConfig, self.plugin.config).bridge
        if not config.enabled or not config.persist_mental_state:
            return False
        if not config.private_only:
            return True
        chat_type = NdfcNfcBridgeHandler._chat_type(params)
        if not chat_type:
            stream_id = str(params.get("stream_id") or "")
            chat_stream = await stream_api.get_stream(stream_id) if stream_id else None
            chat_type = str(getattr(chat_stream, "chat_type", "") or "")
        return chat_type == "private"

    async def execute(
        self, event_name: str, params: dict[str, Any]
    ) -> tuple[EventDecision, dict[str, Any]]:
        """同步成功的 nfc_reply/do_nothing 调用。

        Args:
            event_name: NDFC 工具执行事件名。
            params: 包含 calls 和默认执行结果的事件参数。

        Returns:
            PASS，保证观察器不覆盖默认工具执行结果。
        """
        config = cast(NdfcNfcBridgeConfig, self.plugin.config)
        if not await self._is_enabled(params):
            return EventDecision.PASS, params

        calls = params.get("calls") or []
        results = params.get("results") or []
        successful_calls = [
            call
            for call, result in zip(calls, results, strict=False)
            if result
            and len(result) > 1
            and bool(result[1])
            and str(getattr(call, "name", ""))
            in {"action-nfc_reply", "action-do_nothing"}
        ]
        if not successful_calls:
            return EventDecision.PASS, params

        session_store = self.plugin.session_store
        stream_id = params["stream_id"]
        async with session_store.lock(stream_id):
            session = await session_store.get_or_create(stream_id)
            for call in successful_calls:
                args = dict(call.args) if isinstance(call.args, dict) else {}
                wait_seconds = 0.0
                if config.bridge.use_nfc_waiting:
                    wait_seconds = config.wait.apply_rules(
                        float(args.get("max_wait_seconds") or 0.0),
                        session.consecutive_timeout_count,
                    )
                session.add_bot_planning(
                    thought=str(args.get("thought") or ""),
                    actions=[{"type": call.name, **args}],
                    expected_reaction=str(args.get("expected_reaction") or ""),
                    max_wait_seconds=wait_seconds,
                )
                mood = str(args.get("mood") or "").strip()
                if mood:
                    session.record_mood(mood)
                if config.bridge.use_nfc_waiting:
                    if wait_seconds > 0:
                        session.set_waiting(
                            WaitingConfig(
                                expected_reaction=str(
                                    args.get("expected_reaction") or ""
                                ),
                                max_wait_seconds=wait_seconds,
                                started_at=time.time(),
                            )
                        )
                    else:
                        session.clear_waiting()
            await session_store.save(session)
        return EventDecision.PASS, params


__all__ = ["NdfcNfcBridgeHandler", "NdfcNfcBridgeObserver"]
