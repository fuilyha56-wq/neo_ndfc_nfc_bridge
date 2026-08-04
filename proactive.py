"""桥内主动发起调度与 NDFC 流唤醒。"""

from __future__ import annotations

from enum import Enum

import asyncio
import datetime
import random
import time
from typing import TYPE_CHECKING, Any

from src.app.plugin_system.api import service_api, stream_api
from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.base import Failure, WaitResumeEvent
from src.kernel.concurrency import get_task_manager

from .state import MentalLogEntry, NFCEventType

if TYPE_CHECKING:
    from .plugin import NdfcNfcBridgePlugin

logger = get_logger("neo_ndfc_nfc_bridge.proactive")

_NDFC_SERVICE_SIGNATURE = "neo_default_chatter:service:chat_core"


class _WakeOutcome(Enum):
    """一次主动唤醒尝试的处理结果。"""

    WOKEN = "woken"
    SKIPPED = "skipped"
    RETRY = "retry"


class ProactiveScheduler:
    """扫描桥会话并通过 WaitResumeEvent 唤醒 NDFC。"""

    def __init__(self, plugin: NdfcNfcBridgePlugin) -> None:
        """保存插件引用和后台任务状态。"""
        self._plugin = plugin
        self._task_id: str | None = None
        self._pending_streams: set[str] = set()

    def start(self) -> None:
        """通过项目 TaskManager 启动守护循环。"""
        if self._task_id is not None or not self._plugin.config.proactive.enabled:
            return
        task_info = get_task_manager().create_task(
            self._run(),
            name="neo_ndfc_nfc_bridge_proactive",
            daemon=True,
        )
        self._task_id = task_info.task_id

    async def stop(self) -> None:
        """取消并等待主动发起守护循环退出。"""
        if self._task_id is None:
            return
        task_id = self._task_id
        self._task_id = None
        task_manager = get_task_manager()
        try:
            task_info = task_manager.get_task(task_id)
        except Exception:
            return
        task_manager.cancel_task(task_id)
        if task_info.task is not None:
            try:
                await task_info.task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        """按配置间隔持续检查预约与沉默触发。"""
        while True:
            try:
                await self._check_sessions()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("主动发起检查失败")
            await asyncio.sleep(
                max(5, int(self._plugin.config.proactive.check_interval))
            )

    async def _check_sessions(self) -> None:
        """检查所有持久化会话并唤醒命中的聊天流。"""
        now = time.time()
        for stream_id in await self._plugin.session_store.list_all_stream_ids():
            if stream_id in self._pending_streams:
                continue
            session = await self._plugin.session_store.peek(stream_id)
            if session is None:
                continue

            scheduled = session.scheduled_proactive_at
            is_scheduled = scheduled is not None
            if is_scheduled:
                if scheduled is None or scheduled > now:
                    continue
                reason = session.scheduled_proactive_reason
            else:
                config = self._plugin.config.proactive
                baseline = session.last_user_message_at or session.last_activity_at
                silence_seconds = max(0.0, now - baseline)
                if silence_seconds < max(0, int(config.silence_threshold)):
                    continue
                if (
                    session.last_proactive_at is not None
                    and now - session.last_proactive_at
                    < max(0, int(config.min_interval))
                ):
                    continue
                if self._is_quiet_time(
                    str(config.quiet_hours_start),
                    str(config.quiet_hours_end),
                ):
                    continue
                if session.activity_hours:
                    current_hour = str(time.localtime(now).tm_hour)
                    average = sum(session.activity_hours.values()) / 24
                    if session.activity_hours.get(current_hour, 0) < average * 0.5:
                        continue
                if random.random() > max(
                    0.0,
                    min(float(config.trigger_probability), 1.0),
                ):
                    continue
                reason = "沉默一段时间后，自然地想起了对方"

            self._pending_streams.add(stream_id)
            try:
                prompt = self._build_resume_prompt(session, reason, is_scheduled, now)
                outcome = await self._wake_ndfc(stream_id, prompt)
                if outcome is _WakeOutcome.RETRY:
                    logger.warning(f"主动发起唤醒失败: stream={stream_id[:8]}")
                    continue
                if outcome is _WakeOutcome.SKIPPED:
                    if is_scheduled:
                        async with self._plugin.session_store.lock(stream_id):
                            current = await self._plugin.session_store.get_or_create(
                                stream_id
                            )
                            current.set_scheduled_proactive(None)
                            await self._plugin.session_store.save(current)
                    continue
                async with self._plugin.session_store.lock(stream_id):
                    current = await self._plugin.session_store.get_or_create(stream_id)
                    if is_scheduled:
                        current.set_scheduled_proactive(None)
                    current.last_proactive_at = now
                    current.last_activity_at = now
                    current.mental_log.add(
                        MentalLogEntry(
                            event_type=NFCEventType.PROACTIVE_TRIGGER,
                            timestamp=now,
                            content=reason or "主动想起了对方",
                        )
                    )
                    await self._plugin.session_store.save(current)
                logger.info(f"已唤醒 NDFC 主动决策: stream={stream_id[:8]}")
            finally:
                self._pending_streams.discard(stream_id)

    async def _wake_ndfc(self, stream_id: str, prompt: str) -> _WakeOutcome:
        """通过 NDFC Service 独立驱动主动决策，不改动流上的 Chatter 绑定。"""
        chat_stream = await stream_api.activate_stream(stream_id)
        if chat_stream is None:
            logger.warning(f"主动发起目标流不存在: stream={stream_id[:8]}")
            return _WakeOutcome.RETRY
        if self._plugin.config.bridge.private_only:
            chat_type = str(getattr(chat_stream, "chat_type", "") or "")
            if chat_type != "private":
                logger.info(
                    f"按 private_only 跳过非私聊主动发起: "
                    f"stream={stream_id[:8]} chat_type={chat_type or 'unknown'}"
                )
                return _WakeOutcome.SKIPPED

        context = chat_stream.context
        if (
            context.unread_messages
            or context.message_cache
            or context.is_chatter_processing
        ):
            logger.debug(f"目标流正在处理消息，延后主动发起: stream={stream_id[:8]}")
            return _WakeOutcome.RETRY

        service = service_api.get_service(_NDFC_SERVICE_SIGNATURE)
        create_session = getattr(service, "create_session", None)
        if not callable(create_session):
            logger.warning("NDFC chat_core Service 不可用，无法主动发起")
            return _WakeOutcome.RETRY

        session = create_session(
            stream_id=stream_id,
            plugin=self._plugin.ndfc_plugin,
        )
        runner = session.execute()
        try:
            initial_result = await anext(runner)
            if isinstance(initial_result, Failure):
                logger.warning(
                    f"NDFC 主动会话初始化失败: stream={stream_id[:8]} "
                    f"error={initial_result.error}"
                )
                return _WakeOutcome.RETRY
            result = await runner.asend(
                WaitResumeEvent(
                    source="neo_ndfc_nfc_bridge.proactive",
                    extra={"resume_prompt": prompt},
                )
            )
            if isinstance(result, Failure):
                logger.warning(
                    f"NDFC 主动决策失败: stream={stream_id[:8]} error={result.error}"
                )
                return _WakeOutcome.RETRY
            return _WakeOutcome.WOKEN
        except StopAsyncIteration:
            logger.warning(f"NDFC 主动会话提前结束: stream={stream_id[:8]}")
            return _WakeOutcome.RETRY
        except Exception:
            logger.exception(f"NDFC 主动会话执行异常: stream={stream_id[:8]}")
            return _WakeOutcome.RETRY
        finally:
            await runner.aclose()

    @staticmethod
    def _build_resume_prompt(
        session: Any,
        reason: str,
        is_scheduled: bool,
        now: float,
    ) -> str:
        """构建主动发起时交给 NDFC 的内部决策上下文。"""
        baseline = session.last_user_message_at or session.last_activity_at
        silence_minutes = max(0.0, now - baseline) / 60
        mood = session.get_dominant_mood() or "未记录"
        activity = session.mental_log.format_as_summary(8) or "（无近期活动）"
        trigger_type = (
            "先前预约的时间已经到了" if is_scheduled else "已经沉默了一段时间"
        )
        return f"""系统内部事件：{trigger_type}，现在允许你主动想起并联系对方。
触发理由：{reason or "没有特定事项，只是自然地想起了对方"}
距离对方上次出现约 {silence_minutes:.0f} 分钟。
你近期的主导情绪：{mood}

近期心理活动：
{activity}

请基于你们的关系和上下文诚实决定：
- 真正有话想说时，调用 nfc_reply 自然地发起对话，并写明 thought、expected_reaction、max_wait_seconds 和 mood。
- 现在联系会显得生硬、打扰或没有真实内容时，调用 do_nothing(max_wait_seconds=0)。
- 不要提到调度器、预约系统、沉默阈值或这段内部事件。
响应只能包含工具调用。"""

    @staticmethod
    def _is_quiet_time(start: str, end: str) -> bool:
        """判断当前本地时间是否落在跨日勿扰区间。"""
        try:
            start_time = datetime.time.fromisoformat(start)
            end_time = datetime.time.fromisoformat(end)
        except ValueError:
            logger.warning(f"勿扰时间格式无效: {start!r} - {end!r}")
            return False
        current = datetime.datetime.now().time()
        if start_time <= end_time:
            return start_time <= current < end_time
        return current >= start_time or current < end_time


__all__ = ["ProactiveScheduler"]
