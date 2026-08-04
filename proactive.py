"""桥内主动发起调度与 NDFC 流唤醒。"""

from __future__ import annotations

import asyncio
import datetime
import random
import time
from typing import TYPE_CHECKING, Any

from src.app.plugin_system.api import chat_api, plugin_api, stream_api
from src.app.plugin_system.api.log_api import get_logger
from src.core.managers.chatter_manager import get_chatter_manager
from src.kernel.concurrency import get_task_manager

from .state import MentalLogEntry, NFCEventType

if TYPE_CHECKING:
    from .plugin import NdfcNfcBridgePlugin

logger = get_logger("neo_ndfc_nfc_bridge.proactive")

_NDFC_SIGNATURE = "neo_default_chatter:chatter:neo_default_chatter"


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
                if not await self._wake_ndfc(stream_id, prompt):
                    logger.warning(f"主动发起唤醒失败: stream={stream_id[:8]}")
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

    async def _wake_ndfc(self, stream_id: str, prompt: str) -> bool:
        """激活目标流，确保绑定 NDFC 后注入恢复事件。"""
        chat_stream = await stream_api.activate_stream(stream_id)
        if chat_stream is None:
            return False
        if self._plugin.config.bridge.private_only:
            chat_type = str(getattr(chat_stream, "chat_type", "") or "")
            if chat_type != "private":
                return False

        chatter = chat_api.get_chatter_by_stream(stream_id)
        if chatter is None:
            chatter_class = chat_api.get_chatter_class(_NDFC_SIGNATURE)
            owner = plugin_api.get_plugin("neo_default_chatter")
            if chatter_class is None or owner is None:
                return False
            chatter = chatter_class(stream_id=stream_id, plugin=owner)
            chat_api.bind_chatter_for_stream(stream_id, chatter)
        elif chatter.get_signature() != _NDFC_SIGNATURE:
            logger.debug(
                f"跳过已绑定其他 Chatter 的流: stream={stream_id[:8]} "
                f"chatter={chatter.get_signature()}"
            )
            return False

        return await get_chatter_manager().resume_chatter(
            stream_id,
            source="neo_ndfc_nfc_bridge.proactive",
            extra={"resume_prompt": prompt},
        )

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
