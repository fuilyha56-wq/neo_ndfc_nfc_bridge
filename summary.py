"""NDFC-NFC bridge 的近期记忆压缩服务。"""

from __future__ import annotations

import datetime
import time
from typing import TYPE_CHECKING, Any

from src.app.plugin_system.api import llm_api, stream_api
from src.app.plugin_system.api.log_api import get_logger
from src.kernel.concurrency import get_task_manager
from src.kernel.llm import LLMPayload, ROLE, Text

from .capabilities import BridgePromptBuilder
from .state import NFCEventType

if TYPE_CHECKING:
    from .plugin import NdfcNfcBridgePlugin

logger = get_logger("neo_ndfc_nfc_bridge.summary")


class BridgeSummaryService:
    """按回合阈值异步生成并保存 bridge 的近期记忆摘要。"""

    @staticmethod
    def should_schedule(session: Any, config: Any, *, now: float | None = None) -> bool:
        """判断当前会话是否达到摘要压缩条件。"""
        prompt_config = config.prompt
        if not bool(getattr(prompt_config, "summary_enabled", True)):
            return False
        every_n = max(1, int(getattr(prompt_config, "compress_every_n_rounds", 50)))
        if int(getattr(session, "compress_round_count", 0)) < every_n:
            return False
        current_time = time.time() if now is None else now
        min_interval = max(
            0.0,
            float(getattr(prompt_config, "min_compress_interval_minutes", 120.0)),
        ) * 60
        return current_time - float(getattr(session, "last_compress_at", 0.0)) >= min_interval

    @classmethod
    def schedule(cls, plugin: "NdfcNfcBridgePlugin", stream_id: str) -> None:
        """把指定私聊的摘要任务交由项目 TaskManager 执行。"""
        get_task_manager().create_task(
            cls.compress(plugin, stream_id),
            name=f"neo_ndfc_nfc_bridge_summary_{stream_id}",
            daemon=True,
        )

    @classmethod
    async def compress(
        cls,
        plugin: "NdfcNfcBridgePlugin",
        stream_id: str,
    ) -> None:
        """读取近期消息并以 bridge 模型生成替换式摘要。"""
        config = plugin.config
        if not bool(config.prompt.summary_enabled):
            return

        session_store = plugin.session_store
        async with session_store.lock(stream_id):
            session = await session_store.get_or_create(stream_id)
            if not cls.should_schedule(session, config):
                return
            # 预先写入时间戳，避免同一流并发重复调度压缩任务。
            session.last_compress_at = time.time()
            await session_store.save(session)

        chat_stream = await stream_api.activate_stream(stream_id)
        if chat_stream is None:
            logger.warning(f"无法激活摘要目标流: stream={stream_id[:8]}")
            return

        days = max(0.1, float(config.prompt.compress_days_window))
        since_ts = time.time() - days * 86400
        try:
            messages = await stream_api.get_stream_messages(stream_id, limit=10_000)
        except Exception as exc:
            logger.warning(f"读取摘要消息失败: stream={stream_id[:8]} error={exc}")
            return

        lines = cls._format_history(messages, chat_stream, session, since_ts)
        if not lines:
            logger.debug(f"摘要跳过：近期无可用消息 stream={stream_id[:8]}")
            return

        summary_prompt = cls._build_summary_prompt(lines, days)
        try:
            model_set = llm_api.get_model_set_by_task(config.model.model_task)
            request = llm_api.create_llm_request(
                model_set,
                request_name="neo_ndfc_nfc_bridge.summary",
                stream_id=stream_id,
            )
            system_prompt = await BridgePromptBuilder().build_system_prompt(
                chat_stream,
                session,
                config,
            )
            if system_prompt:
                request.add_payload(LLMPayload(ROLE.SYSTEM, Text(system_prompt)))
            request.add_payload(LLMPayload(ROLE.USER, Text(summary_prompt)))
            response = await request.send(stream=False)
            summary = (await response).strip()
        except Exception as exc:
            logger.warning(f"近期记忆压缩失败: stream={stream_id[:8]} error={exc}")
            return

        if not summary:
            logger.warning(f"近期记忆压缩返回空内容: stream={stream_id[:8]}")
            return

        async with session_store.lock(stream_id):
            latest = await session_store.get_or_create(stream_id)
            latest.history_summary = summary
            latest.last_compress_at = time.time()
            latest.compress_round_count = 0
            await session_store.save(latest)
        logger.info(
            f"近期记忆压缩完成: stream={stream_id[:8]} "
            f"messages={len(lines)} chars={len(summary)}"
        )

    @staticmethod
    def _format_history(
        messages: list[Any],
        chat_stream: Any,
        session: Any,
        since_ts: float,
    ) -> list[str]:
        """格式化窗口内的对话与心理活动，供摘要模型读取。"""
        bot_id = str(getattr(chat_stream, "bot_id", "") or "")
        timeline: list[tuple[float, str]] = []
        for message in messages:
            timestamp = getattr(message, "time", None)
            if not isinstance(timestamp, (int, float)) or timestamp < since_ts:
                continue
            text = str(getattr(message, "processed_plain_text", "") or "").strip()
            if not text:
                continue
            time_text = datetime.datetime.fromtimestamp(timestamp).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            sender_id = str(getattr(message, "sender_id", "") or "")
            sender_name = str(getattr(message, "sender_name", "") or "用户")
            prefix = "你回复" if bot_id and sender_id == bot_id else f"{sender_name}说"
            timeline.append((float(timestamp), f"[{time_text}] {prefix}：{text}"))

        for entry in session.mental_log.entries:
            if entry.timestamp < since_ts or entry.event_type != NFCEventType.BOT_PLANNING:
                continue
            if not entry.thought.strip():
                continue
            time_text = datetime.datetime.fromtimestamp(entry.timestamp).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            timeline.append((entry.timestamp, f"[{time_text}] （你的内心：{entry.thought}）"))

        timeline.sort(key=lambda item: item[0])
        return [line for _, line in timeline[-5_000:]]

    @staticmethod
    def _build_summary_prompt(lines: list[str], days: float) -> str:
        """构建近期记忆摘要的用户指令。"""
        return (
            f"以下是最近 {days:.1f} 天的私聊记录：\n\n"
            f"{chr(10).join(lines)}\n\n"
            "请以第一人称写一段 800 到 1200 字的近期记忆摘要。"
            "保留重要关系变化、情感节点、事实、承诺、偏好和未完成事项；"
            "不要捏造、不输出 JSON、不提及系统或压缩过程。"
        )


__all__ = ["BridgeSummaryService"]