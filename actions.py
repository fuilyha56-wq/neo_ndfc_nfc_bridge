"""向 NDFC 暴露的桥内置 NFC 风格 Action。"""

from __future__ import annotations

import asyncio
import json
import random
import re
import time
from typing import Annotated, Any

from src.app.plugin_system.api import send_api
from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.base import BaseAction

logger = get_logger("neo_ndfc_nfc_bridge.actions")

_THINKING_BLOCK = re.compile(
    r"<(?:think|thinking|analysis)>.*?</(?:think|thinking|analysis)>",
    re.DOTALL | re.IGNORECASE,
)
_METADATA_PATTERNS = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"(?:想法|内心想法|思考|thought|thinking)\s*[:：]",
        r"(?:预计反应|预期反应|expected_reaction)\s*[:：]",
        r"(?:最大等待秒数|max_wait_seconds)\s*[:：]",
        r"(?:心情|情绪|mood)\s*[:：]",
    )
)


class _NDFCBridgeAction(BaseAction):
    """仅在桥接配置允许的聊天流中激活。"""

    def _execution_denial(self, operation: str = "执行桥接 Action") -> str | None:
        """返回当前聊天流不能执行桥接 Action 时的说明。"""
        bridge_config = self.plugin.config.bridge
        if not bridge_config.enabled:
            return "桥接功能当前未启用"
        if (
            bridge_config.private_only
            and str(getattr(self.chat_stream, "chat_type", "") or "") != "private"
        ):
            return f"当前配置仅允许在私聊中{operation}"
        return None

    async def go_activate(self) -> bool:
        """根据总开关和私聊范围决定是否暴露 Action。"""
        return self._execution_denial() is None


class NDFCReplyAction(_NDFCBridgeAction):
    """向当前聊天流发送一条或多条自然语言消息。"""

    action_name = "nfc_reply"
    action_description = (
        "向对方发送文本。content 是字符串或消息段落列表，每个段落会作为独立消息"
        "依次发送。thought 只记录内心想法，绝不能混入 content。"
    )
    chatter_allow = ["neo_default_chatter"]
    associated_types = ["text"]

    async def execute(
        self,
        content: Annotated[
            list[str] | str | None,
            "要发送的消息；可传一条字符串或多条消息组成的列表。",
        ] = None,
        thought: Annotated[str, "你此刻真实的内心想法和回复理由。"] = "",
        expected_reaction: Annotated[str, "你预期对方看到消息后的反应。"] = "",
        max_wait_seconds: Annotated[
            float,
            "发出消息后愿意等待回复的最长秒数，0 表示不等待。",
        ] = 0.0,
        mood: Annotated[str, "你当前的心情，用一两个词描述。"] = "",
        reply_to: Annotated[str, "可选，要引用的消息 ID。"] = "",
        **extra: Any,
    ) -> tuple[bool, str]:
        """清洗、分段并发送文本消息。"""
        del thought, expected_reaction, max_wait_seconds, mood
        denial = self._execution_denial()
        if denial is not None:
            return False, denial
        if extra:
            logger.debug(f"忽略 nfc_reply 未知参数: {sorted(extra)}")

        if content is None:
            return False, "内容为空，未发送"
        raw_items: list[Any]
        if isinstance(content, str):
            raw_text = content.strip()
            parsed: Any = None
            if raw_text.startswith("[") and raw_text.endswith("]"):
                try:
                    parsed = json.loads(raw_text)
                except json.JSONDecodeError:
                    parsed = None
            raw_items = parsed if isinstance(parsed, list) else [raw_text]
        else:
            raw_items = list(content)

        segments: list[str] = []
        for item in raw_items:
            if isinstance(item, dict):
                text = str(item.get("text") or item.get("content") or "")
            else:
                text = str(item)
            text = _THINKING_BLOCK.sub("", text).strip()
            matches = [pattern.search(text) for pattern in _METADATA_PATTERNS]
            if sum(match is not None for match in matches) >= 2:
                text = text[: min(match.start() for match in matches if match)].strip()
            segments.extend(
                part.strip() for part in re.split(r"\n\n+", text) if part.strip()
            )
        if not segments:
            return False, "清洗后内容为空，未发送"

        delay_min = max(0.0, float(self.plugin.config.reply.segment_delay_min))
        delay_max = max(delay_min, float(self.plugin.config.reply.segment_delay_max))
        sent = 0
        for index, segment in enumerate(segments):
            if index > 0 and delay_max > 0:
                await asyncio.sleep(random.uniform(delay_min, delay_max))
            if reply_to and index == 0:
                success = await send_api.send_text(
                    content=segment,
                    stream_id=self.chat_stream.stream_id,
                    reply_to=reply_to,
                )
            else:
                success = await self._send_to_stream(segment)
            if not success:
                return False, f"第 {index + 1} 条消息发送失败，已发送 {sent} 条"
            sent += 1
        return True, f"已发送 {sent} 条消息"


class NDFCDoNothingAction(_NDFCBridgeAction):
    """选择不回复，并可继续等待用户。"""

    action_name = "do_nothing"
    action_description = (
        "选择不发送消息。对方的消息无需回应、你想已读不回，或你只想继续等待时使用。"
    )
    chatter_allow = ["neo_default_chatter"]
    associated_types = ["text"]

    async def execute(
        self,
        thought: Annotated[str, "你选择不回复的真实内心想法。"] = "",
        expected_reaction: Annotated[str, "你预期对方接下来的反应。"] = "",
        max_wait_seconds: Annotated[
            float,
            "继续等待对方的秒数，0 表示结束等待。",
        ] = 0.0,
        mood: Annotated[str, "你当前的心情。"] = "",
        **extra: Any,
    ) -> tuple[bool, str]:
        """确认本轮不发送可见消息。"""
        del thought, expected_reaction, max_wait_seconds, mood
        denial = self._execution_denial()
        if denial is not None:
            return False, denial
        if extra:
            logger.debug(f"忽略 do_nothing 未知参数: {sorted(extra)}")
        return True, "已选择不回复"


class NDFCUpdateMoodStateAction(_NDFCBridgeAction):
    """记录对话角色当前的情绪状态。"""

    action_name = "update_mood_state"
    action_description = "记录你当前的情绪，使后续回复和主动发起保持情绪连续性。"
    chatter_allow = ["neo_default_chatter"]
    associated_types = ["text"]

    async def execute(
        self,
        mood: Annotated[str, "当前情绪，用一到三个短语描述。"] = "",
        **extra: Any,
    ) -> tuple[bool, str]:
        """把情绪写入桥会话。"""
        denial = self._execution_denial()
        if denial is not None:
            return False, denial
        if extra:
            logger.debug(f"忽略 update_mood_state 未知参数: {sorted(extra)}")
        if not mood.strip():
            return False, "情绪不能为空"
        stream_id = self.chat_stream.stream_id
        async with self.plugin.session_store.lock(stream_id):
            session = await self.plugin.session_store.get_or_create(stream_id)
            session.record_mood(mood)
            await self.plugin.session_store.save(session)
        return True, f"已记录当前情绪：{mood.strip()}"


class NDFCRecordUserHabitAction(_NDFCBridgeAction):
    """记录从聊天中观察到的稳定用户习惯。"""

    action_name = "record_user_habit"
    action_description = (
        "记录有对话证据支持的用户作息、偏好或行为模式，供后续对话和主动发起参考。"
    )
    chatter_allow = ["neo_default_chatter"]
    associated_types = ["text"]

    async def execute(
        self,
        habit_text: Annotated[str, "习惯描述，必须基于对话中的真实证据。"] = "",
        category: Annotated[
            str,
            "可选分类，如 sleep、work、social、hobby 或 routine。",
        ] = "",
        **extra: Any,
    ) -> tuple[bool, str]:
        """把习惯观察写入桥会话。"""
        denial = self._execution_denial()
        if denial is not None:
            return False, denial
        if extra:
            logger.debug(f"忽略 record_user_habit 未知参数: {sorted(extra)}")
        if not habit_text.strip():
            return False, "习惯描述不能为空"
        stream_id = self.chat_stream.stream_id
        async with self.plugin.session_store.lock(stream_id):
            session = await self.plugin.session_store.get_or_create(stream_id)
            session.add_habit(habit_text, category)
            await self.plugin.session_store.save(session)
            count = len(session.user_habits)
        return True, f"已记录习惯，当前共 {count} 条观察"


class NDFCQueryActivityPatternAction(_NDFCBridgeAction):
    """查询桥观察到的用户活跃小时分布。"""

    action_name = "query_activity_pattern"
    action_description = (
        "查询对方在各小时出现的频率，用于判断对方通常何时活跃，避免在不合适的时间打扰。"
    )
    chatter_allow = ["neo_default_chatter"]
    associated_types = ["text"]

    async def execute(
        self,
        top_n: Annotated[int, "返回最活跃的小时数量，范围 1 到 12。"] = 5,
        **extra: Any,
    ) -> tuple[bool, str]:
        """返回本聊天流累计的活跃小时统计。"""
        denial = self._execution_denial()
        if denial is not None:
            return False, denial
        if extra:
            logger.debug(f"忽略 query_activity_pattern 未知参数: {sorted(extra)}")
        session = await self.plugin.session_store.peek(self.chat_stream.stream_id)
        if session is None or not session.activity_hours:
            return True, "尚无足够的用户活跃时段数据"
        count = max(1, min(int(top_n), 12))
        ranked = sorted(
            session.activity_hours.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:count]
        total = sum(session.activity_hours.values())
        details = "、".join(
            f"{int(hour):02d}:00（{value} 次）" for hour, value in ranked
        )
        return True, f"累计观察 {total} 次；最活跃时段：{details}"


class NDFCQueryUserHabitsAction(_NDFCBridgeAction):
    """查询桥记录的用户习惯观察。"""

    action_name = "query_user_habits"
    action_description = "查询之前记录的用户习惯，可按分类筛选；结果含可用于纠正或删除的 habit_id。"
    display_name = "查询用户习惯"
    chatter_allow = ["neo_default_chatter"]
    associated_types = ["text"]

    async def execute(
        self,
        category: Annotated[str, "可选分类，如 sleep、work、social、hobby 或 routine。"] = "",
        **extra: Any,
    ) -> tuple[bool, str]:
        """返回当前私聊已记录的习惯观察。"""
        denial = self._execution_denial()
        if denial is not None:
            return False, denial
        if extra:
            logger.debug(f"忽略 query_user_habits 未知参数: {sorted(extra)}")
        session = await self.plugin.session_store.peek(self.chat_stream.stream_id)
        habits = session.get_habits(category) if session is not None else []
        if not habits:
            return True, "尚未记录匹配的用户习惯观察"
        lines = [f"已记录 {len(habits)} 条用户习惯："]
        for habit in habits:
            category_text = str(habit.get("category", "") or "").strip()
            prefix = f"[{category_text}] " if category_text else ""
            lines.append(
                f"{prefix}{habit.get('habit_text', '')} "
                f"（habit_id: {habit.get('id', '')}）"
            )
        return True, "\n".join(lines)


class NDFCUpdateUserHabitAction(_NDFCBridgeAction):
    """纠正桥记录的一条用户习惯观察。"""

    action_name = "update_user_habit"
    action_description = "纠正已有用户习惯。必须先查询习惯并提供 habit_id；至少提供新描述或新分类。"
    display_name = "纠正用户习惯"
    chatter_allow = ["neo_default_chatter"]
    associated_types = ["text"]

    async def execute(
        self,
        habit_id: Annotated[str, "query_user_habits 返回的 habit_id。"] = "",
        habit_text: Annotated[str, "新的习惯描述；不改描述时留空。"] = "",
        category: Annotated[str, "新的分类；不改分类时留空。"] = "",
        **extra: Any,
    ) -> tuple[bool, str]:
        """更新并持久化一条习惯观察。"""
        denial = self._execution_denial()
        if denial is not None:
            return False, denial
        if extra:
            logger.debug(f"忽略 update_user_habit 未知参数: {sorted(extra)}")
        if not habit_id.strip() or not (habit_text.strip() or category.strip()):
            return False, "请提供 habit_id，以及新的习惯描述或分类"
        stream_id = self.chat_stream.stream_id
        async with self.plugin.session_store.lock(stream_id):
            session = await self.plugin.session_store.get_or_create(stream_id)
            if not session.update_habit(
                habit_id,
                habit_text=habit_text,
                category=category,
            ):
                return False, f"未找到习惯 ID：{habit_id.strip()}"
            await self.plugin.session_store.save(session)
        return True, f"已纠正习惯：{habit_id.strip()}"


class NDFCRemoveUserHabitAction(_NDFCBridgeAction):
    """删除桥记录的一条错误或过期习惯观察。"""

    action_name = "remove_user_habit"
    action_description = "删除错误或过期的用户习惯。必须先查询习惯并提供 habit_id。"
    display_name = "删除用户习惯"
    chatter_allow = ["neo_default_chatter"]
    associated_types = ["text"]

    async def execute(
        self,
        habit_id: Annotated[str, "query_user_habits 返回的 habit_id。"] = "",
        **extra: Any,
    ) -> tuple[bool, str]:
        """删除并持久化一条习惯观察。"""
        denial = self._execution_denial()
        if denial is not None:
            return False, denial
        if extra:
            logger.debug(f"忽略 remove_user_habit 未知参数: {sorted(extra)}")
        if not habit_id.strip():
            return False, "请提供 habit_id"
        stream_id = self.chat_stream.stream_id
        async with self.plugin.session_store.lock(stream_id):
            session = await self.plugin.session_store.get_or_create(stream_id)
            if not session.remove_habit(habit_id):
                return False, f"未找到习惯 ID：{habit_id.strip()}"
            await self.plugin.session_store.save(session)
        return True, f"已删除习惯：{habit_id.strip()}"


class NDFCScheduleProactiveAction(_NDFCBridgeAction):
    """设置或取消桥自己的主动发起预约。"""

    action_name = "schedule_proactive"
    action_description = (
        "预约未来主动发起一轮对话。新预约覆盖旧预约；delay_minutes=0 取消预约，"
        "其他值为 30 到 1440 分钟。reason 应写下未来能自然接续的真实想法。"
    )
    chatter_allow = ["neo_default_chatter"]
    associated_types = ["text"]

    async def go_activate(self) -> bool:
        """仅在主动功能启用且聊天流符合桥接范围时暴露预约工具。"""
        return self.plugin.config.proactive.enabled and await super().go_activate()

    async def execute(
        self,
        delay_minutes: Annotated[
            int,
            "多少分钟后主动发起；0 表示取消，其他值限制为 30 到 1440。",
        ] = 30,
        reason: Annotated[str, "预约理由；取消时可以留空。"] = "",
        **extra: Any,
    ) -> tuple[bool, str]:
        """把主动发起预约直接写入桥会话。"""
        denial = self._execution_denial("预约主动发起")
        if denial is not None:
            return False, denial
        if extra:
            logger.debug(f"忽略 schedule_proactive 未知参数: {sorted(extra)}")
        stream_id = self.chat_stream.stream_id
        async with self.plugin.session_store.lock(stream_id):
            session = await self.plugin.session_store.get_or_create(stream_id)
            if delay_minutes == 0:
                session.set_scheduled_proactive(None)
                await self.plugin.session_store.save(session)
                return True, "已取消当前主动发起预约"
            if not self.plugin.config.proactive.enabled:
                return False, "主动发起功能当前未启用"
            delay = max(30, min(int(delay_minutes), 1440))
            scheduled_at = time.time() + delay * 60
            session.set_scheduled_proactive(scheduled_at, reason)
            await self.plugin.session_store.save(session)
        schedule_text = time.strftime("%Y-%m-%d %H:%M", time.localtime(scheduled_at))
        return True, f"已预约在 {schedule_text} 主动发起"


BRIDGE_NFC_ACTIONS: list[type[BaseAction]] = [
    NDFCReplyAction,
    NDFCDoNothingAction,
    NDFCUpdateMoodStateAction,
    NDFCRecordUserHabitAction,
    NDFCQueryActivityPatternAction,
    NDFCQueryUserHabitsAction,
    NDFCUpdateUserHabitAction,
    NDFCRemoveUserHabitAction,
    NDFCScheduleProactiveAction,
]


__all__ = [
    "BRIDGE_NFC_ACTIONS",
    "NDFCDoNothingAction",
    "NDFCQueryActivityPatternAction",
    "NDFCQueryUserHabitsAction",
    "NDFCRecordUserHabitAction",
    "NDFCReplyAction",
    "NDFCRemoveUserHabitAction",
    "NDFCScheduleProactiveAction",
    "NDFCUpdateUserHabitAction",
    "NDFCUpdateMoodStateAction",
]
