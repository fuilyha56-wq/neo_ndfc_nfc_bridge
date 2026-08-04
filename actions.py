"""供 Neo-Default-Chatter 调用的 NFC Action 适配器。"""

from __future__ import annotations

import time
from typing import Annotated, cast

from plugins.neo_fatum_chatter.actions.do_nothing import DoNothingAction
from plugins.neo_fatum_chatter.actions.query_activity_pattern import (
    QueryActivityPatternAction,
)
from plugins.neo_fatum_chatter.actions.query_habits import QueryHabitsAction
from plugins.neo_fatum_chatter.actions.record_habit import RecordHabitAction
from plugins.neo_fatum_chatter.actions.reply import NFCReplyAction
from plugins.neo_fatum_chatter.actions.schedule_proactive import (
    ScheduleProactiveAction,
)

from .config import NdfcNfcBridgeConfig

_NDFC_CHATTER_ALLOW = ["neo_default_chatter"]


class _BridgeActionScopeMixin:
    """统一限制桥 Action 的配置开关与聊天范围。"""

    async def go_activate(self) -> bool:
        """判断当前聊天流是否应暴露 NFC Action。"""
        config = cast(NdfcNfcBridgeConfig, self.plugin.config).bridge
        if not config.enabled or not config.expose_nfc_actions:
            return False
        chat_type = str(getattr(self.chat_stream, "chat_type", "") or "")
        return not config.private_only or chat_type == "private"


class BridgeNFCReplyAction(_BridgeActionScopeMixin, NFCReplyAction):
    """在 NDFC 中提供 NFC 的分段、清洗和流式回复能力。"""

    chatter_allow = _NDFC_CHATTER_ALLOW


class BridgeDoNothingAction(_BridgeActionScopeMixin, DoNothingAction):
    """在 NDFC 中提供携带心理元数据的不回复决策。"""

    chatter_allow = _NDFC_CHATTER_ALLOW


class BridgeQueryActivityPatternAction(
    _BridgeActionScopeMixin, QueryActivityPatternAction
):
    """在 NDFC 中提供用户活跃时段查询。"""

    chatter_allow = _NDFC_CHATTER_ALLOW


class BridgeRecordHabitAction(_BridgeActionScopeMixin, RecordHabitAction):
    """在 NDFC 中把用户习惯写入 NFC 的共享会话。"""

    chatter_allow = _NDFC_CHATTER_ALLOW


class BridgeQueryHabitsAction(_BridgeActionScopeMixin, QueryHabitsAction):
    """在 NDFC 中查询 NFC 会话保存的用户习惯。"""

    chatter_allow = _NDFC_CHATTER_ALLOW


class BridgeScheduleProactiveAction(_BridgeActionScopeMixin, ScheduleProactiveAction):
    """在 NDFC 中持久化 NFC 主动发起预约。"""

    chatter_allow = _NDFC_CHATTER_ALLOW

    async def execute(
        self,
        delay_minutes: Annotated[
            int,
            "多少分钟后发起主动思考。传 0 表示取消；其他值范围 30~1440。",
        ] = 30,
        reason: Annotated[str, "预约主动思考的真实理由；取消时可留空。"] = "",
        **_extra: object,
    ) -> tuple[bool, str]:
        """把预约直接写入桥接插件共享的 NFC SessionStore。

        Args:
            delay_minutes: 延迟分钟数，0 表示取消。
            reason: 未来主动思考时使用的理由。
            **_extra: 模型可能生成的未知参数。

        Returns:
            预约执行结果和状态文本。
        """
        if delay_minutes == 0:
            scheduled_at = None
            result_text = "已取消当前主动思考预约"
        else:
            delay_minutes = max(
                self._MIN_DELAY_MIN, min(self._MAX_DELAY_MIN, delay_minutes)
            )
            scheduled_at = time.time() + delay_minutes * 60
            result_text = f"已预约在 {delay_minutes} 分钟后主动思考"

        session_store = self.plugin.session_store
        stream_id = self.chat_stream.stream_id
        async with session_store.lock(stream_id):
            session = await session_store.get_or_create(stream_id)
            session.set_scheduled_proactive(scheduled_at, reason)
            await session_store.save(session)
        return True, result_text


BRIDGE_NFC_ACTIONS = (
    BridgeNFCReplyAction,
    BridgeDoNothingAction,
    BridgeScheduleProactiveAction,
    BridgeQueryActivityPatternAction,
    BridgeRecordHabitAction,
    BridgeQueryHabitsAction,
)

__all__ = [
    "BRIDGE_NFC_ACTIONS",
    "BridgeDoNothingAction",
    "BridgeNFCReplyAction",
    "BridgeQueryActivityPatternAction",
    "BridgeQueryHabitsAction",
    "BridgeRecordHabitAction",
    "BridgeScheduleProactiveAction",
]
