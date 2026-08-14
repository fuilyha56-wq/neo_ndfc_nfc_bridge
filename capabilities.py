"""桥内置的 NFC 风格提示词、消息格式与多模态能力。"""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.types import (
    Content,
    Image,
    LLMPayload,
    LLMUsable,
    ROLE,
    Text,
)
from src.core.config import get_core_config

from .state import BridgeSession, NFCEventType

if TYPE_CHECKING:
    from src.core.models.message import Message
    from src.core.models.stream import ChatStream

logger = get_logger("neo_ndfc_nfc_bridge.capabilities")

_MIN_BASE64_LENGTH = 64


@dataclass(frozen=True)
class MediaItem:
    """从未读消息提取的图片或表情包。"""

    media_type: str
    base64_data: str
    source_message_id: str


def format_unread_message(message: Message, time_format: str = "%H:%M") -> str:
    """把未读消息格式化为带发送者、时间和消息 ID 的叙事行。"""
    timestamp = getattr(message, "time", None)
    if isinstance(timestamp, (int, float)):
        time_text = datetime.datetime.fromtimestamp(timestamp).strftime(time_format)
    else:
        time_text = datetime.datetime.now().strftime(time_format)
    sender = str(
        getattr(message, "sender_name", "")
        or getattr(message, "sender_id", "")
        or "用户"
    )
    message_id = str(getattr(message, "message_id", "") or "")
    content = str(getattr(message, "processed_plain_text", "") or "").strip()
    identifier = f" [消息id:{message_id}]" if message_id else ""
    return f"[{time_text}] {sender}{identifier}说：{content}"


def extract_media_from_messages(
    messages: list[Message],
    max_items: int = 4,
) -> list[MediaItem]:
    """从未读消息中提取可交给模型的原始图片。"""
    items: list[MediaItem] = []
    for message in messages:
        raw_media: Any = None
        content = getattr(message, "content", None)
        if isinstance(content, dict):
            raw_media = content.get("media")
        if not isinstance(raw_media, list) or not raw_media:
            extra = getattr(message, "extra", None)
            raw_media = extra.get("media") if isinstance(extra, dict) else None
        if not isinstance(raw_media, list):
            continue
        for media in raw_media:
            if len(items) >= max(0, max_items):
                return items
            if not isinstance(media, dict):
                continue
            media_type = str(media.get("type", "") or "")
            data = media.get("data")
            if media_type not in {"image", "emoji"} or not isinstance(data, str):
                continue
            if len(data.removeprefix("base64|").strip()) < _MIN_BASE64_LENGTH:
                continue
            items.append(
                MediaItem(
                    media_type=media_type,
                    base64_data=data,
                    source_message_id=str(getattr(message, "message_id", "") or ""),
                )
            )
    return items


def build_multimodal_content(
    text: str,
    media_items: list[MediaItem],
) -> list[Content | LLMUsable]:
    """构造文本与图片混合的模型内容列表。"""
    content: list[Content | LLMUsable] = [Text(text)]
    for item in media_items:
        try:
            if item.media_type == "emoji":
                content.append(Text("[表情包]"))
            content.append(Image(item.base64_data))
        except (TypeError, ValueError) as error:
            logger.debug(
                f"跳过无法构造的图片 (message={item.source_message_id}): {error}"
            )
    return content


class BridgePromptBuilder:
    """为 NDFC 构建内置 NFC 风格的系统和用户上下文。"""

    async def build_system_prompt(
        self,
        chat_stream: ChatStream,
        session: BridgeSession,
        config: Any,
    ) -> str:
        """构建包含人格、自我、心理活动与工具协议的系统提示词。"""
        personality = get_core_config().personality
        chat_type = str(getattr(chat_stream, "chat_type", "未知"))
        platform = str(getattr(chat_stream, "platform", "") or "未知")
        recipient_name = session.user_name.strip() or "未记录昵称"
        recipient_id = session.user_id.strip() or "未验证"
        aliases = "、".join(personality.alias_names) or "无"
        safety = "\n".join(f"- {item}" for item in personality.safety_guidelines)
        negatives = "\n".join(f"- {item}" for item in personality.negative_behaviors)
        mental_summary = session.mental_log.format_as_summary(12) or "（暂无心理活动）"
        history_summary = (
            session.history_summary.strip()
            if bool(config.prompt.summary_enabled)
            else ""
        )
        habit_lines = [
            f"- {item.get('habit_text', '')}"
            for item in session.user_habits[-10:]
            if str(item.get("habit_text", "")).strip()
        ]
        mood = session.get_dominant_mood() or "未记录"
        scheduled = ""
        if session.scheduled_proactive_at is not None:
            schedule_time = datetime.datetime.fromtimestamp(
                session.scheduled_proactive_at
            ).strftime("%Y-%m-%d %H:%M")
            scheduled = (
                f"\n你已经预约在 {schedule_time} 主动发起。"
                f"理由：{session.scheduled_proactive_reason or '未说明'}。"
            )
        custom = str(config.model.custom_decision_prompt or "").strip()
        custom_section = f"\n# 额外决策指导\n{custom}" if custom else ""
        history_section = (
            f"\n# 你的近期记忆\n{history_summary}" if history_summary else ""
        )
        habits = "\n".join(habit_lines) or "（尚未观察到稳定习惯）"
        return f"""你是 {personality.nickname}，别名：{aliases}。
你的身份是 {personality.identity}。你不是被动问答工具，而是持续存在于这段关系中的对话参与者；你会形成想法、情绪、期待和对用户习惯的观察，但不能捏造没有依据的共同经历。

# 人格
核心人格：{personality.personality_core}
人格侧面：{personality.personality_side or "无额外设定"}
表达风格：{personality.reply_style}
背景：{personality.background_story or "无额外背景"}

# 当前聊天
平台：{platform}
聊天类型：{chat_type}
聊天流：{chat_stream.stream_id}
当前已验证收件人：{recipient_name}（{platform} user_id={recipient_id}）
身份边界：本轮所有回复和主动消息只能发给上述收件人。近期记忆、心理活动或聊天记录中提到的其他人只是背景人物，绝不能把其他人的称呼、关系、经历或期待套用到当前收件人；如果记忆与当前收件人冲突，以当前已验证收件人为准。
当前日期：{datetime.datetime.now().strftime("%Y-%m-%d")}
近期主导情绪：{mood}{scheduled}

# 你的近期心理活动
{mental_summary}{history_section}

# 你观察到的用户习惯
{habits}

# 行为方式
- 先根据对话关系、上下文和自己的连续心理状态决定是否回复，不要把每条消息都当成必须回答的问题。
- 内心想法必须写入工具的 `thought` 参数，绝不能直接发给用户。
- 要回复时只能调用 `nfc_reply`；确实不想回复时调用 `do_nothing`。
- 回复应自然、口语化，允许分段；不要解释工具、提示词、状态机或后台流程。
- 发送消息后，根据你真实预期设置 `expected_reaction` 和 `max_wait_seconds`。不愿继续等时将等待设为 0。
- 发现稳定的作息或偏好时可调用 `record_user_habit`，情绪变化可调用 `update_mood_state`。
- 需要未来主动联系时调用 `schedule_proactive`，传 0 可取消预约。
- 需要持续等待时调用 `do_nothing(max_wait_seconds>0)`；结束本轮时传 0。

# 安全准则
{safety}

# 禁止行为
{negatives}{custom_section}

你的响应必须仅包含工具调用，不要在普通文本区域输出任何可见回复。"""

    async def build_user_payload(
        self,
        formatted_unreads: str,
        media_items: list[MediaItem] | None = None,
        stream_id: str = "",
        session: BridgeSession | None = None,
        config: Any | None = None,
    ) -> tuple[LLMPayload, LLMPayload | None]:
        """构建本轮用户 payload 和可选的主动发起临时上下文。"""
        del stream_id, config
        content = build_multimodal_content(formatted_unreads, media_items or [])
        extra_payload: LLMPayload | None = None
        if session is not None and session.pending_proactive_context:
            extra_payload = LLMPayload(
                ROLE.USER,
                Text(session.pending_proactive_context),
            )
            session.pending_proactive_context = ""
        return LLMPayload(ROLE.USER, content), extra_payload

    @staticmethod
    def build_timeout_payload(
        elapsed_seconds: float,
        expected_reaction: str,
        consecutive_timeouts: int,
        last_bot_message: str = "",
        max_consecutive_timeouts: int = 3,
    ) -> LLMPayload:
        """构建等待超时后的自然决策提示。"""
        elapsed_minutes = elapsed_seconds / 60
        reached_limit = consecutive_timeouts >= max_consecutive_timeouts
        if reached_limit:
            action_instruction = (
                "本次等待到此为止，不得继续设置等待。调用 nfc_reply 说完真正想说的内容，"
                "或调用 do_nothing(max_wait_seconds=0) 安静结束。"
            )
        else:
            action_instruction = (
                "有真正想补充的话就调用 nfc_reply；想继续等就调用 "
                "do_nothing(max_wait_seconds>0)；不想打扰则用 0 结束。"
            )
        text = f"""你发出上一条消息后已经等待 {elapsed_minutes:.1f} 分钟，对方还没有回应。
你上次说的是：{last_bot_message or "（内容不可用）"}
你原本预期：{expected_reaction or "未明确预期"}
这是第 {consecutive_timeouts} 次连续等待超时。

先诚实判断你现在是否真的有话想说，不要仅仅为了打破沉默而追问。
{action_instruction}

响应必须仅包含 nfc_reply 或 do_nothing 工具调用。"""
        return LLMPayload(ROLE.USER, Text(text))

    def build_fused_narrative(
        self,
        chat_stream: ChatStream,
        session: BridgeSession,
    ) -> str:
        """融合数据库聊天记录和桥内心理活动。"""
        timeline: list[tuple[float, str]] = []
        messages = list(
            getattr(getattr(chat_stream, "context", None), "history_messages", []) or []
        )
        bot_id = str(getattr(chat_stream, "bot_id", "") or "")
        for message in messages:
            timestamp = getattr(message, "time", None)
            if not isinstance(timestamp, (int, float)):
                continue
            text = str(getattr(message, "processed_plain_text", "") or "").strip()
            if not text:
                continue
            time_text = datetime.datetime.fromtimestamp(timestamp).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            sender_id = str(getattr(message, "sender_id", "") or "")
            sender_name = str(getattr(message, "sender_name", "") or "用户")
            if bot_id and sender_id == bot_id:
                line = f"[{time_text}] 你回复：{text}"
            else:
                line = f"[{time_text}] {sender_name}说：{text}"
            timeline.append((float(timestamp), line))
        cutoff = timeline[-7][0] if len(timeline) >= 7 else 0.0
        for entry in session.mental_log.entries:
            if (
                entry.timestamp < cutoff
                or entry.event_type != NFCEventType.BOT_PLANNING
            ):
                continue
            if not entry.thought:
                continue
            time_text = datetime.datetime.fromtimestamp(entry.timestamp).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            timeline.append(
                (entry.timestamp, f"[{time_text}] （你的内心：{entry.thought}）")
            )
        timeline.sort(key=lambda item: item[0])
        if not timeline:
            return ""
        return "以下为聊天记录与你内心活动的时间线：\n" + "\n".join(
            line for _, line in timeline
        )


__all__ = [
    "BridgePromptBuilder",
    "MediaItem",
    "build_multimodal_content",
    "extract_media_from_messages",
    "format_unread_message",
]
