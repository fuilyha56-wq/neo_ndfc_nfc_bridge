"""NDFC-NFC 桥的心理状态模型与独立会话存储。"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import Counter
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from src.app.plugin_system.api.log_api import get_logger
from src.kernel.storage import JSONStore

logger = get_logger("neo_ndfc_nfc_bridge.state")


class NFCEventType(Enum):
    """心理活动流中的事件类型。"""

    USER_MESSAGE = "user_message"
    BOT_PLANNING = "bot_planning"
    WAITING_START = "waiting_start"
    WAITING_UPDATE = "waiting_update"
    REPLY_IN_TIME = "reply_in_time"
    REPLY_LATE = "reply_late"
    WAIT_TIMEOUT = "wait_timeout"
    PROACTIVE_TRIGGER = "proactive_trigger"
    USER_INTERRUPTED = "user_interrupted"

    def __str__(self) -> str:
        """返回可持久化的事件名称。"""
        return self.value


@dataclass
class WaitingConfig:
    """一次回复后的等待状态。"""

    expected_reaction: str = ""
    max_wait_seconds: float = 0.0
    started_at: float = 0.0
    followup_count: int = 0

    def is_active(self) -> bool:
        """返回当前是否处于有效等待期。"""
        return self.max_wait_seconds > 0 and self.started_at > 0

    def get_elapsed_seconds(self) -> float:
        """返回已经等待的秒数。"""
        if not self.is_active():
            return 0.0
        return max(0.0, time.time() - self.started_at)

    def is_timeout(self) -> bool:
        """返回等待是否已经超时。"""
        return self.is_active() and self.get_elapsed_seconds() >= self.max_wait_seconds

    def reset(self) -> None:
        """清空等待状态。"""
        self.expected_reaction = ""
        self.max_wait_seconds = 0.0
        self.started_at = 0.0
        self.followup_count = 0

    def to_dict(self) -> dict[str, Any]:
        """序列化等待状态。"""
        return {
            "expected_reaction": self.expected_reaction,
            "max_wait_seconds": self.max_wait_seconds,
            "started_at": self.started_at,
            "followup_count": self.followup_count,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WaitingConfig:
        """从字典恢复等待状态。"""
        return cls(
            expected_reaction=str(data.get("expected_reaction", "") or ""),
            max_wait_seconds=float(data.get("max_wait_seconds", 0) or 0),
            started_at=float(data.get("started_at", 0) or 0),
            followup_count=int(data.get("followup_count", 0) or 0),
        )


@dataclass
class MentalLogEntry:
    """心理活动流中的单个事件。"""

    event_type: NFCEventType
    timestamp: float
    content: str = ""
    user_name: str = ""
    user_id: str = ""
    message_id: str = ""
    thought: str = ""
    actions: list[dict[str, Any]] = field(default_factory=list)
    expected_reaction: str = ""
    max_wait_seconds: float = 0.0
    elapsed_seconds: float = 0.0
    waiting_thought: str = ""
    mood: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """序列化心理活动条目。"""
        return {
            "event_type": str(self.event_type),
            "timestamp": self.timestamp,
            "content": self.content,
            "user_name": self.user_name,
            "user_id": self.user_id,
            "message_id": self.message_id,
            "thought": self.thought,
            "actions": self.actions,
            "expected_reaction": self.expected_reaction,
            "max_wait_seconds": self.max_wait_seconds,
            "elapsed_seconds": self.elapsed_seconds,
            "waiting_thought": self.waiting_thought,
            "mood": self.mood,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MentalLogEntry:
        """从桥或旧 NFC 字典恢复心理活动条目。"""
        try:
            event_type = NFCEventType(str(data.get("event_type", "user_message")))
        except ValueError:
            event_type = NFCEventType.USER_MESSAGE
        return cls(
            event_type=event_type,
            timestamp=float(data.get("timestamp", time.time()) or time.time()),
            content=str(data.get("content", "") or ""),
            user_name=str(data.get("user_name", "") or ""),
            user_id=str(data.get("user_id", "") or ""),
            message_id=str(data.get("message_id", "") or ""),
            thought=str(data.get("thought", "") or ""),
            actions=list(data.get("actions", []) or []),
            expected_reaction=str(data.get("expected_reaction", "") or ""),
            max_wait_seconds=float(data.get("max_wait_seconds", 0) or 0),
            elapsed_seconds=float(data.get("elapsed_seconds", 0) or 0),
            waiting_thought=str(data.get("waiting_thought", "") or ""),
            mood=str(data.get("mood", "") or ""),
            metadata=dict(data.get("metadata", {}) or {}),
        )


class MentalLog:
    """带上限裁剪的心理活动流。"""

    def __init__(self, max_entries: int = 50) -> None:
        """初始化心理活动流。"""
        self._entries: list[MentalLogEntry] = []
        self._max_entries = max(1, max_entries)

    @property
    def entries(self) -> list[MentalLogEntry]:
        """返回活动条目的只读副本。"""
        return list(self._entries)

    def add(self, entry: MentalLogEntry) -> None:
        """追加条目并裁剪最旧记录。"""
        self._entries.append(entry)
        self._entries = self._entries[-self._max_entries :]

    def get_last_bot_reply_content(self) -> str:
        """返回最近一次规划中真正发送的文本。"""
        for entry in reversed(self._entries):
            if entry.event_type != NFCEventType.BOT_PLANNING:
                continue
            for action in entry.actions:
                if action.get("type") not in {
                    "nfc_reply",
                    "action-nfc_reply",
                    "respond",
                }:
                    continue
                content = action.get("content", "")
                if isinstance(content, list):
                    return " ".join(str(item) for item in content if str(item).strip())
                if isinstance(content, str):
                    return content
        return ""

    def format_as_summary(self, max_entries: int = 12) -> str:
        """把近期活动格式化为紧凑时间线。"""
        lines: list[str] = []
        for entry in self._entries[-max_entries:]:
            time_text = time.strftime("%H:%M", time.localtime(entry.timestamp))
            if entry.event_type == NFCEventType.USER_MESSAGE:
                summary = f"{entry.user_name or '用户'}：{entry.content[:120]}"
            elif entry.event_type == NFCEventType.BOT_PLANNING:
                summary = f"你的内心：{entry.thought[:120]}"
            elif entry.event_type == NFCEventType.WAIT_TIMEOUT:
                summary = f"等待超时（{entry.elapsed_seconds:.0f} 秒）"
            else:
                summary = (
                    entry.content or entry.waiting_thought or str(entry.event_type)
                )[:120]
            lines.append(f"[{time_text}] {summary}")
        return "\n".join(lines)

    def to_list(self) -> list[dict[str, Any]]:
        """序列化全部心理活动条目。"""
        return [entry.to_dict() for entry in self._entries]

    @classmethod
    def from_list(
        cls,
        data: list[dict[str, Any]],
        max_entries: int = 50,
    ) -> MentalLog:
        """从字典列表恢复心理活动流。"""
        log = cls(max_entries=max_entries)
        for item in data[-max_entries:]:
            if isinstance(item, dict):
                log.add(MentalLogEntry.from_dict(item))
        return log


@dataclass
class BridgeSession:
    """桥独立维护的 NFC 风格会话状态。"""

    user_id: str
    stream_id: str
    platform: str = ""
    user_name: str = ""
    waiting_config: WaitingConfig = field(default_factory=WaitingConfig)
    consecutive_timeout_count: int = 0
    created_at: float = field(default_factory=time.time)
    last_activity_at: float = field(default_factory=time.time)
    last_user_message_at: float | None = None
    last_proactive_at: float | None = None
    scheduled_proactive_at: float | None = None
    scheduled_proactive_reason: str = ""
    pending_proactive_context: str = ""
    mental_log: MentalLog = field(default_factory=MentalLog)
    history_summary: str = ""
    last_compress_at: float = 0.0
    compress_round_count: int = 0
    request_snapshot: dict[str, Any] = field(default_factory=dict)
    mood_history: list[dict[str, Any]] = field(default_factory=list)
    activity_hours: dict[str, int] = field(default_factory=dict)
    user_habits: list[dict[str, Any]] = field(default_factory=list)
    total_interactions: int = 0

    def set_waiting(self, config: WaitingConfig) -> None:
        """设置等待状态；非正等待时间等同于清除等待。"""
        if config.max_wait_seconds <= 0:
            self.clear_waiting()
            return
        self.waiting_config = config

    def clear_waiting(self) -> None:
        """结束当前等待并刷新活动时间。"""
        self.waiting_config.reset()
        self.last_activity_at = time.time()

    def is_waiting(self) -> bool:
        """返回会话是否正在等待用户。"""
        return self.waiting_config.is_active()

    def add_user_message(
        self,
        content: str,
        user_name: str,
        user_id: str,
        timestamp: float | None = None,
        message_id: str = "",
    ) -> MentalLogEntry:
        """记录用户消息和回复时效。"""
        message_time = timestamp or time.time()
        entry = MentalLogEntry(
            event_type=NFCEventType.USER_MESSAGE,
            timestamp=message_time,
            content=content,
            user_name=user_name,
            user_id=user_id,
            message_id=message_id,
        )
        if self.waiting_config.is_active():
            elapsed = self.waiting_config.get_elapsed_seconds()
            entry.metadata["reply_status"] = (
                "in_time" if elapsed <= self.waiting_config.max_wait_seconds else "late"
            )
            entry.metadata["elapsed_seconds"] = elapsed
        self.mental_log.add(entry)
        self.consecutive_timeout_count = 0
        self.last_user_message_at = message_time
        self.last_activity_at = message_time
        hour = str(time.localtime(message_time).tm_hour)
        self.activity_hours[hour] = self.activity_hours.get(hour, 0) + 1
        return entry

    def bind_user_identity(
        self,
        user_id: str,
        user_name: str = "",
        platform: str = "",
    ) -> bool:
        """绑定当前流的真实收件人，并在身份变化时隔离旧心理状态。"""
        normalized_user_id = str(user_id or "").strip()
        if not normalized_user_id:
            return False
        changed = bool(self.user_id and self.user_id != normalized_user_id)
        if changed:
            self.waiting_config.reset()
            self.mental_log = MentalLog(self.mental_log._max_entries)
            self.history_summary = ""
            self.mood_history = []
            self.user_habits = []
            self.request_snapshot = {}
            self.last_compress_at = 0.0
            self.compress_round_count = 0
        self.user_id = normalized_user_id
        if user_name.strip():
            self.user_name = user_name.strip()
        if platform.strip():
            self.platform = platform.strip()
        return changed

    def add_bot_planning(
        self,
        thought: str,
        actions: list[dict[str, Any]],
        expected_reaction: str = "",
        max_wait_seconds: float = 0.0,
        raw_response: str = "",
    ) -> MentalLogEntry:
        """记录模型的内心想法和本轮动作。"""
        entry = MentalLogEntry(
            event_type=NFCEventType.BOT_PLANNING,
            timestamp=time.time(),
            thought=thought,
            actions=actions,
            expected_reaction=expected_reaction,
            max_wait_seconds=max_wait_seconds,
        )
        if raw_response:
            entry.metadata["raw_response"] = raw_response
        self.mental_log.add(entry)
        self.total_interactions += 1
        self.last_activity_at = time.time()
        return entry

    def record_mood(self, mood: str) -> None:
        """记录情绪轨迹。"""
        value = mood.strip()
        if not value:
            return
        self.mood_history.append({"mood": value, "ts": time.time()})
        self.mood_history = self.mood_history[-30:]

    def get_dominant_mood(self, recent_n: int = 5) -> str:
        """返回近期最常见情绪。"""
        moods = [str(item.get("mood", "")) for item in self.mood_history[-recent_n:]]
        counter = Counter(mood for mood in moods if mood)
        return counter.most_common(1)[0][0] if counter else ""

    def add_habit(self, habit_text: str, category: str = "") -> None:
        """记录一条用户习惯观察。"""
        value = habit_text.strip()
        if not value:
            return
        self.user_habits.append(
            {
                "id": uuid.uuid4().hex[:8],
                "habit_text": value,
                "category": category.strip(),
                "recorded_at": time.time(),
            }
        )
        self.user_habits = self.user_habits[-50:]

    def get_habits(self, category: str = "") -> list[dict[str, Any]]:
        """返回已记录习惯，可选按分类筛选。"""
        target_category = category.strip().lower()
        if not target_category:
            return list(self.user_habits)
        return [
            habit
            for habit in self.user_habits
            if str(habit.get("category", "") or "").lower() == target_category
        ]

    def update_habit(
        self,
        habit_id: str,
        *,
        habit_text: str = "",
        category: str = "",
    ) -> bool:
        """按稳定 ID 修正一条习惯观察。"""
        target_id = habit_id.strip()
        if not target_id:
            return False
        for habit in self.user_habits:
            if str(habit.get("id", "") or "") != target_id:
                continue
            if habit_text.strip():
                habit["habit_text"] = habit_text.strip()
            if category.strip():
                habit["category"] = category.strip()
            habit["updated_at"] = time.time()
            return True
        return False

    def remove_habit(self, habit_id: str) -> bool:
        """按稳定 ID 删除一条错误或过期的习惯观察。"""
        target_id = habit_id.strip()
        if not target_id:
            return False
        original_count = len(self.user_habits)
        self.user_habits = [
            habit
            for habit in self.user_habits
            if str(habit.get("id", "") or "") != target_id
        ]
        return len(self.user_habits) != original_count

    def set_scheduled_proactive(self, at: float | None, reason: str = "") -> None:
        """设置或清除下一次主动发起预约。"""
        self.scheduled_proactive_at = at
        self.scheduled_proactive_reason = reason.strip() if at is not None else ""

    def to_dict(self) -> dict[str, Any]:
        """序列化桥会话。"""
        return {
            "user_id": self.user_id,
            "stream_id": self.stream_id,
            "platform": self.platform,
            "user_name": self.user_name,
            "waiting_config": self.waiting_config.to_dict(),
            "consecutive_timeout_count": self.consecutive_timeout_count,
            "created_at": self.created_at,
            "last_activity_at": self.last_activity_at,
            "last_user_message_at": self.last_user_message_at,
            "last_proactive_at": self.last_proactive_at,
            "scheduled_proactive_at": self.scheduled_proactive_at,
            "scheduled_proactive_reason": self.scheduled_proactive_reason,
            "mental_log": self.mental_log.to_list(),
            "history_summary": self.history_summary,
            "last_compress_at": self.last_compress_at,
            "compress_round_count": self.compress_round_count,
            "request_snapshot": self.request_snapshot,
            "mood_history": self.mood_history,
            "activity_hours": self.activity_hours,
            "user_habits": self.user_habits,
            "total_interactions": self.total_interactions,
        }

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        max_log_entries: int = 50,
    ) -> BridgeSession:
        """从桥或旧 NFC 会话字典恢复所需状态。"""
        session = cls(
            user_id=str(data.get("user_id", "") or ""),
            stream_id=str(data.get("stream_id", "") or ""),
            platform=str(data.get("platform", "") or ""),
            user_name=str(data.get("user_name", "") or ""),
        )
        session.waiting_config = WaitingConfig.from_dict(
            dict(data.get("waiting_config", {}) or {})
        )
        session.consecutive_timeout_count = int(
            data.get("consecutive_timeout_count", 0) or 0
        )
        session.created_at = float(data.get("created_at", time.time()) or time.time())
        session.last_activity_at = float(
            data.get("last_activity_at", time.time()) or time.time()
        )
        for attribute in (
            "last_user_message_at",
            "last_proactive_at",
            "scheduled_proactive_at",
        ):
            value = data.get(attribute)
            setattr(session, attribute, float(value) if value is not None else None)
        session.scheduled_proactive_reason = str(
            data.get("scheduled_proactive_reason", "") or ""
        )
        raw_log = data.get("mental_log", [])
        if isinstance(raw_log, list):
            session.mental_log = MentalLog.from_list(raw_log, max_log_entries)
        session.history_summary = str(data.get("history_summary", "") or "")
        session.last_compress_at = float(data.get("last_compress_at", 0) or 0)
        session.compress_round_count = int(data.get("compress_round_count", 0) or 0)
        raw_snapshot = data.get("request_snapshot", {})
        session.request_snapshot = (
            dict(raw_snapshot) if isinstance(raw_snapshot, dict) else {}
        )
        session.mood_history = [
            item for item in data.get("mood_history", []) if isinstance(item, dict)
        ][-30:]
        raw_hours = data.get("activity_hours", {})
        if isinstance(raw_hours, dict):
            session.activity_hours = {
                str(hour): int(count)
                for hour, count in raw_hours.items()
                if isinstance(count, (int, float)) and not isinstance(count, bool)
            }
        session.user_habits = []
        for item in data.get("user_habits", []):
            if not isinstance(item, dict):
                continue
            habit_text = str(item.get("habit_text", "") or "").strip()
            if not habit_text:
                continue
            habit = dict(item)
            habit["habit_text"] = habit_text
            habit["id"] = str(habit.get("id", "") or uuid.uuid4().hex[:8])
            session.user_habits.append(habit)
        session.user_habits = session.user_habits[-50:]
        session.total_interactions = int(data.get("total_interactions", 0) or 0)
        return session


class BridgeSessionStore:
    """桥专用的异步 JSON 会话存储。"""

    def __init__(self, max_log_entries: int = 50) -> None:
        """初始化本地和旧数据存储句柄。"""
        self._sessions: dict[str, BridgeSession] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._max_log_entries = max(1, max_log_entries)
        self._storage_path = Path("data/neo_ndfc_nfc_bridge/sessions")
        self._legacy_storage_path = Path("data/neo_fatum_chatter/sessions")
        self._store = JSONStore(self._storage_path)
        self._legacy_store = JSONStore(self._legacy_storage_path)

    @asynccontextmanager
    async def lock(self, stream_id: str) -> AsyncIterator[None]:
        """串行化同一聊天流的状态读写。"""
        lock = self._locks.setdefault(stream_id, asyncio.Lock())
        async with lock:
            yield

    async def get_or_create(self, stream_id: str) -> BridgeSession:
        """获取会话，不存在时尝试导入旧数据后创建。"""
        session = await self.get(stream_id)
        if session is not None:
            return session
        session = BridgeSession(user_id="", stream_id=stream_id)
        session.mental_log = MentalLog(self._max_log_entries)
        self._sessions[stream_id] = session
        return session

    async def get(self, stream_id: str) -> BridgeSession | None:
        """获取缓存、本地磁盘或旧 NFC 磁盘中的会话。"""
        if stream_id in self._sessions:
            return self._sessions[stream_id]
        data = await self._store.load(stream_id)
        migrated = False
        if not isinstance(data, dict) and self._legacy_storage_path.is_dir():
            data = await self._legacy_store.load(stream_id)
            migrated = isinstance(data, dict)
        if not isinstance(data, dict):
            return None
        session = BridgeSession.from_dict(data, self._max_log_entries)
        session.stream_id = stream_id
        self._sessions[stream_id] = session
        if migrated:
            await self.save(session)
            logger.info(f"已迁移旧 NFC 会话: stream={stream_id[:8]}")
        return session

    async def peek(self, stream_id: str) -> BridgeSession | None:
        """读取会话；该轻量实现与 get 共享缓存。"""
        return await self.get(stream_id)

    async def save(self, session: BridgeSession) -> None:
        """把会话写入桥自己的数据目录。"""
        self._sessions[session.stream_id] = session
        await self._store.save(session.stream_id, session.to_dict())

    def get_all_cached(self) -> dict[str, BridgeSession]:
        """返回当前缓存的浅副本。"""
        return dict(self._sessions)

    async def list_all_stream_ids(self) -> list[str]:
        """列出桥自己的全部持久化聊天流。"""
        local_ids = await self._store.list_all()
        return sorted(
            stream_id for stream_id in local_ids if not stream_id.startswith("_")
        )

    async def migrate_legacy_sessions(self) -> int:
        """一次性导入尚未出现在桥目录的旧 NFC 会话。"""
        if not self._legacy_storage_path.is_dir():
            return 0
        migrated = 0
        local_ids = set(await self._store.list_all())
        for stream_id in await self._legacy_store.list_all():
            if stream_id.startswith("_") or stream_id in local_ids:
                continue
            data = await self._legacy_store.load(stream_id)
            if not isinstance(data, dict):
                continue
            session = BridgeSession.from_dict(data, self._max_log_entries)
            session.stream_id = stream_id
            await self.save(session)
            migrated += 1
        return migrated


__all__ = [
    "BridgeSession",
    "BridgeSessionStore",
    "MentalLog",
    "MentalLogEntry",
    "NFCEventType",
    "WaitingConfig",
]
