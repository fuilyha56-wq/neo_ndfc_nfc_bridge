"""NDFC-NFC bridge 插件的 LLM 请求体快照序列化与恢复工具。"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import Any

from src.kernel.llm import Audio, Image, LLMPayload, ReasoningText, Text, ToolCall, ToolResult, Video
from src.kernel.llm.payload.content import File
from src.kernel.llm.roles import ROLE

_RUNTIME_REMINDER_RE = re.compile(
	r"\s*<system_reminder>.*?</system_reminder>\s*",
	re.DOTALL,
)
_HISTORY_ROLES = {ROLE.USER.value, ROLE.ASSISTANT.value, ROLE.TOOL_RESULT.value}


@dataclass(slots=True)
class PayloadSnapshotEntry:
	"""一条可 JSON 序列化的 payload 快照记录。"""

	role: str
	parts: list[dict[str, Any]] = field(default_factory=list)

	def to_dict(self) -> dict[str, Any]:
		"""返回可持久化的字典表示。"""
		return {"role": self.role, "parts": self.parts}

	@classmethod
	def from_dict(cls, data: dict[str, Any]) -> "PayloadSnapshotEntry":
		"""从持久化字典恢复快照记录。"""
		raw_parts = data.get("parts", [])
		return cls(
			role=str(data.get("role", "") or ""),
			parts=[part for part in raw_parts if isinstance(part, dict)]
			if isinstance(raw_parts, list)
			else [],
		)


@dataclass(slots=True)
class PayloadSnapshot:
	"""一个聊天流最近一次实际发送给模型的 payload 快照。"""

	stream_id: str
	entries: list[PayloadSnapshotEntry] = field(default_factory=list)
	updated_at: float = field(default_factory=time.time)
	version: int = 1

	def to_dict(self) -> dict[str, Any]:
		"""返回可持久化的字典表示。"""
		return {
			"stream_id": self.stream_id,
			"entries": [entry.to_dict() for entry in self.entries],
			"updated_at": self.updated_at,
			"version": self.version,
		}

	@classmethod
	def from_dict(cls, data: dict[str, Any]) -> "PayloadSnapshot":
		"""从持久化字典恢复完整请求体快照。"""
		raw_entries = data.get("entries", [])
		return cls(
			stream_id=str(data.get("stream_id", "") or ""),
			entries=[
				PayloadSnapshotEntry.from_dict(entry)
				for entry in raw_entries
				if isinstance(entry, dict)
			]
			if isinstance(raw_entries, list)
			else [],
			updated_at=float(data.get("updated_at", time.time()) or time.time()),
			version=max(1, int(data.get("version", 1) or 1)),
		)


def capture_payload_snapshot(
	stream_id: str,
	payloads: list[LLMPayload],
) -> PayloadSnapshot | None:
	"""把最终请求 payload 链转换为可跨进程保存的快照。"""
	if not stream_id or not payloads:
		return None

	entries: list[PayloadSnapshotEntry] = []
	for payload in payloads:
		role = _role_value(getattr(payload, "role", None))
		if not role or role == ROLE.TOOL.value:
			continue
		parts = _serialize_parts(getattr(payload, "content", []), role)
		if parts:
			entries.append(PayloadSnapshotEntry(role=role, parts=parts))

	if not entries:
		return None
	return PayloadSnapshot(stream_id=stream_id, entries=entries)


def restore_payload_snapshot(snapshot: PayloadSnapshot) -> list[LLMPayload]:
	"""恢复可注入本次请求的合法对话历史 payload 链。"""
	restored: list[LLMPayload] = []
	pending_call_ids: set[str] = set()
	safe_length = 0
	current_turn_start = 0
	pending_rollback_length = 0

	for entry in snapshot.entries:
		role = str(entry.role or "")
		if role not in _HISTORY_ROLES:
			continue

		if role == ROLE.USER.value:
			if pending_call_ids:
				restored = restored[:pending_rollback_length]
				pending_call_ids.clear()
			current_turn_start = safe_length
			payload = _deserialize_entry(entry)
			if payload is None:
				continue
			restored.append(payload)
			continue

		if not restored:
			continue

		if role == ROLE.ASSISTANT.value:
			if pending_call_ids:
				restored = restored[:pending_rollback_length]
				pending_call_ids.clear()
			payload = _deserialize_entry(entry)
			if payload is None:
				continue
			restored.append(payload)
			pending_call_ids = _tool_call_ids(payload)
			if not pending_call_ids:
				safe_length = len(restored)
			else:
				pending_rollback_length = current_turn_start
			continue

		if not pending_call_ids:
			continue
		payload = _deserialize_tool_result_entry(entry, pending_call_ids)
		if payload is None:
			continue
		restored.append(payload)
		pending_call_ids.difference_update(_tool_result_call_ids(payload))
		if not pending_call_ids:
			safe_length = len(restored)

	if pending_call_ids:
		restored = restored[:pending_rollback_length]

	while restored and restored[0].role != ROLE.USER:
		restored.pop(0)
	return restored


def _serialize_parts(parts: Any, role: str) -> list[dict[str, Any]]:
	"""序列化一条 payload 的内容片段。"""
	values = parts if isinstance(parts, list) else [parts]
	serialized: list[dict[str, Any]] = []
	for part in values:
		if isinstance(part, Text):
			text = _strip_runtime_reminders(part.text) if role == ROLE.USER.value else part.text
			if text:
				serialized.append({"type": "text", "text": text})
		elif isinstance(part, ReasoningText):
			if part.text:
				serialized.append(
					{
						"type": "reasoning",
						"text": part.text,
						"signature": part.signature,
						"redacted_data": part.redacted_data,
					}
				)
		elif isinstance(part, ToolCall):
			serialized.append(
				{
					"type": "tool_call",
					"id": part.id,
					"name": part.name,
					"args": _json_compatible(part.args),
				}
			)
		elif isinstance(part, ToolResult):
			serialized.append(
				{
					"type": "tool_result",
					"value": _json_compatible(part.value),
					"call_id": part.call_id,
					"name": part.name,
				}
			)
		elif isinstance(part, Video):
			serialized.append(
				{
					"type": "video",
					"value": part.value,
					"mime_type": part.mime_type,
				}
			)
		elif isinstance(part, Image):
			serialized.append({"type": "image", "value": part.value})
		elif isinstance(part, Audio):
			serialized.append({"type": "audio", "value": part.value})
		elif isinstance(part, File):
			serialized.append({"type": "file", "value": part.value})
	return serialized


def _deserialize_entry(entry: PayloadSnapshotEntry) -> LLMPayload | None:
	"""把普通 user/assistant 快照记录恢复为 payload。"""
	role = _restore_role(entry.role)
	if role not in {ROLE.USER, ROLE.ASSISTANT}:
		return None
	parts = _deserialize_parts(entry.parts)
	return LLMPayload(role, parts) if parts else None  # type: ignore[arg-type]


def _deserialize_tool_result_entry(
	entry: PayloadSnapshotEntry,
	pending_call_ids: set[str],
) -> LLMPayload | None:
	"""恢复与未闭合工具调用匹配的工具结果。"""
	matched_parts = [
		part
		for part in _deserialize_parts(entry.parts)
		if isinstance(part, ToolResult)
		and str(part.call_id or "") in pending_call_ids
	]
	return (
		LLMPayload(ROLE.TOOL_RESULT, matched_parts)  # type: ignore[arg-type]
		if matched_parts
		else None
	)


def _deserialize_parts(parts: list[dict[str, Any]]) -> list[Any]:
	"""从快照片段恢复框架内容对象。"""
	restored: list[Any] = []
	for part in parts:
		part_type = str(part.get("type", "") or "")
		try:
			if part_type == "text":
				text = str(part.get("text", "") or "")
				if text:
					restored.append(Text(text))
			elif part_type == "reasoning":
				text = str(part.get("text", "") or "")
				if text:
					restored.append(
						ReasoningText(
							text=text,
							signature=_optional_string(part.get("signature")),
							redacted_data=_optional_string(part.get("redacted_data")),
						)
					)
			elif part_type == "tool_call":
				name = str(part.get("name", "") or "")
				if name:
					restored.append(
						ToolCall(
							id=_optional_string(part.get("id")),
							name=name,
							args=part.get("args", {}),
						)
					)
			elif part_type == "tool_result":
				restored.append(
					ToolResult(
						value=part.get("value"),
						call_id=_optional_string(part.get("call_id")),
						name=_optional_string(part.get("name")),
					)
				)
			elif part_type == "image":
				value = str(part.get("value", "") or "")
				if value:
					restored.append(Image(value))
			elif part_type == "audio":
				value = str(part.get("value", "") or "")
				if value:
					restored.append(Audio(value))
			elif part_type == "video":
				value = str(part.get("value", "") or "")
				if value:
					restored.append(
						Video(
							value,
							mime_type=str(part.get("mime_type", "video/mp4") or "video/mp4"),
						)
					)
			elif part_type == "file":
				value = str(part.get("value", "") or "")
				if value:
					restored.append(File(value))
		except (TypeError, ValueError):
			continue
	return restored


def _tool_call_ids(payload: LLMPayload) -> set[str]:
	"""返回 assistant payload 中全部有效工具调用 ID。"""
	return {
		str(part.id)
		for part in payload.content
		if isinstance(part, ToolCall) and part.id is not None and str(part.id)
	}


def _tool_result_call_ids(payload: LLMPayload) -> set[str]:
	"""返回工具结果 payload 中全部有效调用 ID。"""
	return {
		str(part.call_id)
		for part in payload.content
		if isinstance(part, ToolResult)
		and part.call_id is not None
		and str(part.call_id)
	}


def _role_value(role: Any) -> str:
	"""规范化 ROLE 枚举或字符串。"""
	return str(getattr(role, "value", role) or "")


def _restore_role(role: str) -> ROLE | None:
	"""把快照角色字符串转换为当前 ROLE。"""
	try:
		return ROLE(role)
	except ValueError:
		return None


def _strip_runtime_reminders(text: str) -> str:
	"""移除每次发送都会重新生成的 system reminder。"""
	cleaned = _RUNTIME_REMINDER_RE.sub("", text)
	return cleaned.strip()


def _json_compatible(value: Any) -> Any:
	"""把工具参数或结果转换为 JSON 可保存的数据。"""
	try:
		return json.loads(json.dumps(value, ensure_ascii=False, default=str))
	except (TypeError, ValueError):
		return str(value)


def _optional_string(value: Any) -> str | None:
	"""把空值恢复为 None，其他值恢复为字符串。"""
	if value is None:
		return None
	text = str(value)
	return text if text else None


__all__ = [
	"PayloadSnapshot",
	"PayloadSnapshotEntry",
	"capture_payload_snapshot",
	"restore_payload_snapshot",
]