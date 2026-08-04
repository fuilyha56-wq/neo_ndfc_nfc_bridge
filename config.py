"""Neo NDFC-NFC Bridge 配置定义。"""

from __future__ import annotations

from typing import ClassVar

from src.app.plugin_system.base import BaseConfig, Field, SectionBase, config_section


class NdfcNfcBridgeConfig(BaseConfig):
    """控制 NFC 能力注入 NDFC 的范围。"""

    name: ClassVar[str] = "config"
    description: ClassVar[str] = "Neo NDFC-NFC Bridge 配置"

    @config_section("bridge", title="桥接设置", tag="plugin")
    class BridgeSection(SectionBase):
        """桥接总开关与能力开关。"""

        enabled: bool = Field(
            default=True,
            description="启用 NDFC 到 NFC 的能力桥接。",
            label="启用桥接",
            tag="plugin",
        )
        private_only: bool = Field(
            default=True,
            description="仅在私聊中注入 NFC 的私聊特化行为。",
            label="仅私聊启用",
            tag="plugin",
        )
        use_nfc_system_prompt: bool = Field(
            default=True,
            description="使用 NFC 系统提示词与关系上下文替换 NDFC 系统提示词。",
            label="桥接 NFC 系统提示词",
            tag="ai",
        )
        use_nfc_history: bool = Field(
            default=True,
            description="使用 NFC 的聊天历史与心理活动融合叙事。",
            label="桥接心理活动历史",
            tag="ai",
        )
        use_nfc_message_format: bool = Field(
            default=True,
            description="使用 NFC 的消息标签、账号与消息 ID 格式。",
            label="桥接消息格式",
            tag="ai",
        )
        use_nfc_multimodal: bool = Field(
            default=True,
            description="使用 NFC 的图片提取与多模态 payload 构建。",
            label="桥接 NFC 多模态",
            tag="ai",
        )
        expose_nfc_actions: bool = Field(
            default=True,
            description="向 NDFC 模型暴露 NFC 回复、等待、主动发起与记忆工具。",
            label="桥接 NFC Actions",
            tag="ai",
        )
        use_nfc_waiting: bool = Field(
            default=True,
            description="将 NFC 的等待规则用于 NDFC 的冷却与唤醒计算。",
            label="桥接等待规则",
            tag="ai",
        )
        persist_mental_state: bool = Field(
            default=True,
            description="在 NDFC 会话阶段变化时维护并持久化 NFC 会话状态。",
            label="持久化心理状态",
            tag="memory",
        )

    bridge: BridgeSection = Field(default_factory=BridgeSection)


__all__ = ["NdfcNfcBridgeConfig"]
