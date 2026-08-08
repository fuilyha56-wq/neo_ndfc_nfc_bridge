"""Neo NDFC-NFC Bridge 配置定义。"""

from __future__ import annotations

from typing import ClassVar

from src.app.plugin_system.base import BaseConfig, Field, SectionBase, config_section


class NdfcNfcBridgeConfig(BaseConfig):
    """控制内置 NFC 能力注入 NDFC 的范围。"""

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
            description="在 NDFC 会话阶段变化时维护并持久化内置心理会话状态。",
            label="持久化心理状态",
            tag="memory",
        )

    bridge: BridgeSection = Field(default_factory=BridgeSection)

    @config_section("model", title="模型与多模态", tag="ai")
    class ModelSection(SectionBase):
        """NDFC 请求和原生多模态参数。"""

        model_task: str = Field(
            default="actor",
            description="NDFC 使用的模型任务名称。",
            label="模型任务",
            tag="ai",
        )
        max_images_per_payload: int = Field(
            default=4,
            description="每轮最多直接交给主模型的图片数量。",
            label="图片上限",
            tag="ai",
        )
        custom_decision_prompt: str = Field(
            default="",
            description="追加到内置 NFC 行为提示词的决策指导。",
            label="自定义决策指导",
            tag="ai",
        )

    model: ModelSection = Field(default_factory=ModelSection)

    @config_section("wait", title="等待规则", tag="performance")
    class WaitSection(SectionBase):
        """等待、冷却和提前唤醒规则。"""

        enabled: bool = Field(default=True, description="启用回复等待。")
        min_seconds: float = Field(default=10.0, description="最小等待秒数。")
        max_seconds: float = Field(default=600.0, description="最大等待秒数。")
        max_consecutive_timeouts: int = Field(
            default=3,
            description="连续超时达到该次数后不再等待。",
        )
        suppress_early_wake: bool = Field(
            default=True,
            description="有效等待期间禁止新消息提前唤醒。",
        )

        def apply_rules(self, raw_seconds: float, consecutive_timeouts: int) -> float:
            """把模型给出的等待秒数规整到有效范围。"""
            if not self.enabled or raw_seconds <= 0:
                return 0.0
            if consecutive_timeouts >= max(0, self.max_consecutive_timeouts):
                return 0.0
            lower = max(0.0, float(self.min_seconds))
            upper = max(lower, float(self.max_seconds))
            return max(lower, min(float(raw_seconds), upper))

    wait: WaitSection = Field(default_factory=WaitSection)

    @config_section("reply", title="回复节奏", tag="performance")
    class ReplySection(SectionBase):
        """分段回复的发送节奏。"""

        segment_delay_min: float = Field(default=0.5, description="分段最小间隔。")
        segment_delay_max: float = Field(default=2.0, description="分段最大间隔。")

    reply: ReplySection = Field(default_factory=ReplySection)

    @config_section("prompt", title="心理上下文", tag="memory")
    class PromptSection(SectionBase):
        """心理日志与近期记忆设置。"""

        max_log_entries: int = Field(default=50, description="最大心理活动条目数。")
        request_snapshot_enabled: bool = Field(
            default=True,
            description="保存完整请求体，并在重启后的首个私聊请求恢复。",
        )
        summary_enabled: bool = Field(
            default=True, description="注入已有的近期记忆摘要。"
        )
        compress_every_n_rounds: int = Field(
            default=50,
            description="每完成 N 次实际回复触发一次近期记忆压缩。",
        )
        compress_days_window: float = Field(
            default=3.0,
            description="生成摘要时覆盖的近期消息时间窗口（天）。",
        )
        min_compress_interval_minutes: float = Field(
            default=120.0,
            description="两次近期记忆压缩之间的最短间隔（分钟）。",
        )

    prompt: PromptSection = Field(default_factory=PromptSection)

    @config_section("proactive", title="主动发起", tag="plugin")
    class ProactiveSection(SectionBase):
        """预约与沉默触发规则。"""

        enabled: bool = Field(default=True, description="启用主动发起检查。")
        check_interval: int = Field(default=60, description="检查间隔秒数。")
        silence_threshold: int = Field(default=7200, description="沉默触发阈值秒数。")
        trigger_probability: float = Field(default=0.3, description="沉默触发概率。")
        min_interval: int = Field(default=1800, description="两次主动发起最小间隔。")
        quiet_hours_start: str = Field(default="23:00", description="勿扰开始时间。")
        quiet_hours_end: str = Field(default="07:00", description="勿扰结束时间。")

    proactive: ProactiveSection = Field(default_factory=ProactiveSection)


__all__ = ["NdfcNfcBridgeConfig"]
