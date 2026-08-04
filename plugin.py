"""Neo NDFC-NFC Bridge 插件入口。"""

from __future__ import annotations

from typing import cast

from src.app.plugin_system.api import plugin_api
from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.base import BasePlugin, register_plugin

from .actions import BRIDGE_NFC_ACTIONS
from .config import NdfcNfcBridgeConfig
from .handlers import NdfcNfcBridgeHandler, NdfcNfcBridgeObserver

logger = get_logger("neo_ndfc_nfc_bridge", display="Neo NDFC-NFC Bridge")


@register_plugin
class NdfcNfcBridgePlugin(BasePlugin):
    """把 NeoFatumChatter 的能力注入 Neo-Default-Chatter 执行链。"""

    plugin_name = "neo_ndfc_nfc_bridge"
    plugin_version = "0.1.0"
    plugin_author = "MoFox Team"
    plugin_description = "让 NDFC 复用 NFC 的提示词、心理活动、记忆与动作能力"
    configs = [NdfcNfcBridgeConfig]

    ndfc_plugin: BasePlugin
    nfc_plugin: BasePlugin

    @property
    def session_store(self):
        """返回 NFC 插件持有的共享会话存储。"""
        return self.nfc_plugin.session_store

    async def on_plugin_loaded(self) -> None:
        """校验桥接目标，并确保 NFC 提示词模板已经注册。"""
        config = cast(NdfcNfcBridgeConfig, self.config)
        if not config.bridge.enabled:
            logger.info("桥接已在配置中关闭")
            return

        ndfc_plugin = plugin_api.get_plugin("neo_default_chatter")
        nfc_plugin = plugin_api.get_plugin("neo_fatum_chatter")
        if ndfc_plugin is None or nfc_plugin is None:
            missing = []
            if ndfc_plugin is None:
                missing.append("neo_default_chatter")
            if nfc_plugin is None:
                missing.append("neo_fatum_chatter")
            raise RuntimeError(f"桥接依赖未加载: {', '.join(missing)}")

        self.ndfc_plugin = ndfc_plugin
        self.nfc_plugin = nfc_plugin

        from plugins.neo_fatum_chatter.prompts.modules import register_nfc_prompts

        register_nfc_prompts()
        logger.info("NDFC-NFC 桥接依赖检查完成")

    def get_components(self) -> list[type]:
        """返回桥接事件处理器与 NDFC 兼容的 NFC Actions。"""
        config = cast(NdfcNfcBridgeConfig, self.config)
        if not config.bridge.enabled:
            return []
        components: list[type] = [NdfcNfcBridgeHandler, NdfcNfcBridgeObserver]
        if config.bridge.expose_nfc_actions:
            components.extend(BRIDGE_NFC_ACTIONS)
        return components


__all__ = ["NdfcNfcBridgePlugin"]
