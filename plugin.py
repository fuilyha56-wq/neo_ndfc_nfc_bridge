"""Neo NDFC-NFC Bridge 插件入口。"""

from __future__ import annotations

from typing import cast

from src.app.plugin_system.api import plugin_api
from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.base import BasePlugin, register_plugin

from .actions import BRIDGE_NFC_ACTIONS
from .config import NdfcNfcBridgeConfig
from .handlers import NdfcNfcBridgeHandler, NdfcNfcBridgeObserver
from .proactive import ProactiveScheduler
from .state import BridgeSessionStore

logger = get_logger("neo_ndfc_nfc_bridge", display="Neo NDFC-NFC Bridge")


@register_plugin
class NdfcNfcBridgePlugin(BasePlugin):
    """为 Neo-Default-Chatter 内置 NFC 风格的连续对话能力。"""

    plugin_name = "neo_ndfc_nfc_bridge"
    plugin_version = "0.1.1"
    plugin_author = "MoFox Team"
    plugin_description = "让 NDFC 独立拥有心理活动、记忆、等待与主动发起能力"
    configs = [NdfcNfcBridgeConfig]

    ndfc_plugin: BasePlugin
    _session_store: BridgeSessionStore | None = None
    _proactive_scheduler: ProactiveScheduler | None = None

    @property
    def session_store(self) -> BridgeSessionStore:
        """返回桥自己持有的会话存储。"""
        if self._session_store is None:
            raise RuntimeError("桥会话存储尚未初始化")
        return self._session_store

    async def on_plugin_loaded(self) -> None:
        """校验 NDFC、初始化独立存储并启动主动发起任务。"""
        config = cast(NdfcNfcBridgeConfig, self.config)
        if not config.bridge.enabled:
            logger.info("桥接已在配置中关闭")
            return

        ndfc_plugin = plugin_api.get_plugin("neo_default_chatter")
        if ndfc_plugin is None:
            raise RuntimeError("桥接依赖未加载: neo_default_chatter")

        self.ndfc_plugin = ndfc_plugin
        self._session_store = BridgeSessionStore(
            max_log_entries=int(config.prompt.max_log_entries)
        )
        migrated = await self._session_store.migrate_legacy_sessions()
        self._proactive_scheduler = ProactiveScheduler(self)
        self._proactive_scheduler.start()
        logger.info(f"NDFC-NFC 桥已就绪，迁移旧会话 {migrated} 个")

    async def on_plugin_unloaded(self) -> None:
        """停止桥自己的主动发起后台任务。"""
        if self._proactive_scheduler is not None:
            await self._proactive_scheduler.stop()
            self._proactive_scheduler = None

    def get_components(self) -> list[type]:
        """返回桥接事件处理器与 NDFC 兼容的内置 Actions。"""
        config = cast(NdfcNfcBridgeConfig, self.config)
        if not config.bridge.enabled:
            return []
        components: list[type] = [NdfcNfcBridgeHandler, NdfcNfcBridgeObserver]
        if config.bridge.expose_nfc_actions:
            components.extend(BRIDGE_NFC_ACTIONS)
        return components


__all__ = ["NdfcNfcBridgePlugin"]
