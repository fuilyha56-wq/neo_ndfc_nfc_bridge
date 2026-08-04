# Neo NDFC-NFC Bridge

`neo_ndfc_nfc_bridge` 让 `neo_default_chatter` 保持唯一会话状态机，同时复用
`neo_fatum_chatter` 的私聊提示词、心理活动流、消息格式、多模态、等待规则和 Actions。
插件不会启动第二个 Chatter，也不会重复消费或 flush 消息。

## 依赖

- `neo_default_chatter`
- `neo_fatum_chatter`

三个插件必须同时启用。桥接插件加载时会校验依赖，并直接复用 NFC 的
`NFCSessionStore`，因此心理活动、等待状态、习惯和主动发起预约与 NFC 使用同一份数据。

## 桥接能力

| NDFC 切面 | 桥接行为 |
| --- | --- |
| `format_unread_line` | 使用 NFC 的账号、消息 ID 和时间标签格式 |
| `build_history_text` | 使用 NFC 的历史消息与 MentalLog 融合叙事 |
| `inject_unread_payload` | 使用 NFC 的图片提取和原生多模态 payload |
| `create_request` | 使用 NFC 配置的 `model_task` |
| `preprocess` | 注入 NFC system prompt、近期记忆和动态关系上下文 |
| `run_tool_call` | 在默认工具执行后记录 thought、mood、等待意图和心理活动 |
| `build_resume_prompt` | 构造 NFC 风格的等待超时心理提示 |
| `compute_cooldown` | 使用 NFC 等待上下限和连续超时规则规整冷却时间 |
| `compute_stop_wake` | 有效等待期间按 NFC 规则抑制提前唤醒 |
| `session_transition` | 更新并持久化 NFC 会话活跃时间 |

桥 handler 订阅全部 17 个 `NdfcEvent`。`fetch_unreads`、`flush_unreads`、
`pick_trigger_message`、`dedupe_tool_call`、`format_tool_result` 和
`build_negative_extra` 保留 NDFC 默认实现，避免重复消费、重复去重或覆盖 NDFC 控制流。

## Actions

桥接插件向 `neo_default_chatter` 提供以下 NFC Actions：

- `nfc_reply`
- `do_nothing`
- `schedule_proactive`
- `nfc_query_activity_pattern`
- `nfc_record_habit`
- `nfc_query_habits`

`schedule_proactive` 会直接写入共享 NFC Session，而不是只返回提示文本。所有 Action
都遵守 `enabled`、`expose_nfc_actions` 和 `private_only`，会在每个聊天流上动态判定。

## 配置

| 配置项 | 默认值 | 作用 |
| --- | --- | --- |
| `enabled` | `true` | 启用整个桥接插件 |
| `private_only` | `true` | 仅向真实私聊流注入 NFC 行为 |
| `use_nfc_system_prompt` | `true` | 注入 NFC system prompt 和动态关系上下文 |
| `use_nfc_history` | `true` | 使用 NFC 心理活动融合历史 |
| `use_nfc_message_format` | `true` | 使用 NFC 未读消息格式 |
| `use_nfc_multimodal` | `true` | 使用 NFC 多模态 payload 构建 |
| `expose_nfc_actions` | `true` | 注册并暴露 NFC Actions |
| `use_nfc_waiting` | `true` | 桥接等待、超时、冷却和提前唤醒规则 |
| `persist_mental_state` | `true` | 写入 MentalLog、mood、waiting 和会话活跃状态 |

`private_only=true` 时，早期事件若没有携带聊天类型，插件会通过 `stream_id` 查询
已存在的真实聊天流。流不存在或类型未知时不会桥接，避免将私聊行为误注入群聊。

## 验证

在项目根目录执行：

```powershell
uv run pytest test/plugins/neo_ndfc_nfc_bridge/test_bridge_handler.py -q --no-cov
uv run python examples/plugins/neo_ndfc_nfc_bridge_example.py
```
