# Neo NDFC-NFC Bridge

`neo_ndfc_nfc_bridge` 让 `neo_default_chatter` 直接拥有 NFC 风格的连续对话能力：
人格化系统提示词、心理活动流、消息与内心时间线、原生多模态、等待与超时、
情绪和用户习惯、分段回复，以及可实际触发 NDFC 的主动发起预约。

桥不会加载或调用 `neo_fatum_chatter`，也不会注册第二个 Chatter。
NDFC 始终是唯一的会话状态机和消息消费者。

## 依赖

只需要启用：

- `neo_default_chatter`
- `neo_ndfc_nfc_bridge`

不需要安装或启用 `neo_fatum_chatter`。

## 工作方式

桥通过高权重 Handler 订阅 NDFC 的全部 `NdfcEvent`，在以下切面提供内置能力：

| NDFC 切面 | 桥内行为 |
| --- | --- |
| `format_unread_line` | 生成带时间、发送者和消息 ID 的叙事格式 |
| `build_history_text` | 融合聊天记录与桥内心理活动 |
| `inject_unread_payload` | 提取未读消息中的图片和表情包，构造多模态 payload |
| `create_request` | 使用桥配置的模型任务 |
| `preprocess` | 注入人格、自我认知、心理状态、习惯和工具协议 |
| `run_tool_call` | 记录 thought、mood、等待意图和实际动作 |
| `build_resume_prompt` | 等待超时后引导模型决定追问、继续等待或结束 |
| `compute_cooldown` | 应用等待上下限和连续超时规则 |
| `compute_stop_wake` | 有效等待期间抑制提前唤醒 |
| `session_transition` | 持久化会话活跃时间 |

未覆盖的消息获取、flush、去重和工具执行仍由 NDFC 默认实现负责，避免重复消费。

## Actions

桥向 `neo_default_chatter` 注册六个本地 Action：

- `nfc_reply`：清洗并分段发送文本。
- `do_nothing`：选择不回复或继续等待。
- `update_mood_state`：持久化当前情绪。
- `record_user_habit`：记录有对话证据支持的用户习惯。
- `query_activity_pattern`：查询桥累计的活跃小时分布。
- `schedule_proactive`：设置、覆盖或取消主动发起预约。

`schedule_proactive` 不只是保存时间戳。桥自己的后台调度器会在预约到期时通过
`ChatterManager.resume_chatter()` 唤醒目标 NDFC 流，把主动上下文交给 NDFC 决策，
再由模型选择 `nfc_reply` 或 `do_nothing`。

## 数据

桥只写入自己的目录：

```text
data/neo_ndfc_nfc_bridge/sessions/
```

首次加载时，如果本机存在旧的 `data/neo_fatum_chatter/sessions/`，桥会把等待、
心理活动、情绪、习惯、预约和活跃时段复制到自己的目录。旧目录只是可选迁移来源；
迁移后 NFC 插件和旧目录都不参与桥的运行。

## 配置

主要开关位于 `bridge`：

| 配置项 | 默认值 | 作用 |
| --- | --- | --- |
| `enabled` | `true` | 启用桥 |
| `private_only` | `true` | 只对真实私聊流生效 |
| `use_nfc_system_prompt` | `true` | 注入连续人格与心理上下文 |
| `use_nfc_history` | `true` | 使用聊天与内心活动融合历史 |
| `use_nfc_message_format` | `true` | 使用叙事式未读消息格式 |
| `use_nfc_multimodal` | `true` | 直接把图片交给主模型 |
| `expose_nfc_actions` | `true` | 注册六个本地 Action |
| `use_nfc_waiting` | `true` | 启用等待、超时和提前唤醒规则 |
| `persist_mental_state` | `true` | 持久化心理、情绪、习惯和预约 |

其他配置节：

- `model`：模型任务、每轮图片上限和额外决策指导。
- `wait`：等待上下限、连续超时次数和提前唤醒规则。
- `reply`：分段消息发送间隔。
- `prompt`：心理日志条目上限和近期记忆开关。
- `proactive`：检查间隔、沉默阈值、触发概率、最小间隔和勿扰时段。

预约触发不受勿扰时段限制；概率性的沉默触发会遵守勿扰时段、最小间隔和已观察到的
用户活跃规律。目标聊天流已经绑定其他 Chatter 时，桥不会覆盖该绑定。

## 验证

在 Neo-MoFox 项目根目录执行：

```powershell
uv run python -c "import plugins.neo_ndfc_nfc_bridge.plugin"
uv run ruff check plugins/neo-ndfc-nfc-bridge
```

插件加载日志应出现 `NDFC-NFC 桥已就绪`，且无需安装 `neo_fatum_chatter`。