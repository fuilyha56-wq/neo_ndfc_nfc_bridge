# Neo NDFC-NFC Bridge

`neo_ndfc_nfc_bridge` 是 `neo_default_chatter` 的连续对话能力扩展。它在不启动第二个
Chatter 的前提下，为 NDFC 提供人格上下文、心理活动、原生多模态、等待与超时、
分段回复、用户习惯和主动发起等能力。

> 当前版本：`0.1.3`

## 设计原则

- **NDFC 是唯一 Chatter**：消息消费、工具执行和状态机推进仍由
	`neo_default_chatter` 负责。
- **不依赖 NFC 插件**：运行时不会导入、加载或调用 `neo_fatum_chatter`。
- **状态独立持久化**：桥拥有自己的会话模型和数据目录。
- **按事件扩展**：能力通过 NDFC 的公开事件切面注入，不接管 NDFC 的核心流程。

## 安装

将插件目录放入 Neo-MoFox 的 `plugins` 目录：

```text
plugins/neo-ndfc-nfc-bridge/
```

启用以下两个插件即可：

1. `neo_default_chatter`
2. `neo_ndfc_nfc_bridge`

`neo_fatum_chatter` 不是依赖，无需安装或启用。

## 提供的能力

| 能力 | 说明 |
| --- | --- |
| 人格提示词 | 从核心人格配置构建身份、性格、背景、回复风格和安全边界 |
| 心理活动 | 记录用户消息、回复计划、情绪变化、等待超时和主动触发 |
| 融合历史 | 将聊天记录与心理活动整理成连续叙事交给模型 |
| 多模态 | 从未读消息中提取图片和表情包，构建原生多模态输入 |
| 等待机制 | 约束等待时长、连续超时次数，并控制等待期间的提前唤醒 |
| 分段回复 | 清理模型输出后按配置间隔发送多个消息段 |
| 用户习惯 | 持久化有聊天证据支持的用户习惯及活跃时间分布 |
| 主动发起 | 支持模型预约和沉默概率触发，并唤醒目标 NDFC 会话 |

桥会处理 NDFC 的格式化、历史构建、请求创建、预处理、工具观察、冷却和恢复提示等
事件。未读消息获取、消息清空、去重和实际工具调度继续使用 NDFC 默认实现，因此不会
发生重复消费。

## Actions

插件向 `neo_default_chatter` 注册以下本地 Action：

| Action | 用途 |
| --- | --- |
| `nfc_reply` | 清理并分段发送回复内容 |
| `do_nothing` | 当前不回复，或按参数继续等待 |
| `update_mood_state` | 更新并保存当前情绪状态 |
| `record_user_habit` | 保存有对话依据的用户习惯 |
| `query_activity_pattern` | 查询用户活跃小时分布 |
| `schedule_proactive` | 设置、覆盖或取消主动发起预约 |

`schedule_proactive` 设置的预约由桥内调度器执行。预约到期后，调度器会通过
NDFC 的 `chat_core` Service 为目标流创建独立会话，再由模型决定调用 `nfc_reply`
还是 `do_nothing`。该过程不查询、替换或恢复全局 Chatter 绑定，因此同一聊天流上
由其他适配器使用的 Chatter 可以继续运行。

## 配置

### `bridge`

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `enabled` | `true` | 启用桥接插件 |
| `private_only` | `true` | 仅对真实私聊流注入能力 |
| `use_nfc_system_prompt` | `true` | 注入人格和心理上下文 |
| `use_nfc_history` | `true` | 使用聊天与心理活动融合历史 |
| `use_nfc_message_format` | `true` | 使用带时间、发送者和消息 ID 的格式 |
| `use_nfc_multimodal` | `true` | 将图片和表情包加入模型输入 |
| `expose_nfc_actions` | `true` | 向 NDFC 注册六个本地 Action |
| `use_nfc_waiting` | `true` | 启用等待、超时和提前唤醒规则 |
| `persist_mental_state` | `true` | 持久化心理活动、情绪、习惯和预约 |

### 其他配置节

| 配置节 | 说明 |
| --- | --- |
| `model` | 模型任务、每轮图片上限、自定义决策指导 |
| `wait` | 等待开关、时长上下限、连续超时次数、提前唤醒规则 |
| `reply` | 分段消息的最小和最大发送间隔 |
| `prompt` | 心理日志条目上限、近期记忆摘要开关 |
| `proactive` | 调度检查间隔、沉默阈值、概率、最小间隔和勿扰时段 |

预约触发不受勿扰时段限制；沉默概率触发会遵守勿扰时段、最小间隔和已记录的用户
活跃规律。

## 数据目录与旧数据迁移

桥的会话数据保存在：

```text
data/neo_ndfc_nfc_bridge/sessions/
```

如果首次加载时检测到以下旧目录：

```text
data/neo_fatum_chatter/sessions/
```

桥会将兼容的心理活动、等待、情绪、习惯、预约和活跃时段复制到自己的目录。旧目录
只是一次性的可选数据来源；目录不存在时不会被创建，迁移后也不会参与运行。

## 验证

在 Neo-MoFox 项目根目录执行：

```powershell
uv run python -c "import plugins.neo_ndfc_nfc_bridge.plugin"
uv run ruff check plugins/neo-ndfc-nfc-bridge
uv run ruff format --check plugins/neo-ndfc-nfc-bridge
```

插件正常加载后，日志会出现 `NDFC-NFC 桥已就绪`。整个运行过程不需要安装
`neo_fatum_chatter`。
