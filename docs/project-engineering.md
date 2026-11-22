# Pero 工程图

本文按当前代码划分组件职责，配合 README 总图阅读。箭头表示逻辑数据流，不代表每一阶段都会调用模型或把全部数据装入上下文。

## 1. 动态上下文与记忆

```mermaid
flowchart TB
    INPUT["接入消息"] --> ACCEPT["身份／授权／会话路由校验"]
    ACCEPT --> RAW["持久原始聊天库<br/>接收后保存，不以唤醒为条件"]
    RAW --> L1["L1：最近 10 条原始聊天"]
    RAW --> L10["L10：最近 20 条原始聊天"]
    ACCEPT --> POLICY["参与策略<br/>唤醒 · 活动窗口 · 私聊策略"]
    L10 -->|"需要模型判断时"| JUDGE["参与判断机"]
    POLICY -->|"需要判断"| JUDGE
    POLICY -->|"策略允许直答"| BUILD["本轮上下文装配"]
    JUDGE -->|"JOIN"| BUILD
    JUDGE -->|"PASS"| SILENT["保留原文，不生成正式回复"]
    RAW -->|"实际交互来源引用"| L2["L2：独立交互记忆<br/>用户输入 + 确认发送的 Agent 回复"]
    L2 -->|"未归档交互"| BUILD
    L2 -->|"达到条数／字符条件"| ARCHIVE["后台归档<br/>按完整轮次选择旧交互"]
    ARCHIVE -->|"成功后标记已归档，保留近期交互"| L2
    ARCHIVE --> L4["L4：长期事项与归档摘要"]
    L4 -->|"最新归档前情／相关检索"| BUILD
    L3["L3：有来源的人物事实"] -->|"检索相关人物"| BUILD
    DIARY["事件日记"] -->|"按人物／话题／时间检索"| BUILD
    KB["独立知识库"] -->|"配置启用时自动检索，或由工具检索"| BUILD
    L10 -->|"近期环境参考，排除与交互重复项"| BUILD
    PERSONA["本会话人格与配置"] --> BUILD
    BUILD --> MODEL["正式回复与工具续答"]
    MODEL --> DELIVERY["输出投递"]
    DELIVERY -->|"确认 sent"| RAW
    DELIVERY -->|"确认 sent"| L2
    ACTIVITY["活动期后台整理／明确保存"] --> L3
    ACTIVITY --> L4
    ACTIVITY --> DIARY
    RAW -->|"有来源的筛选与整理"| ACTIVITY
```

- L1、L10 是同一份持久原始聊天的不同读取窗口，不逐级搬运。图中 L1 表示可用的短期窗口，不宣称当前判断机每次都同时输入 L1 和 L10。
- 正式对话优先使用 L2 交互、最新归档前情与必要检索。当前默认 L2 以 80 条触发、约 30 条保留为策略，字符预算及完整轮次边界也会影响实际批次；不是固定每轮读取所有层。
- L3 普通聊天后台筛选受活动期和频率配置约束；明确保存与日记/事件整理的具体写入路径各自独立。图将这些后台写入概括为一个区域，不表示每句话都写入三层。
- 原始聊天不因归档删除。L4 摘要、人物事实、知识文档与原件具有各自的存储语义。

## 2. 工具与后台任务

```mermaid
flowchart TB
    MODEL["模型返回工具调用"] --> REG["核对本轮工具定义与参数"]
    REG --> EXEC["工具执行器<br/>当前 Agent + 会话作用域"]
    EXEC --> MODE{"后台工具？"}
    MODE -->|"否"| RUN["执行工具／SDK 插件"]
    RUN --> RESULT["本轮结果与实际挂载资源"]
    RESULT --> CONTINUE["模型工具续答"]
    MODE -->|"是"| SUBMIT["创建真实后台任务<br/>返回 queued + job_id"]
    SUBMIT --> ACK["本轮确认已受理<br/>结束后允许继续对话"]
    SUBMIT --> WORK["独立后台执行<br/>不占用整个会话处理期间"]
    WORK --> CHECK["完成时重新核对权限与路由"]
    CHECK --> ANALYSIS["原会话内串行结果分析<br/>本次独立图片作用域"]
    ANALYSIS --> REPORT["完成回报投递"]
    EXEC --> EVIDENCE["L20：受控工具证据"]
    REPORT --> EVIDENCE
    REPORT -->|"确认 sent"| CHAT["原始聊天与 L2"]
    CONTINUE --> SEND["普通回复投递与确认"]
    SEND --> CHAT
    EVIDENCE --> UI["控制室：对应回复下展开工具记录"]
    WORK --> STATUS["任务状态列表<br/>处理与投递状态分开"]
    REPORT --> STATUS
    STATUS --> UI
```

后台任务不要求插件另建模型或发送通道。宿主负责结果分析和原会话回报；外部插件保留独立业务实现。缓存命中仍返回真实结果及本轮资源，不跳过完成回报。清理任务列表不删除 L20、聊天、记忆或视频缓存。

同会话的正式回复与后台完成分析串行，长时间业务执行在会话锁之外。`drain` 拒绝新工作并等待已有任务收尾；真实撤权、插件停用或路由变化仍可阻止回报。

## 3. 消息平台与设备

```mermaid
flowchart LR
    WX["微信"] <--> WEBOT["webot"]
    WEBOT <--> OB["现有 OneBot v11 适配器"]
    QQ["QQ／其它消息平台<br/>待联调"] -.-> BRIDGE["兼容桥／新增适配"]
    BRIDGE -.-> OB
    ESP["ESP32／桌面伴侣<br/>待开发设备端"] -.-> GW["设备网关<br/>协议转换 · ASR／TTS · 展示与执行回执"]
    GW -.-> OB
    OB <--> CORE["Pero<br/>对话 · 上下文 · 记忆 · 知识库 · 工具"]
    APP["自有网页／应用"] --> HTTP["HTTP 会话接口"]
    HTTP <--> CORE
```

虚线表示接入方向，不表示已有 QQ 联调结果、ESP32 固件、语音管线或 MQTT 适配器。设备端不需承载完整模型与记忆库；可复用的核心能力运行在电脑或服务器上。具体身份、路由、授权、动作和回执要求见 [接入契约](integration-contract.md)。

## 4. 代码导航

| 区域 | 主要源码 |
| --- | --- |
| 装配与配置 | `src/pero/runtime.py`、`src/pero/core/settings.py` |
| 会话编排与参与策略 | `src/pero/agent/orchestrator.py`、`policy.py`、`private_judge.py` |
| 动态历史与模型循环 | `src/pero/agent/working_history.py`、`loop.py` |
| 模型请求与凭据 | `src/pero/providers/deepseek/`、`src/pero/configuration/credentials.py` |
| 原始聊天、L2 与归档 | `src/pero/memory/chat_stream.py`、`chat_memory.py`、`repository.py` |
| 知识库与本地嵌入接入 | `src/pero/knowledge/` |
| 工具与公共插件契约 | `src/pero/tools/`、`src/pero/plugins/`、`src/pero_plugin_sdk/` |
| 后台任务完成回报 | `src/pero/agent/plugin_jobs.py`、`src/pero/plugins/jobs.py` |
| 消息桥 | `src/pero/channels/onebot11/` |
| 图片、文件和表情资源 | `src/pero/resources/` |
| API、控制室与配置 | `src/pero/api/`、`src/pero/manager/`、`frontend/src/` |
