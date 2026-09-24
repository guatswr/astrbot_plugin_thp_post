# AstrBot THP 现场投稿插件

接收 NapCat / OneBot v11 群消息，将 `/投稿 内容` 可靠上传至 THP Post 后端。
不上传普通聊天，不读取群历史，不调用 LLM，不设置审核流程。

## 安装

本版按 AstrBot 4.28.1 源码（提交 `95e98b8aed75d56713666eff39e31bafffd95426`）核对了
`CustomFilter`、`event_message_type`、`Star.initialize/terminate`、`StarTools.get_data_dir`、
`event.send/stop_event` 与 `Context.send_message`。实际机器人加载及收发仍需实机验收。

1. 将插件目录安装到 AstrBot `data/plugins/astrbot_plugin_thp_post`，或使用其 WebUI 安装工程生成的 ZIP。
2. 安装 requirements.txt 依赖；AstrBot WebUI 常规插件安装流程会处理依赖。
3. 在插件配置填写云端 HTTPS 根地址、上传凭据、活动 ID、稳定实例 ID 和允许群列表。
4. 初次没有配置时插件可以加载，但投稿与后台上传保持停用；配置保存后重载插件才会启用。凭据输入默认遮蔽。公网服务地址必须使用 HTTPS；仅本机调试可用 `http://localhost` 或 `http://127.0.0.1`。
5. NapCat 使用 OneBot v11 反向 WebSocket 接入 AstrBot，消息上报格式必须为**数组**。
6. AstrBot 会话插件管理应允许此插件，机器人必须能接收目标群消息；不用要求观众 @ 机器人。

当前 metadata 声明 AstrBot `>=4.28,<5`。较旧实例应先核对兼容性，不能直接忽略版本限制假定可用。
本插件不提供 QQ 登录工具；沿用已有 NapCat/QQ 登录状态。

## 使用

`/投稿 你好，现场！`。支持中文、emoji、换行，默认最多 300 个 Unicode code point。
图片、引用消息、语音、转发等混合附件均明确拒绝。

QQ群公告建议：主动投稿的正文、昵称、QQ号与头像会在活动屏幕公开展示；普通聊天不会被收集。

## 可靠性

- 插件数据在 AstrBot plugin_data 的专用目录，`outbox.db` 不在插件代码目录内。
- 先保存，再异步上传。云端提交后回复编号；未确认则回复暂存，成功/失败后尽量补回执。
- 网络错误/408/429/5xx 重试；其他 4xx 终止并记录状态。相同来源消息不重复入队。
- 关闭/更换活动后，旧稿不会自动投到新活动。默认 30 分钟过期、1000 条待上传上限。
- 仅 QQ 普通群、纯文本。QQ 撤回不会自动撤稿，现场可在主持端隐藏。
- 热重载会关闭后台任务并保留队列。保存好 `source_instance_id`，迁移时保留 outbox 数据。
- 若 token 配错导致终止失败，先修复配置，再让观众发一条新稿；不要自行修改失败记录为成功。

只读查看队列（在插件数据目录）：

```bash
python -c "import sqlite3; c=sqlite3.connect('file:outbox.db?mode=ro',uri=True); print(c.execute('select status,count(*) from outbox group by status').fetchall())"
```

日志不含 token 或投稿全文。不要将 outbox、配置文件或观众资料提交到代码仓库。
活动完成后，对队列及其备份采用与云端一致的 30 天保留期，人工确认后清理；不要在插件启动时自动删除。
