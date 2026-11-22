# Pero Plugin SDK v1 — 实际公开接口（RC）

2026-11-21。核心接口已经实现，正式稳定版需实际独立插件导入验证后冻结。插件只实现业务，不依赖宿主内部代码。此 SDK 小包包含公共类型，运行由宿主提供。

## 创建插件

根目录 `plugin.json`（schema_version=1, sdk_version="1"）、`plugin_adapter.py`、业务模块、README、LICENSE、NOTICE、固定依赖及 `requirements.lock.txt`。远程包必须明确 `runtime="worker"`。唯一 `id` 与 tools；不得覆盖内置工具。声明配置 JSON Schema、所需能力。若不联网、无存储、无产物可不给额外能力。公共 `pero_plugin_sdk` 包提供 SDK、ToolResult、ToolDeclaration、RequestContext、ArtifactHandle 类型。

注册函数 `register()` 返回工具声明列表：name、description、parameters、handler；异步 `handler(arguments, sdk)` 返回结构化结果。工具参数宿主验证。可返回 `ok/status/data/artifacts/warnings/error/cache`，`status` 为 complete/partial/unavailable/failed；不能用大段文本/图片字节代替产物。缓存命中推荐 `cache={"hit":true}`，兼容已有 `data.cache`。

| 实际接口 | 作用 |
|---|---|
| `sdk.context` | run_id、scope_ref、agent_id、conversation_id、channel_id、language、timezone |
| `sdk.config.get(key, default)` / `all()` | 默认值 + 实例覆盖 + 当前会话覆盖 |
| `sdk.storage.read_json/write_json`、`read_blob/write_blob/list_blobs/delete_blob/total_bytes`、`cache_dir` | 声明 `cache.private` 后可用，按实例和会话隔离。远程容器的数据由宿主核验后提交。 |
| `await sdk.artifacts.publish(data, name, kind="document" or "image")` | 声明 `artifact.document` / `artifact.image` 后发布，返回 `ArtifactHandle` `{handle,kind,name,mime,size,sha256}`。 |
| `await sdk.artifacts.attach(handle)` | 指定本轮附加图片；真正提供给模型由宿主确认。文档可发布，但自动附文档尚未开放。 |
| `await sdk.network.read(url, max_bytes=..., timeout=...)` | `network.read` + manifest `network_hosts` + 管理员批准的明确公开 HTTPS 服务；远程容器通过宿主读取通道，不直接联网。 |
| `sdk.events.emit(type, data)` | `events.emit`，plugin.status/progress/record，有界状态。worker 当前在结果回传时提交事件。 |
| `sdk.is_cancelled` | host 模式可查询；worker 由宿主取消回收容器/进程，不保证第三方代码获得合作式通知。 |

将发布返回的 ArtifactHandle 放进结果 `artifacts` 列表。发布图片与 attach 独立；只发布图片可以在资源目录查看，不表示已输入模型。会话内重复处理可复用缓存，但本轮要再次发布/挂载。宿主将产物映射成真实资源 ID，L20 记录受控引用及缓存状态。

兼容旧接口 `publish_image`/`images.attach`/`write_artifact` 仍存在，但返回形式按执行模式不同；新插件用统一 `artifacts.publish/attach`。`network.video_read` 是旧 host 适配能力，**新远程插件应该使用通用 `network.read`**。远程容器无直接外网；不要用普通 HTTP 库绕过宿主能力接口。现有 B 站业务模块独立运行可继续自己的 HTTP fetcher，宿主适配层需要把 fetcher 接到 SDK 的读取能力。

## 安装及管理

## 后台工具与短链接（2026-11-21 扩展）

2026-11-22 回报修订：`drain` 只拒绝新任务，已受理后台任务继续执行、分析和回报；`/api/v1/management/drain` 的 `busy` 包含 `plugin_jobs`，部署必须等其归零。只有真实权限撤回、路由变化或停用才阻止回报。已确认发送的完成回复进入原始聊天和 L2，L20 仍挂在提交轮次。任务 `state` 表示处理结果，`delivery_state` 表示回报投递结果；回报异常另记 `delivery_error_code`（如 `delivery_timeout`）。通知超时或异常不自动重复发送，也不把已完成处理改写为处理超时。插件 handler 正常返回结果和产物即可，缓存命中也不能跳过返回；无须自行实现唤醒 AI、微信发送或二次回调。

任务列表清理（同日扩展）：认证管理 API `DELETE /manage/v1/plugins/jobs/{job_id}` 删除一条已结束记录，`DELETE /manage/v1/plugins/jobs/finished` 清空所有会话已结束记录。Core 对应前缀为 `/api/v1/plugins`。queued/running/delivering 或仍有活动任务对象时拒绝删除，先调用取消并等待结束；不存在返回 job_not_found。删除只持久清理 jobs.json 的列表记录，不删除资源、视频缓存、L20 或聊天/记忆，不产生模型请求、不重新投递。所有端点沿用现有认证与 CSRF。

工具调用真实性（同日补充）：每轮主回复/工具续答收到宿主提供的当前 Agent+会话最多六条后台任务状态，无结果正文；空列表不能解读为历史从未执行。该状态仅为本轮临时注记，不写入原始聊天或 L2。模型不能拿旧助手“在看/看完”的文字充当真实工具执行；需要处理当前外部资料且没有实际结果时应调用可用工具，只有真实 queued/job_id 才能承诺后台完成回报。后台工具的宿主执行约定在可编辑 Prompt 后独立追加，避免 Prompt 文件覆盖时丢失；这不代替模型语义判断、不按链接强制派发、不额外重试模型。

插件在 `plugin.json` 声明 `"background_tools": ["工具名"]`（必须属于 tools）以及 `"background_timeout_seconds": 180`（10–600 秒）。handler 签名和结果契约不变。工具被对话调用后只返回任务提交回执和 job_id，宿主负责正常初次回应、释放当前会话、独立运行与完成回报。后台业务与后续对话并行；最终结果分析及投递与该会话正常回复串行。独立新 ImageScope 承接结果，关闭思考、无新工具调用，在 L20 原始提交轮次追加完成证据；只有确认发送的回报才进入聊天流水。取消、权限变化、插件停用、超时、重启均有明确状态，不自动重试既有任务。

控制站插件页可查看/取消任务：`GET /manage/v1/plugins/jobs`、`POST /manage/v1/plugins/jobs/{job_id}/cancel`。任务状态持久化，重启未完成任务标记 interrupted；任务结果正文和二进制不进入任务状态文件。全局并发 3、活跃任务最多 16、同会话最多 2。未装配后台回报的调用入口明确返回 background_unavailable，不将耗时工作偷偷改为前台。

`sdk.network.read(url, redirect_metadata=True)` 可取得批准范围中的 3xx 响应元数据：`status_code`、`location`，不自动访问跳转目标，`bytes` 为空。普通 200 行为保持不变。来源和目标域名均须在 manifest 声明且管理员批准；应用自行限制跳转次数，仅接受所需业务地址。此能力适合短链接、资源迁移等通用场景。

控制站插件页允许本地可信目录或 GitHub HTTPS URL + tag/commit。预览固定提交、清单、依赖、域名与摘要；确认安装，默认停用。批准网络域名（停用时修改），再启用及按会话覆盖。更新不执行未知安装脚本，远程版本只在受控 Linux 容器运行。当前容器内预装批准的 `httpx==0.28.1` 与 `Pillow==11.3.0` 及固定传递依赖；其他依赖须经宿主审核后更新版本化的镜像，不能由插件任意安装。

管理接口 `/manage/v1/plugins`、`/plugins/source/preview`、`/plugins/install-github`、`/{id}/enable|disable|update`、`/{id}/config`、`/{id}/network`、`/scopes/{scope_id}/plugins/{id}`（均在 manage/v1 前缀下）。保留管理员登录及CSRF，不提供模型自行安装权限。插件状态、来源、版本、scope configs、网络许可均从后端返回。

## 尚未开放的可选能力

主动模型调用、记忆/知识库读取、任意外部写服务、插件长期任务、自定义第三方页面、自动附文件、公共跨会话缓存。内置能力继续正常，不把这些未实现的门面写成可调用接口。后续按需扩展并保持 SDK 兼容。
