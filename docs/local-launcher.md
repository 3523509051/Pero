# Windows 本地启动

## 网页核心控制

通过本仓库 start.cmd 启动后，后台监督进程自动向控制室提供核心状态和固定启停入口，无需用户配置服务绑定。网页中的 Pero 启动/停止/重启只作用于核心，控制室保持在线；核心停止后仍能点启动。stop.cmd 则用于完全退出核心、控制室和监督进程。

桥的按钮只调用 webot 自己的微信桥启动器，不操作微信客户端。并列目录可自动识别已配置的本机 webot；非并列目录由既有“连接Pero.cmd”自动登记路径，无需手填任务名。仅通过手动 python 启动、没有监督进程的部署继续显示观测状态，不伪造可控能力。

不要同时运行多个入口来接管同一端口或数据目录。网页操作返回后会显示真实回执；忙碌或停止未确认时会保留错误，不把未确认的关闭标记为成功。升级启动器代码后需先用旧 stop.cmd 正常退出，再运行 start.cmd 采用新监督进程。

启动器用于源码仓库，不安装依赖或替换现有部署。start.cmd 在后台运行监督进程，只启动本项目。先按 README 创建 `.venv`、安装后端依赖并构建 `frontend/dist`。本入口仅管理 Core／Manager；微信客户端始终由用户手动启动与登录。

## 日常操作

1. 双击仓库根目录 `start.cmd`，等待核心与控制室健康检查通过。
2. 浏览器自动打开本机控制室。双击 `login-code.cmd`，用显示的管理访问码登录。
3. 启动窗口完成后可以关闭，监督进程在后台运行。要停止时运行 `stop.cmd` 。
4. `status.cmd` 显示当前脚本实例的地址、配置、日志与两个服务的健康状态。

只有查看访问码脚本显式展示明文，启动日志和 session.json 不保存它。首次自动生成的随机码经现有用户级 DPAPI 加密，路径为 `data/manager/management-token.dpapi`；再次启动复用同一个码。

启动前的 `PERO_MANAGEMENT_TOKEN` 可指定访问码，启动器会将其加密保存，核心和控制室共用该值。已有其它部署使用不同访问码时应分别管理；本脚本不从其它任务提取令牌，也不接管其它实例。密文不可直接迁给另一台机器或另一个用户使用。

## 配置与路径

- 配置优先顺序：`--config` → `PERO_CONFIG_FILE` → 仓库 `config/local.json`。
- 未指定配置且 local.json 不存在时，从 default.json 创建；显式指定却不存在的配置直接报错。
- 相对配置路径按仓库根目录解析，因此仓库路径包含空格或中文也可使用。
- 核心监听地址／端口由选中的配置决定；控制室默认绑定 `127.0.0.1:8766`。
- 管理界面配置及凭据默认沿用 `data/manager/`，核心记忆目录仍由运行配置决定。
- 日志分开写入 `logs/launcher/<run_id>/core.log` 与 `manager.log`。
- 启动状态及停止请求在 `data/launcher/`，一份仓库只允许一个本启动器实例；不要借更换端口或配置让多个 Core 共用同一聊天数据库。

重复运行 start 只确认或打开已有脚本实例，不把新参数应用到正在运行的服务。修改配置、端口或状态目录后，先正常停止，再重新启动。

```powershell
.\start.cmd --config "config/local.json" --no-browser
.\start.cmd --config "config/local.json" --manager-port 8876
.\.venv\Scripts\python.exe scripts\launcher.py check --config "config/local.json"
```

控制室使用自定义端口时，启动器会同时为 Core 设置相应的 `PERO_MANAGER_URL`，保持凭据解析目标一致。此启动器要求 Core 能通过本机回环访问，不直接提供远程管理入口。

## 参数

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `--config` | 见优先顺序 | 两个服务共用的运行配置 |
| `--manager-port` | 8766 | 本机控制室端口，不修改运行配置中的核心端口 |
| `--no-browser` | 关闭 | 只启动服务，不自动打开浏览器 |
| `--start-timeout` | 60 秒 | 等待两个服务健康；超时清理本次创建的进程 |
| `--stop-timeout` | 120 秒 | 等待对话／后台任务收尾，超过后取消本次停止请求 |
| `--state-dir` | data/launcher | 启动状态目录；使用后 stop/status 需传同一参数。访问码统一保存在 Pero/data/manager，旧目录可用于启动时迁移 |
| `--manager-state-dir` | data/manager | 控制室配置／托管凭据目录；更换此目录意味着使用另一份控制室数据 |

自定义 `--state-dir` 的状态操作示例：

```powershell
.\status.cmd --state-dir "D:\Pero本地状态"
.\stop.cmd --state-dir "D:\Pero本地状态"
.\login-code.cmd --state-dir "D:\Pero本地状态"
```

## 停止与进程归属

正常停止先调用 Core 的 drain 接口，等待已有工作结束，然后通知两个服务走各自的 ASGI 关闭流程。若忙碌超过等待时间，或无法确认管理接口状态，启动器取消停止请求并保留实例，便于稍后重试。

脚本通过 Windows Job Object 管理自己创建的进程树，创建服务进程时先挂起、登记归属后才恢复执行。关闭启动窗口或启动器被意外终止后，所属进程由系统清理；这属于中断路径，应优先正常停止。有界等待后仍不退出的本次进程也会被清理。

端口占用只导致报错，不结束占用该端口的进程。停止脚本也不会按名字扫描并终止所有 Python、微信或其它服务。启动器不安装 Windows 服务、计划任务或登录自启。

Windows 机制参考：Microsoft [Job Objects](https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects)、[CreateProcessW](https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-createprocessw)。

## 手动运行

需要分开调试时可继续使用原入口。先确保没有运行中的启动器实例，再在两个终端为两个服务提供同一个管理访问码：

```powershell
# 终端一，仓库根目录
$env:PERO_MANAGEMENT_TOKEN = '<你设置的管理访问码>'
.\.venv\Scripts\python.exe -m pero --config config/local.json
```

```powershell
# 终端二，仓库根目录
$env:PERO_MANAGEMENT_TOKEN = '<与核心相同的管理访问码>'
.\.venv\Scripts\python.exe -m pero.manager --config config/local.json
```

手动模式不自动使用启动器密文、不自动管理进程或打开浏览器。`stop.cmd` 只处理启动器管理的实例，手动模式请在相应终端退出。

## 常见情况

| 提示 | 处理 |
| --- | --- |
| 缺少 Python 环境／依赖 | 按 README 创建 `.venv` 并安装依赖，不由启动脚本临时联网安装 |
| 前端未构建 | 在 frontend 运行 npm ci、npm run build 后重试 |
| 端口占用 | 核对是否已有手动／服务实例；自行调整配置或管理原实例 |
| 加密访问码读取失败 | 使用原 Windows 用户核对，脚本不会覆盖旧密文 |
| 页面正常但不能对话 | 核对模型、桥、路由、授权及自动处理／发送开关 |
| 停止等待超时 | 查看正在执行的对话及任务，收尾后重试；不要把未确认发送当成未发送 |

微信组合启动已由 webot 统一管理。Pero 不再提供微信扫描、配对和桥进程启动参数。不要同时运行独立 Pero 与 webot 组合实例；需要组合部署时，请阅读 webot 仓库的 docs/deployment-windows.md。

管理访问码统一由 Pero 负责。访问码在 Pero 根目录运行 login-code.cmd 查看；新位置为 data/manager/management-token.dpapi，启动时可迁移原有用户加密的访问码。webot 不提供查看入口。更新后重新启动组合，以采用统一凭据。

双方现独立启动：先启动 Pero，后启动 webot；各自 start/stop 不控制对方。连接配置由 webot 的连接Pero.cmd 在双方停止时执行，不再由启动入口组合拉起。
