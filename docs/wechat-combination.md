# 微信与 Pero 独立启动

Pero/start.cmd 只后台启动 Core 和 Manager；webot/start.cmd 只后台启动微信桥。各自停止入口只停止本项目。首次连接需在双方停止时运行 webot/连接Pero.cmd；该脚本同步全部群路由和连接凭据，不启动服务、不选群。管理访问码始终由 Pero/login-code.cmd 查看。

步骤见 [webot Windows 部署指南](https://github.com/3523509051/webot/blob/main/docs/deployment-windows.md)。
