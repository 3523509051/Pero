# 示例笔记理解插件（Pero 插件 SDK v1 验收样板）

本包只用于**本地验收**：演示清单解析、真实 ToolSpec 注册、启用/停用、配置、
缓存命中、插件独立存储、图片产物导入当前会话并挂载本轮。它不是视频模块，
也不调用任何模型或付费接口。

## 独立业务

`business/digest.py` 只依赖 Python 标准库，不导入 Pero：

```python
from business.digest import inspect_note
result = await inspect_note("要整理的文字", {"max_chars": 400, "include_image": True})
```

结果里的 `image.bytes` 是独立 PNG 字节；完整字幕/视频产物契约见
`D:\webot\交流\gpt指示报告\B站视频理解模块-实现规范.md`。

## 宿主接线

真实签名：`register() -> list[{"name","description","parameters","handler"}]`，
`async handler(arguments: dict, sdk) -> dict`。本包是 host 模式：`await
sdk.artifacts.publish_image(bytes, name)` 返回本会话 `r-…`，`await
sdk.images.attach(r_id, reason="...")` 挂入本轮；worker 模式下这两个门面语义不同，
见 `docs/plugin-sdk-v1.md`。handler 只使用传入的 `sdk`，不接收模型给的身份、
会话或路径参数。

## 许可

MIT。无第三方代码复制。
