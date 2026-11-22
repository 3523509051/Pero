"""表情资产工具（P12）：`list_stickers` / `save_sticker` / `rename_sticker`。

边界：
- 资产元数据在 `resources/stickers.py` 独立库；原图引用 Pero 资源目录，
  不接受磁盘路径；作用域 = 当前 ToolContext 的 (agent_id, conversation_id)；
- `save_sticker` 只允许来源于**本轮 provided_to_model 证据**的可见图片；
  目录/仅挂载/模型口头声称看过不足以授权；入库只返元数据，不把图片再送模型；
- 每轮入库上限由 settings（默认 1）控制，重试/重复调用幂等（同名复用原资产）；
- `rename_sticker` 只改元数据，同名冲突明确失败，不覆盖他人资产；
- 选择表情不调用识图；目录返回不等于"已经看过"。
"""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from typing import Any, Dict, List, Optional

from pero.core.contracts import ToolSpec
from pero.core.settings import StickerSettings
from pero.resources.catalog import ResourceCatalog, ResourceCatalogError
from pero.resources.stickers import StickerAssetStore, StickerStoreError

_logger = logging.getLogger(__name__)

DISABLED_NOTE = "表情资产功能未启用（stickers.enabled=false），如实告诉用户该功能当前关闭。"


def _note(code: str, detail: str) -> Dict[str, Any]:
    """失败统一 ok=false + 统一 error_code；执行器/L20 保留具体失败，
    不降级成通用 tool_rejected。"""

    return {"ok": False, "error": code, "error_code": code, "detail": detail}


class StickerBackend:
    """进程级共享：独立资产库 + 资源目录元数据 + 每轮入库计数。"""

    #: 每轮入库计数表的容量上限（防泄漏；超出整表重建）。
    _TURN_COUNTER_MAX = 512

    def __init__(
        self,
        settings: StickerSettings,
        catalog: ResourceCatalog,
        store: StickerAssetStore,
    ) -> None:
        self.settings = settings
        self.catalog = catalog
        self.store = store
        #: 键为 turn_id 的每轮入库计数（进程内存，重启即空）。
        self._turn_saves: "OrderedDict[str, int]" = OrderedDict()

    def metadata(self) -> Dict[str, Any]:
        return {
            "enabled": bool(self.settings.enabled),
            "auto_send_enabled": bool(self.settings.auto_send_enabled),
            "media_action": self.settings.media_action,
            "saves_per_turn": int(self.settings.saves_per_turn),
            "max_stickers_per_reply": int(self.settings.max_stickers_per_reply),
            "max_image_bytes": int(self.settings.max_image_bytes),
            "bridge_extension": "not_verified",
            "id_format": "sa-<hex32>（不复用旧桥 st_xxxx / 图片缓存编号）",
        }

    def _context(self):
        from pero.tools.context import current_tool_context

        return current_tool_context()

    def _check_run_saves(self, run_id: str) -> bool:
        """本次运行是否还有入库名额；只有成功入库才占用，失败可释放重试。

        键为**单次运行标识**（ToolContext.run_id，由编排/HTTP 生成）：HTTP 与
        通道各自隔离；不用 '-' 桶，也不按会话 ID 永久共用。
        """

        count = self._turn_saves.get(run_id, 0)
        if count >= self.settings.saves_per_turn:
            return False
        if run_id not in self._turn_saves:
            while len(self._turn_saves) >= self._TURN_COUNTER_MAX:
                self._turn_saves.popitem(last=False)
        self._turn_saves[run_id] = count + 1
        self._turn_saves.move_to_end(run_id)
        return True

    def _release_run_saves(self, run_id: str) -> None:
        count = self._turn_saves.get(run_id, 0)
        if count <= 1:
            self._turn_saves.pop(run_id, None)
        else:
            self._turn_saves[run_id] = count - 1

    def release_run(self, run_id: str) -> None:
        """运行结束（成功/失败/取消）由编排调用的清理入口。"""

        self._turn_saves.pop(run_id, None)

    # ------------------------------------------------------------------ #

    # ------------------------------------------------------------------ #
    # 异步门面：资产库 sqlite 一律走线程，事件循环/HTTP 不做同步 IO。
    # ------------------------------------------------------------------ #

    async def _store_find(self, **kwargs) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(self.store.find, **kwargs)

    async def _store_count_filtered(self, **kwargs) -> int:
        return await asyncio.to_thread(self.store.count_filtered, **kwargs)

    async def _store_get_by_name(self, **kwargs):
        return await asyncio.to_thread(self.store.get_by_name, **kwargs)

    async def _store_save(self, **kwargs):
        return await asyncio.to_thread(self.store.save, **kwargs)

    async def _store_rename(self, **kwargs):
        return await asyncio.to_thread(self.store.rename, **kwargs)

    async def list_stickers(
        self,
        *,
        agent_id: str,
        conversation_id: str,
        query: str = "",
        emotion: str = "",
        scene: str = "",
        limit: int = 0,
        offset: int = 0,
    ) -> Dict[str, Any]:
        limit = int(limit) if int(limit) > 0 else self.settings.list_limit
        limit = min(max(int(limit), 1), 100)
        offset = max(0, int(offset))
        filters = dict(
            agent_id=agent_id,
            conversation_id=conversation_id,
            query=query,
            emotion=emotion,
            scene=scene,
        )
        try:
            items = await self._store_find(limit=limit, offset=offset, **filters)
            total = await self._store_count_filtered(**filters)
        except StickerStoreError as exc:
            return _note(exc.code, "表情目录检索未成功。")
        except Exception as exc:  # noqa: BLE001 - 存储异常如实返回
            _logger.warning("表情目录检索失败 error=%s", type(exc).__name__)
            return _note("store_error", "表情目录检索未成功（存储异常）。")
        return {
            "items": [{**{k:item.get(k) for k in ('asset_id','name','emotion','scene')},'description':str(item.get('description') or '')[:80]} for item in items],
            "total": total,
            "returned": len(items),
            "offset": offset,
            "has_more": offset + len(items) < total,
            "next_offset": offset + len(items) if offset + len(items) < total else None,
            "send_hint": self._send_hint(),
        }

    def _send_hint(self) -> str:
        limit = max(1, int(self.settings.max_stickers_per_reply))
        return (
            f"发送方式：在回复最末尾按顺序写 [[表情:资产ID]]，单次最多 {limit} 张；"
            "只能引用当前会话返回的 sa-… 资产 ID，不要写名字或自造 ID。"
        )

    async def save_sticker(
        self,
        *,
        agent_id: str,
        conversation_id: str,
        run_id: str,
        image_id: str,
        name: str,
        emotion: str = "",
        scene: str = "",
        tags: Optional[List[str]] = None,
        description: str = "",
        event_id: str = "",
    ) -> Dict[str, Any]:
        if not isinstance(image_id, str) or not image_id.strip():
            return _note("image_id_required", "请提供本轮图片编号（来自目录或引用）。")
        image_id = image_id.strip()
        # 保存限额按**真实 run_id**（编排/HTTP 每次运行独立生成，不由工具参数
        # 自行设置）；HTTP 与通道各自隔离，不按 '-' 或会话 ID 永久共用。
        if not isinstance(run_id, str) or not run_id.strip():
            return _note(
                "no_run_context",
                "本次调用缺少真实运行标识，不能确定保存名额归属；请通过正常运行调用。",
            )
        run_id = run_id.strip()
        # 保存资格：必须来自本轮图片证据，且模型实际收到过（provided_to_model=True）。
        evidence = []
        scope = getattr(self._context(), "images", None) if self._context() else None
        if scope is not None:
            evidence = [
                item
                for item in scope.evidence()
                if item.get("resource_id") == image_id and item.get("provided_to_model")
            ]
        if not evidence:
            return _note(
                "image_not_provided",
                "该图片本轮没有实际提供给模型，不能仅凭目录或口头描述入库；"
                "请先让模型查看该图片（look_image）再保存。",
            )
        item = evidence[0]
        size = int(item.get("size") or 0)
        if size > self.settings.max_image_bytes:
            return _note(
                "image_too_large",
                f"图片原件 {size} 字节超过表情上限 {self.settings.max_image_bytes}。",
            )
        # 来源关联只用本轮可信证据/资源目录，不从模型参数伪造；
        # ConversationRef 没有 event_id 字段，绝不访问不存在的属性。
        source_event_id = str(item.get("event_id") or "")[:120] if isinstance(item, dict) else ""
        try:
            meta = await self.catalog.get(
                agent_id=agent_id, conversation_id=conversation_id, resource_id=image_id
            )
        except ResourceCatalogError as exc:
            return _note(exc.code or "resource_error", "原图元数据读取未成功。")
        if meta is None or meta.get("kind") != "image":
            return _note("image_not_found", "该编号在本会话资源目录中不是可用图片。")
        # 幂等复用：同作用域同名资产必须资源 ID/版本/哈希完全一致；
        # 不同图同名是明确冲突，不得悄悄把旧图当新图保存成功。
        try:
            existing = await self._store_get_by_name(
                agent_id=agent_id, conversation_id=conversation_id, name=name
            )
        except Exception as exc:  # noqa: BLE001
            _logger.warning("表情同名查找失败 error=%s", type(exc).__name__)
            return _note("store_error", "表情同名查找未成功（存储异常）。")
        if existing is not None:
            same_source = (
                existing.get("source_resource_id") == str(meta["resource_id"])
                and int(existing.get("source_version") or 0) == int(meta["version"])
                and existing.get("source_sha256") == str(meta["sha256"])
            )
            if same_source:
                return {
                    "ok": True,
                    "asset": existing,
                    "reused": True,
                    "send_hint": self._send_hint()
                    + " 该资产已存在，可直接引用其 asset_id 发送。",
                }
            return _note(
                "name_conflict_different_image",
                "同名资产已存在但对应不同图片；请换一个名字，不要覆盖旧资产。",
            )
        if not self._check_run_saves(run_id):
            return _note(
                "save_limit",
                f"本次运行最多保存 {self.settings.saves_per_turn} 张表情，已达到上限。",
            )
        try:
            asset = await self._store_save(
                agent_id=agent_id,
                conversation_id=conversation_id,
                name=name,
                source_resource_id=str(meta["resource_id"]),
                source_version=int(meta["version"]),
                source_sha256=str(meta["sha256"]),
                source_event_id=source_event_id,
                emotion=emotion,
                scene=scene,
                tags=tags or (),
                description=description,
            )
        except StickerStoreError as exc:
            # 资源读取/验证/事务失败都释放**本次**预留（只释放自己的名额）。
            self._release_run_saves(run_id)
            return _note(exc.code, "表情入库未成功（存储事务失败）。")
        except Exception as exc:  # noqa: BLE001
            self._release_run_saves(run_id)
            _logger.warning("表情入库异常 error=%s", type(exc).__name__)
            return _note("store_error", "表情入库未成功（存储异常）。")
        return {
            "ok": True,
            "asset": asset,
            "reused": False,
            "send_hint": self._send_hint() + " 本次保存的 asset_id 可直接在本轮回复中引用，无需再检索。",
        }

    async def rename_sticker(
        self,
        *,
        agent_id: str,
        conversation_id: str,
        asset_id: str,
        name: Optional[str] = None,
        emotion: Optional[str] = None,
        scene: Optional[str] = None,
        add_tags: Optional[List[str]] = None,
        remove_tags: Optional[List[str]] = None,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            asset = await self._store_rename(
                agent_id=agent_id,
                conversation_id=conversation_id,
                asset_id=asset_id,
                name=name,
                emotion=emotion,
                scene=scene,
                add_tags=add_tags or (),
                remove_tags=remove_tags or (),
                description=description,
            )
        except StickerStoreError as exc:
            detail = {
                "sticker_not_found": "该资产不在当前会话作用域内，请先用 list_stickers 查看。",
                "name_conflict": "同名资产已存在，请换一个名字。",
                "invalid_asset_id": "资产 ID 形态不合法，只能使用 list_stickers 返回的 sa-… 编号。",
                "name_required": "名称不能为空。",
            }.get(exc.code, "重命名未成功（存储事务失败）。")
            return _note(exc.code, detail)
        except Exception as exc:  # noqa: BLE001
            _logger.warning("表情重命名异常 error=%s", type(exc).__name__)
            return _note("store_error", "重命名未成功（存储异常）。")
        return {"ok": True, "asset": asset}


class StickerTool:
    """规格与执行入口；与 ImageTool 同型（真实可执行工具）。"""

    _SPECS = {
        "list_stickers": (
            "按名称/情绪/场景检索当前会话的表情资产目录（只返回元数据，不含图片内容）。"
            "回复最末尾按顺序写 [[表情:资产ID]] 可附表情；单次回复最多 6 张，"
            "只能用当前会话返回的 sa-… 资产 ID。",
            {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "名称或描述关键词。", "maxLength": 60},
                    "emotion": {"type": "string", "description": "情绪筛选。", "maxLength": 40},
                    "scene": {"type": "string", "description": "使用场景筛选。", "maxLength": 60},
                    "limit": {"type": "integer", "minimum":1, "maximum":100, "description": "每页最多 100 项；默认 100。"},
                    "offset": {"type": "integer", "description": "分页偏移量（默认 0）。"},
                },
                "additionalProperties": False,
            },
        ),
        "save_sticker": (
            "把本轮已经实际查看过的图片保存为表情资产。只能使用本轮图片编号，"
            "且该图片必须已经提供给模型查看；每轮最多保存 1 张。",
            {
                "type": "object",
                "properties": {
                    "image_id": {"type": "string", "description": "本轮图片编号（r-…）。", "maxLength": 80},
                    "name": {"type": "string", "description": "表情名称。", "maxLength": 60},
                    "emotion": {"type": "string", "description": "适用情绪。", "maxLength": 40},
                    "scene": {"type": "string", "description": "适用场景。", "maxLength": 60},
                    "tags": {
                        "type": "array",
                        "items": {"type": "string", "maxLength": 30},
                        "maxItems": 8,
                        "description": "标签列表。",
                    },
                    "description": {"type": "string", "description": "一句话描述。", "maxLength": 200},
                },
                "required": ["image_id", "name"],
                "additionalProperties": False,
            },
        ),
        "rename_sticker": (
            "修改当前会话表情资产的名称/情绪/场景/标签等元数据；只能用 list_stickers "
            "返回的资产 ID，同名冲突会失败。",
            {
                "type": "object",
                "properties": {
                    "asset_id": {"type": "string", "description": "资产 ID（sa-…）。", "maxLength": 80},
                    "name": {"type": "string", "description": "新名称；不传则不改。", "maxLength": 60},
                    "emotion": {"type": "string", "description": "新情绪；不传则不改。", "maxLength": 40},
                    "scene": {"type": "string", "description": "新场景；不传则不改。", "maxLength": 60},
                    "add_tags": {
                        "type": "array",
                        "items": {"type": "string", "maxLength": 30},
                        "maxItems": 8,
                        "description": "追加标签。",
                    },
                    "remove_tags": {
                        "type": "array",
                        "items": {"type": "string", "maxLength": 30},
                        "maxItems": 8,
                        "description": "移除标签。",
                    },
                    "description": {"type": "string", "description": "新描述；不传则不改。", "maxLength": 200},
                },
                "required": ["asset_id"],
                "additionalProperties": False,
            },
        ),
    }

    def __init__(self, name: str, backend: StickerBackend) -> None:
        self.tool_id = name
        self.backend = backend

    def spec(self) -> ToolSpec:
        description, parameters = self._SPECS[self.tool_id]
        return ToolSpec(name=self.tool_id, description=description, parameters=parameters)

    async def run(self, arguments: dict) -> dict:
        from pero.tools.context import current_tool_context

        context = current_tool_context()
        if context is None:
            return _note("no_context", "工具缺少本轮上下文。")
        if not self.backend.settings.enabled:
            return _note("stickers_disabled", DISABLED_NOTE)
        args = dict(arguments)
        if self.tool_id == "list_stickers":
            return await self.backend.list_stickers(
                agent_id=context.agent_id,
                conversation_id=context.conversation_id,
                query=str(args.get("query") or ""),
                emotion=str(args.get("emotion") or ""),
                scene=str(args.get("scene") or ""),
                limit=int(args.get("limit") or 0),
                offset=int(args.get("offset") or 0),
            )
        if self.tool_id == "save_sticker":
            return await self.backend.save_sticker(
                agent_id=context.agent_id,
                conversation_id=context.conversation_id,
                run_id=context.run_id or "",
                image_id=str(args.get("image_id") or ""),
                name=str(args.get("name") or ""),
                emotion=str(args.get("emotion") or ""),
                scene=str(args.get("scene") or ""),
                tags=[str(t) for t in (args.get("tags") or ())],
                description=str(args.get("description") or ""),
            )
        return await self.backend.rename_sticker(
            agent_id=context.agent_id,
            conversation_id=context.conversation_id,
            asset_id=str(args.get("asset_id") or ""),
            name=(str(args["name"]) if "name" in args else None),
            emotion=(str(args["emotion"]) if "emotion" in args else None),
            scene=(str(args["scene"]) if "scene" in args else None),
            add_tags=[str(t) for t in (args.get("add_tags") or ())],
            remove_tags=[str(t) for t in (args.get("remove_tags") or ())],
            description=(str(args["description"]) if "description" in args else None),
        )
