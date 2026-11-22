"""History image tools mount Base64 only in the current scoped model turn."""
from pero.core.contracts import ToolSpec
from pero.tools.context import current_tool_context


class ImageTool:
    ephemeral_result=True
    def __init__(self,name,backend):
        self.tool_id=name
        self.backend=backend

    def spec(self):
        listing=self.tool_id=="list_images"
        return ToolSpec(name=self.tool_id,description=(
            "列出本会话最近图片的 Pero 编号与名称，不看图；每页最多100项，按 offset 翻页。" if listing else
            "按 Pero 图片编号看历史图（r- 开头）；已在本轮提供的图无需重复调用，直接回答。仅挂本轮，不发图。"),
            parameters={"type":"object","properties":({
                "sender_ref":{"type":"string","maxLength":100},
                "limit":{"type":"integer","minimum":1,"maximum":100},
                "offset":{"type":"integer","minimum":0,"maximum":10000}} if listing else {
                "image_id":{"type":"string","minLength":1,"maxLength":100}}),
                "required":[] if listing else ["image_id"],"additionalProperties":False})

    async def run(self,arguments):
        context=current_tool_context()
        scope=getattr(context,"images",None)
        if scope is None or not self.backend.settings.enabled:
            return {"ok":False,"error_code":"images_unavailable"}
        try:
            if self.tool_id=="list_images":
                count=getattr(scope,"list_count",0)
                if count>=4:
                    return {"ok":False,"error_code":"image_list_limit"}
                scope.list_count=count+1
                limit=int(arguments.get('limit',100));offset=int(arguments.get('offset',0))
                items=await self.backend.catalog.recent_images(agent_id=scope.agent_id,
                    conversation_id=scope.conversation_id,limit=limit+1,offset=offset,
                    sender_ref=str(arguments.get("sender_ref") or "") or None)
                more=len(items)>limit;items=items[:limit]
                return {"ok":True,"items":[{k:item.get(k) for k in
                    ("resource_id","name","sender_ref","observed_at","version")} for item in items],"returned":len(items),"offset":offset,"has_more":more,"next_offset":offset+len(items) if more else None}
            value=await scope.mount(str(arguments.get("image_id") or "").strip().lstrip("#"),reason="tool")
            return {"ok":True,**value,"detail":"图片只挂本轮；提供证据以模型请求状态为准，不要重复调用。"}
        except Exception as exc:
            return {"ok":False,"error_code":getattr(exc,"code","image_error"),
                "detail":"图片未提供，可能不存在或不属于此会话；不要猜测或自动换另一张。"}
