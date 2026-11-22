"""Public, host-independent runtime contract for Pero plugin SDK v1."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, Protocol, TypedDict

SDK_VERSION = '1'


class ArtifactHandle(TypedDict):
    handle: str
    kind: str
    name: str
    mime: str
    size: int
    sha256: str


class ToolResult(TypedDict, total=False):
    ok: bool
    status: str
    data: dict[str, Any]
    artifacts: list[ArtifactHandle]
    warnings: list[str]
    error: dict[str, str] | None
    cache: dict[str, Any]


class ToolDeclaration(TypedDict):
    name: str
    description: str
    parameters: dict[str, Any]
    handler: Callable[[Mapping[str, Any], Any], Awaitable[ToolResult]]


@dataclass(frozen=True)
class RequestContext:
    run_id: str
    scope_ref: str
    agent_id: str
    conversation_id: str
    channel_id: str = ''
    language: str = 'zh-CN'
    timezone: str = 'Asia/Shanghai'


class Artifacts(Protocol):
    async def publish(self, data: bytes, name: str, *, kind: str = 'document') -> ArtifactHandle: ...
    async def attach(self, handle: str) -> Mapping[str, Any]: ...


class SDK(Protocol):
    context: RequestContext
    artifacts: Artifacts
    config: Any
    storage: Any
    events: Any
    @property
    def is_cancelled(self) -> bool: ...
