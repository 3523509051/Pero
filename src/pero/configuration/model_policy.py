"""Typed configuration for reply models and optional tool model stages."""
from typing import Literal
from urllib.parse import urlsplit
from pydantic import BaseModel, ConfigDict, Field, field_validator


class ModelChanges(BaseModel):
    model_config=ConfigDict(extra='forbid',strict=True)
    model: str | None = Field(default=None,min_length=1,max_length=120)
    base_url: str | None = None
    credential_ref: str | None = Field(default=None,pattern=r'^cred-[0-9a-f]{32}$')
    reply_thinking: bool | None = None
    @field_validator('base_url')
    @classmethod
    def origin(cls,value):
        if value is None:return value
        p=urlsplit(value)
        if (p.scheme not in {'http','https'} or not p.hostname or p.username or p.password or p.query or p.fragment
                or (p.scheme=='http' and p.hostname not in {'127.0.0.1','localhost','::1'})):
            raise ValueError('model_base_url_invalid')
        return value.rstrip('/')


class PolicyChanges(BaseModel):
    model_config=ConfigDict(extra='forbid',strict=True)
    default_profile: str | None = Field(default=None,min_length=1,max_length=80,pattern=r'^[A-Za-z0-9_-]+$')
    tool_thinking_default: bool | None = None
