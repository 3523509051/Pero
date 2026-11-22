"""F03 configuration documents: no credentials, reasoning overrides or arbitrary code."""
import hashlib
import json
from typing import Any, Literal
from .model_policy import ModelChanges, PolicyChanges
from pydantic import BaseModel, ConfigDict, Field


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


class PersonaChanges(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    prompt: str | None = Field(default=None, min_length=1, max_length=12000)


class ToolChanges(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    enabled: bool | None = None
    prompt: str | None = Field(default=None, min_length=1, max_length=6000)
    defaults: dict[str, Any] | None = None
    timeout_seconds: float | None = Field(default=None, gt=0, le=240, allow_inf_nan=False)
    credential_ref: str | None = Field(default=None,pattern=r'^cred-[0-9a-f]{32}$')
    analysis_enabled: bool | None = None
    analysis_prompt: str | None = Field(default=None,min_length=1,max_length=6000)
    thinking_mode: Literal['inherit','enabled','disabled'] | None = None
    model_profile: str | None = Field(default=None,min_length=1,max_length=80,pattern=r'^[A-Za-z0-9_-]+$')


class SaveRequest(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    expected_revision: int = Field(ge=0)
    request_id: str = Field(min_length=8, max_length=128, pattern=r'^[A-Za-z0-9_-]+$')
    changes: dict[str, Any]

class WakeChanges(BaseModel):
    model_config=ConfigDict(extra='forbid',strict=True)
    wake_words: list[str] | None = Field(default=None,max_length=100)
    wake_match: Literal['contains','prefix'] | None = None
    at_is_trusted_mention: bool | None = None


class ConfigurationPayload(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    source_id: str = Field(min_length=8, max_length=64, pattern=r'^[A-Za-z0-9_-]+$')
    generation: int = Field(ge=0)
    personas: dict[str, dict[str, Any]] = Field(default_factory=dict)
    tools: dict[str, dict[str, dict[str, Any]]] = Field(default_factory=dict)

    models: dict[str, dict[str, Any]] = Field(default_factory=dict)
    policy: dict[str, Any] = Field(default_factory=dict)
    wake: dict[str,dict[str,Any]] = Field(default_factory=dict)

    def digest(self):
        return hashlib.sha256(canonical(self.model_dump()).encode()).hexdigest()


def validate_changes(kind, value):
    model = {'persona':PersonaChanges,'tool':ToolChanges,'model':ModelChanges,'policy':PolicyChanges,'wake':WakeChanges}[kind]
    result = model.model_validate(value).model_dump(exclude_unset=True)
    if kind=='wake' and result.get('wake_words') is not None:
        words=list(dict.fromkeys(w.strip() for w in result['wake_words'] if w.strip()))
        if any(len(w)>100 for w in words):raise ValueError('wake_word_too_long')
        result['wake_words']=words
    if result.get('prompt') is not None and not result['prompt'].strip():
        raise ValueError('empty_prompt')
    canonical(result)
    return result
