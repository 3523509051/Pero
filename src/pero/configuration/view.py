"""Atomic configuration publication and immutable per-turn snapshots."""
from dataclasses import dataclass
import json
import uuid
import os
from pathlib import Path
from .models import ConfigurationPayload, canonical, validate_changes
from .catalog import validate_defaults, backend_enabled


@dataclass(frozen=True)
class ConfigurationSnapshot:
    generation: int = 0
    persona: str | None = None
    tools_json: str = '{}'
    source_id: str | None = None
    models_json: str = '{}'
    policy_json: str = '{}'
    wake_json: str = '{}'

    def model_configuration(self):
        return json.loads(self.models_json), json.loads(self.policy_json)

    def tool(self, name):
        return json.loads(self.tools_json).get(name, {})


class RuntimeConfiguration:
    def __init__(self, settings, registry, cache_path=None):
        self.settings, self.registry = settings, registry
        self._payload = None
        self.instance_id = uuid.uuid4().hex
        self.cache_path = None
        self.load_error = None
        self.scope_resolver = lambda: ()
        path = Path(cache_path) if cache_path is not None else None
        if path is not None and path.is_file():
            try:self.apply(json.loads(path.read_text(encoding='utf-8-sig')))
            except (ValueError, TypeError, OSError):self.load_error = 'runtime_configuration_cache_invalid'
        self.cache_path = path

    def state(self):
        current = self._payload
        return {'instance_id': self.instance_id, 'load_error':self.load_error,
            'source_id': current.source_id if current else None,
            'generation': current.generation if current else None,
            'digest': current.digest() if current else None}

    def apply(self, raw):
        candidate = ConfigurationPayload.model_validate(raw)
        current = self._payload
        if current and (current.source_id != candidate.source_id
                        or candidate.generation < current.generation):
            raise ValueError('configuration_source_or_revision_conflict')
        if current and candidate.generation == current.generation:
            if candidate.digest() != current.digest():
                raise ValueError('configuration_revision_conflict')
            return self.state()
        known = {f'onebot11:{r.account}:{r.conversation_type}:{r.target}'
                 for r in self.settings.orchestration.routes}
        known.update(self.scope_resolver())
        if set(candidate.personas) - known or set(candidate.tools) - (known | {'*'}) or set(candidate.wake)-known:
            raise ValueError('unknown_configuration_scope')
        values=list(candidate.personas.values())+list(candidate.models.values())+[candidate.policy]+[v for scope in candidate.tools.values() for v in scope.values()]
        if any(any(v is None for v in value.values()) for value in values):raise ValueError('snapshot_null_override')
        for value in candidate.models.values():validate_changes('model',value)
        validate_changes('policy',candidate.policy)
        known_profiles=set(candidate.models)|{'default'}
        if candidate.policy.get('default_profile','default') not in known_profiles:
            raise ValueError('model_profile_missing')
        for value in candidate.personas.values():
            validate_changes('persona', value)
        for value in candidate.wake.values():validate_changes('wake',value)
        for scope in candidate.tools.values():
            for name, value in scope.items():
                validate_changes('tool', value)
                if value.get('model_profile') and value['model_profile'] not in known_profiles:
                    raise ValueError('model_profile_missing')
                validate_defaults(name, value, self.registry)
                if value.get('enabled') is True and not self.registry.has(name):
                    raise ValueError('tool_backend_not_registered')
                if value.get('enabled') is True and not backend_enabled(self.settings,name):
                    raise ValueError('tool_backend_disabled')
        # Build/validate first; publishing one reference does not mutate active runs.
        normalized = ConfigurationPayload.model_validate(json.loads(canonical(candidate.model_dump())))
        if self.cache_path is not None:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.cache_path.with_suffix('.tmp')
            with temporary.open('w',encoding='utf-8') as stream:
                stream.write(canonical(normalized.model_dump()))
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(self.cache_path)
        self._payload = normalized
        self.load_error = None
        return self.state()

    def snapshot(self, conversation_id):
        current = self._payload
        if current is None:
            return ConfigurationSnapshot()
        tools = dict(current.tools.get('*', {}))
        for name, override in current.tools.get(conversation_id, {}).items():
            tools[name] = {**tools.get(name, {}), **override}
        persona = current.personas.get(conversation_id, {}).get('prompt')
        return ConfigurationSnapshot(current.generation, persona, canonical(tools), current.source_id,
                                     canonical(current.models),canonical(current.policy),canonical(current.wake.get(conversation_id,{})))
