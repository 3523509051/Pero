"""Stable configured scope directory. Never open business SQLite or import a bridge."""
from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path

from pero.core.settings import Settings


class DirectoryUnavailable(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class Scope:
    scope_id: str
    channel_id: str
    account_ref: str
    conversation_type: str
    target_ref: str
    conversation_id: str
    display_name: str
    display_name_source: str
    current_agent_id: str
    agent_display_name: str
    allowed: bool

    def public(self):
        return asdict(self)


class ScopeDirectory:
    def __init__(self, config_path: Path, labels_path: Path | None = None):
        self.config_path, self.labels_path = Path(config_path), labels_path
        self._private_scopes = ()

    def load(self):
        if not self.config_path.is_file():
            raise DirectoryUnavailable('core_config_missing')
        try:
            config = Settings.model_validate(json.loads(self.config_path.read_text(encoding='utf-8-sig')))
        except (OSError, ValueError, TypeError):
            raise DirectoryUnavailable('core_config_invalid') from None
        labels = {}
        if self.labels_path is not None:
            try:
                labels = json.loads(Path(self.labels_path).read_text(encoding='utf-8-sig'))
                if not isinstance(labels, dict) or any(
                        not isinstance(k, str) or not isinstance(v, str) or not v.strip()
                        or len(v) > 120 for k, v in labels.items()):
                    raise ValueError('invalid labels')
            except (OSError, ValueError, TypeError):
                raise DirectoryUnavailable('scope_labels_invalid') from None
        agents = {a.agent_id: a for a in config.agents}
        scopes = []
        for route in config.orchestration.routes:
            if route.conversation_type == 'private':
                continue  # Private scope authority belongs to Core's grant directory.
            if route.agent_id not in agents:
                raise DirectoryUnavailable('scope_agent_missing')
            channel = 'onebot11'  # Current route contract is explicitly OneBot v11.
            cid = f'{channel}:{route.account}:{route.conversation_type}:{route.target}'
            digest = hashlib.sha256(json.dumps(
                [channel, route.account, route.conversation_type, route.target],
                ensure_ascii=True, separators=(',', ':')).encode()).hexdigest()[:24]
            label = labels.get(cid)
            fallback = ('群聊 ' if route.conversation_type == 'group' else '私聊 ') + route.target
            scopes.append(Scope('sc-' + digest, channel, route.account,
                route.conversation_type, route.target, cid, label or fallback,
                'configured_label' if label else 'target_fallback', route.agent_id,
                agents[route.agent_id].display_name, cid in config.orchestration.allowed_conversations))
        known = {scope.scope_id for scope in scopes}
        scopes.extend(scope for scope in self._private_scopes if scope.scope_id not in known)
        return sorted(scopes, key=lambda scope: scope.scope_id)

    def replace_private_scopes(self, items):
        try:
            scopes = tuple(Scope(**{key: item[key] for key in Scope.__dataclass_fields__}) for item in items)
            if any(s.conversation_type != 'private' for s in scopes):
                raise ValueError()
        except (TypeError, ValueError):
            self._private_scopes = ()
            raise DirectoryUnavailable('private_scope_invalid') from None
        self._private_scopes = scopes

    def get(self, scope_id):
        return next((scope for scope in self.load() if scope.scope_id == scope_id), None)
