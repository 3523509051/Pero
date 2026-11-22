from __future__ import annotations
import asyncio
import hashlib
import json
import re
import secrets
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pero.memory.models import stable_identity_id

DEFAULT_PROMPT = '这是已经开通的一对一私聊。问题、追问、纠正、感受分享、连续对话、简短回应优先JOIN。连续纯表情可以PASS；纯图片不自动看图，结合本批文字决定。只输出JSON：{"decision":"JOIN或PASS"}。'
DEFAULT_POLICY = dict(quiet_seconds=5.0, max_wait_seconds=30.0, prompt=DEFAULT_PROMPT, failure_strategy='join', max_messages=20, max_chars=8000, window_seconds=30.0)

class AccessError(ValueError):
    def __init__(self, code, status=422):
        self.code, self.status = code, status
        super().__init__(code)

def now():
    return datetime.now(timezone.utc).isoformat()

def uid(prefix):
    return prefix + uuid.uuid4().hex

class AccessService:
    """Access state uses the memory store's one connection and RLock only.

    All disk methods run through run(); admission helpers only read an immutable
    snapshot published after a transaction commits. No private text is persisted.
    """
    def __init__(self, settings, memory):
        self.settings = settings
        self.store = getattr(memory, '_store', None)
        if self.store is None:
            raise AccessError('memory_unavailable', 503)
        self._snapshot = None
        self.on_change = None
        # Bootstrap is scheduled outside event loops by callers via run/initialize.

    async def run(self, method, *args, **kwargs):
        return await asyncio.to_thread(getattr(self, method), *args, **kwargs)

    async def initialize(self):
        await self.run('refresh')
        return self

    @contextmanager
    def transaction(self):
        with self.store._lock:
            conn = self.store._conn
            try:
                conn.execute('BEGIN IMMEDIATE')
                yield conn
                conn.commit()
                self._refresh_locked()
            except BaseException:
                conn.rollback()
                raise

    def refresh(self):
        with self.store._lock:
            self._refresh_locked()

    def _refresh_locked(self):
        previous = self._snapshot
        c = self.store._conn
        grants = [dict(r) for r in c.execute('SELECT * FROM private_chat_grant')]
        owner = c.execute('SELECT * FROM instance_owner').fetchone()
        state = c.execute('SELECT * FROM private_access_state WHERE singleton=1').fetchone()
        names = {r['identity_id']: r['alias'] for r in c.execute("SELECT identity_id,alias FROM identity_alias WHERE alias_type='display_name' ORDER BY observed_at")}
        owner = dict(owner) if owner else None
        for g in grants:
            g['revision'] = g['status_revision']
            name = names.get(g['identity_id'])
            g['display_name'] = name if name and name.strip().lower() not in {'none','null','unknown','undefined'} else '联系人 ' + g['user_ref']
            g['is_owner'] = bool(owner and owner['grant_id'] == g['grant_id'])
        self._snapshot = dict(grants=grants, owner=owner, revision=state['revision'],
            policy={**DEFAULT_POLICY, **json.loads(state['policy_json']), 'revision': state['revision']},
            groups=tuple(r[0] for r in c.execute('SELECT conversation_id FROM owner_group_access WHERE enabled=1')))
        if previous is not None and previous['revision'] != self._snapshot['revision'] and self.on_change is not None:
            self.on_change(previous, self._snapshot)

    def _state(self):
        # Fail closed until asynchronously initialized; never read disk here.
        return self._snapshot or dict(grants=[],owner=None,revision=0,groups=(),policy={**DEFAULT_POLICY,'revision':0})

    def grant_for(self, message):
        if message.conversation_type != 'private' or not message.sender_raw_ref:
            return None
        for g in self._state()['grants']:
            if (g['channel_id'],g['account_id'],g['user_ref'],g['user_wxid'],g['conversation_id']) == (message.channel_id,message.account_id,message.sender_ref,message.sender_raw_ref,message.conversation_id) and g['status']=='active':
                return dict(g)
        return None

    def authorizes(self, message):
        return self.grant_for(message) is not None

    def grant_for_conversation(self, conversation_id, include_inactive=False):
        return next((dict(g) for g in self._state()['grants'] if g['conversation_id']==conversation_id and (include_inactive or g['status']=='active')),None)

    def fingerprint(self, message):
        g = self.grant_for(message)
        if not g:
            return None
        s=self._state()
        policy = {key:value for key,value in s['policy'].items() if key != 'revision'}
        return (g['grant_id'], g['status_revision'], g['is_owner'],
                tuple(sorted(s['groups'])) if g['is_owner'] else (),
                json.dumps(policy, sort_keys=True, ensure_ascii=False))

    def allowed_groups(self, message):
        g = self.grant_for(message)
        valid={f'onebot11:{r.account}:group:{r.target}' for r in self.settings.orchestration.routes if r.conversation_type=='group' and str(r.account)==message.account_id}
        valid &= set(self.settings.orchestration.allowed_conversations)
        return tuple(cid for cid in self._state()['groups'] if cid in valid) if g and g['is_owner'] else ()

    def policy_snapshot(self):
        return dict(self._state()['policy'])

    def _bump(self,c):
        c.execute('UPDATE private_access_state SET revision=revision+1 WHERE singleton=1')

    def _check_revision(self,c,revision):
        if c.execute('SELECT revision FROM private_access_state WHERE singleton=1').fetchone()[0] != revision:
            raise AccessError('revision_conflict',409)

    def _audit(self,c,operation,target):
        c.execute('INSERT INTO access_audit_log(log_id,operation,actor_type,actor_ref,target_type,target_id,result,created_at) VALUES(?,?,?,?,?,?,?,?)',(uid('aa-'),operation,'internal','authenticated','access',target,'success',now()))

    def _agent(self,agent_id=None):
        agents=[a.agent_id for a in self.settings.agents]
        if agent_id is None:
            agent_id=agents[0] if agents else None
        if agent_id not in agents:
            raise AccessError('agent_not_found')
        return agent_id

    def _account(self,channel_id,account_id):
        if channel_id!='onebot11' or not any(str(r.account)==account_id for r in self.settings.orchestration.routes):
            raise AccessError('account_not_configured')

    def create_token(self,channel_id,account_id,label='',hours_valid=24,agent_id=None,request_id=None):
        self._account(channel_id,account_id)
        agent_id=self._agent(agent_id)
        if not 0<hours_valid<=720 or len(label)>200 or not request_id or len(request_id)>128:
            raise AccessError('invalid_token_options')
        with self.transaction() as c:
            old=c.execute('SELECT * FROM activation_token WHERE request_id=?',(request_id,)).fetchone()
            if old:
                if (old['channel_id'],old['account_id'],old['agent_id'],old['label'])!=(channel_id,account_id,agent_id,label):
                    raise AccessError('request_id_conflict',409)
                return {**self._token_public(old),'replayed':True}
            code='PERO-'+secrets.token_urlsafe(24)
            token_id=uid('pt-'); created=now(); expires=(datetime.now(timezone.utc)+timedelta(hours=hours_valid)).isoformat()
            c.execute('INSERT INTO activation_token(token_id,token_hash,channel_id,account_id,label,created_at,expires_at,agent_id,request_id) VALUES(?,?,?,?,?,?,?,?,?)',(token_id,hashlib.sha256(code.encode()).hexdigest(),channel_id,account_id,label,created,expires,agent_id,request_id))
            self._audit(c,'token_create',token_id)
            result=self._token_public(c.execute('SELECT * FROM activation_token WHERE token_id=?',(token_id,)).fetchone())
            return {**result,'code':code,'activation_code':code,'replayed':False}

    def _token_public(self,row):
        r=dict(row);r.pop('token_hash',None)
        r['status']='revoked' if r['revoked'] else 'used' if r['used_at'] else 'expired' if r['expires_at']<now() else 'unused'
        return r

    def list_tokens(self):
        with self.store._lock:
            return {'tokens':[self._token_public(r) for r in self.store._conn.execute('SELECT * FROM activation_token ORDER BY created_at DESC LIMIT 500')]}

    def revoke_token(self,token_id):
        with self.transaction() as c:
            if not c.execute('UPDATE activation_token SET revoked=1,revoked_at=? WHERE token_id=?',(now(),token_id)).rowcount:
                raise AccessError('token_not_found',404)
            self._audit(c,'token_revoke',token_id)
        return {'success':True}

    def redeem(self,channel_id,account_id,user_ref,user_wxid,activation_code,source_event_id,display_name=None):
        self._account(channel_id,account_id)
        if not re.fullmatch(r'[A-Za-z0-9_-]{3,128}',user_wxid or '') or user_wxid.isdigit() or user_wxid in {'unknown','None'} or user_wxid.endswith('@chatroom'):
            raise AccessError('trusted_wxid_required')
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',user_ref) or not source_event_id or len(source_event_id)>256:
            raise AccessError('invalid_identity')
        if not re.fullmatch(r'PERO-[A-Za-z0-9_-]{24,64}',activation_code.strip()):
            return {'success':False,'error':'invalid_code'}
        digest=hashlib.sha256(activation_code.strip().encode()).hexdigest()
        with self.transaction() as c:
            t=c.execute('SELECT * FROM activation_token WHERE token_hash=?',(digest,)).fetchone()
            if not t or t['channel_id']!=channel_id or t['account_id']!=account_id:
                return {'success':False,'error':'invalid_code'}
            identity_id=stable_identity_id(channel_id,account_id,user_ref)
            conflict=c.execute('SELECT 1 FROM identity WHERE channel_id=? AND account_id=? AND wxid=? AND bridge_user_id<>?',(channel_id,account_id,user_wxid,user_ref)).fetchone()
            if conflict:raise AccessError('identity_reference_conflict',409)
            g=c.execute('SELECT * FROM private_chat_grant WHERE channel_id=? AND account_id=? AND user_ref=?',(channel_id,account_id,user_ref)).fetchone()
            if g and g['user_wxid']!=user_wxid:
                raise AccessError('identity_conflict',409)
            if t['used_at']:
                if t['used_by_identity']!=identity_id or not g:
                    return {'success':False,'error':'code_used'}
                return self._redemption(c,g,True)
            if t['revoked'] or t['expires_at']<now():
                return {'success':False,'error':'invalid_code'}
            agent_id=self._agent(t['agent_id']); stamp=now();cid=f'{channel_id}:{account_id}:private:{user_ref}'
            existing=c.execute('SELECT wxid FROM identity WHERE identity_id=?',(identity_id,)).fetchone()
            if existing and existing['wxid'] and existing['wxid']!=user_wxid:
                raise AccessError('identity_conflict',409)
            c.execute("INSERT INTO identity(identity_id,channel_id,account_id,bridge_user_id,wxid,wxid_source,created_at,updated_at) VALUES(?,?,?,?,?,'bridge_extension',?,?) ON CONFLICT(identity_id) DO UPDATE SET wxid=excluded.wxid,wxid_source=excluded.wxid_source,updated_at=excluded.updated_at",(identity_id,channel_id,account_id,user_ref,user_wxid,stamp,stamp))
            c.execute("INSERT OR IGNORE INTO identity_wxid_evidence(identity_id,wxid,source,evidence_ref,observed_at) VALUES(?,?,'bridge_extension',?,?)",(identity_id,user_wxid,'event:'+source_event_id,stamp))
            if display_name:
                c.execute("INSERT INTO identity_alias(identity_id,alias,alias_type,scope_type,scope_ref,source,observed_at) VALUES(?,?,'display_name','account',?,'channel_display_name',?) ON CONFLICT(identity_id,alias,alias_type,scope_type,scope_ref) DO UPDATE SET observed_at=excluded.observed_at",(identity_id,display_name[:120],account_id,stamp))
            c.execute("INSERT INTO conversation(conversation_id,agent_id,channel_id,account_id,conversation_type,target_ref,created_at,updated_at) VALUES(?,?,?,?,'private',?,?,?) ON CONFLICT(conversation_id) DO UPDATE SET updated_at=excluded.updated_at",(cid,agent_id,channel_id,account_id,user_ref,stamp,stamp))
            if not g:
                gid=uid('pg-')
                c.execute("INSERT INTO private_chat_grant(grant_id,channel_id,account_id,user_ref,user_wxid,identity_id,agent_id,conversation_id,status,granted_via,token_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,'active','activation_code',?,?,?)",(gid,channel_id,account_id,user_ref,user_wxid,identity_id,agent_id,cid,t['token_id'],stamp,stamp))
                g=c.execute('SELECT * FROM private_chat_grant WHERE grant_id=?',(gid,)).fetchone()
            c.execute('UPDATE activation_token SET used_at=?,used_by_identity=? WHERE token_id=?',(stamp,identity_id,t['token_id']))
            c.execute("INSERT OR IGNORE INTO instance_owner(owner_id,channel_id,account_id,user_ref,user_wxid,identity_id,grant_id,display_name,bound_at,updated_at) VALUES('singleton',?,?,?,?,?,?,?,?,?)",(channel_id,account_id,user_ref,user_wxid,identity_id,g['grant_id'],display_name,stamp,stamp))
            self._bump(c);self._audit(c,'redeem',g['grant_id'])
            return self._redemption(c,g,False)

    def _redemption(self,c,g,replayed):
        owner=c.execute('SELECT grant_id FROM instance_owner').fetchone()
        return dict(success=True,grant_id=g['grant_id'],conversation_id=g['conversation_id'],agent_id=g['agent_id'],status=g['status'],status_revision=g['status_revision'],is_owner=bool(owner and owner[0]==g['grant_id']),replayed=replayed)

    def list_grants(self):
        self.refresh()
        return {'grants':[dict(g) for g in self._state()['grants']], 'revision':self._state()['revision']}

    def update_grant(self,grant_id,status,revision):
        if status not in {'active','paused','revoked'}:raise AccessError('invalid_status')
        with self.transaction() as c:
            g=c.execute('SELECT * FROM private_chat_grant WHERE grant_id=?',(grant_id,)).fetchone()
            if not g:raise AccessError('grant_not_found',404)
            if g['status_revision']!=revision:raise AccessError('revision_conflict',409)
            c.execute('UPDATE private_chat_grant SET status=?,status_revision=status_revision+1,updated_at=? WHERE grant_id=?',(status,now(),grant_id))
            self._bump(c);self._audit(c,'grant_'+status,grant_id)
        return next(g for g in self._state()['grants'] if g['grant_id']==grant_id)

    def get_owner(self):
        self.refresh();s=self._state()
        return {'owner':dict(s['owner']) if s['owner'] else None,'revision':s['revision']}

    def _scope(self,channel,account,kind,target,agent,display):
        cid=f'{channel}:{account}:{kind}:{target}'
        digest=hashlib.sha256(json.dumps([channel,account,kind,target],ensure_ascii=True,separators=(',',':')).encode()).hexdigest()[:24]
        a=next((a for a in self.settings.agents if a.agent_id==agent),None)
        return dict(scope_id='sc-'+digest,channel_id=channel,account_ref=account,account_id=account,conversation_type=kind,target_ref=target,conversation_id=cid,display_name=display,display_name_source='access_identity' if kind=='private' else 'target_fallback',current_agent_id=agent,agent_id=agent,agent_display_name=a.display_name if a else agent,allowed=True)

    def private_scopes(self):
        return [{**self._scope(g['channel_id'],g['account_id'],'private',g['user_ref'],g['agent_id'],g['display_name']), 'allowed':g['status']=='active', 'status':g['status']} for g in self._state()['grants']]

    def group_access(self):
        self.refresh();s=self._state();owner=s['owner'];groups=[]
        for r in self.settings.orchestration.routes:
            cid=f'onebot11:{r.account}:group:{r.target}'
            if r.conversation_type!='group' or cid not in self.settings.orchestration.allowed_conversations:continue
            if owner and (owner['channel_id']!='onebot11' or owner['account_id']!=str(r.account)):continue
            scope=self._scope('onebot11',str(r.account),'group',str(r.target),r.agent_id,'群聊 '+str(r.target))
            scope['enabled']=cid in s['groups'];groups.append(scope)
        return {'groups':groups,'conversation_ids':list(s['groups']),'revision':s['revision']}

    def update_group_access(self,conversation_ids,revision):
        valid={g['conversation_id'] for g in self.group_access()['groups']}
        if len(conversation_ids)!=len(set(conversation_ids)) or not set(conversation_ids)<=valid:raise AccessError('invalid_group_scope')
        with self.transaction() as c:
            self._check_revision(c,revision)
            if not c.execute('SELECT 1 FROM instance_owner').fetchone():raise AccessError('owner_not_bound',409)
            self._bump(c);c.execute('DELETE FROM owner_group_access');stamp=now()
            for cid in conversation_ids:c.execute('INSERT INTO owner_group_access(conversation_id,enabled,granted_at,updated_at,access_revision) VALUES(?,1,?,?,?)',(cid,stamp,stamp,revision+1))
            self._audit(c,'owner_group_access','singleton')
        return self.group_access()

    def get_policy(self):
        self.refresh();return self.policy_snapshot()

    def update_policy(self,revision,**values):
        policy={**DEFAULT_POLICY,**values}
        # window_seconds：私聊激活窗口时长（P04）。旧数据没有这个键，读取时按默认 30 补齐；
        # 显式校验 1–120 秒，不用硬编码覆盖用户已保存的静默合并秒数。
        try:
            window=float(policy.get('window_seconds',30))
        except (TypeError,ValueError):
            raise AccessError('invalid_policy') from None
        policy['window_seconds']=window
        if not 0<policy['quiet_seconds']<=policy['max_wait_seconds']<=120 or not isinstance(policy['prompt'],str) or not 1<=len(policy['prompt'])<=12000 or policy['failure_strategy']!='join' or not 1<=policy['max_messages']<=100 or not 100<=policy['max_chars']<=40000 or not 1<=window<=120:raise AccessError('invalid_policy')
        with self.transaction() as c:
            self._check_revision(c,revision)
            c.execute('UPDATE private_access_state SET policy_json=? WHERE singleton=1',(json.dumps(policy,ensure_ascii=False),));self._bump(c);self._audit(c,'policy_update','singleton')
        return self.policy_snapshot()

    def transfer_owner(self,grant_id,revision,confirm=False):
        if confirm is not True:raise AccessError('explicit_confirmation_required')
        with self.transaction() as c:
            self._check_revision(c,revision)
            g=c.execute("SELECT * FROM private_chat_grant WHERE grant_id=? AND status='active'",(grant_id,)).fetchone()
            if not g:raise AccessError('active_grant_required')
            if not c.execute('SELECT 1 FROM instance_owner').fetchone():raise AccessError('owner_not_bound',409)
            c.execute("UPDATE instance_owner SET channel_id=?,account_id=?,user_ref=?,user_wxid=?,identity_id=?,grant_id=?,display_name=NULL,updated_at=? WHERE owner_id='singleton'",(g['channel_id'],g['account_id'],g['user_ref'],g['user_wxid'],g['identity_id'],g['grant_id'],now()))
            c.execute('DELETE FROM owner_group_access');self._bump(c);self._audit(c,'owner_transfer',grant_id)
        return self.get_owner()

_factory_lock=threading.RLock()
def access_for(settings,memory):
    with _factory_lock:
        service=getattr(memory,'_private_access_service',None)
        if service is None:
            service=AccessService(settings,memory);memory._private_access_service=service
        return service

def service_for(runtime):
    return access_for(runtime.settings,runtime.memory)
