"""Explicit legacy group commands, separate from model tools and wake windows."""
import asyncio
from dataclasses import replace
from datetime import datetime,timezone
import json
import logging
from pathlib import Path
import re
import uuid

from pero.core.contracts import OutboundAction
from pero.memory.models import ConversationRef,InboundRecord,render_save_receipt

COMMANDS={'gaw','gactx','gasleep','gaclear','gamem','gaforget','gareload','gareset',
          'gaon','gaoff','gakb','gakbforget','garemember','gamemorypurge','gahelp'}
logger=logging.getLogger(__name__)


def options(owner):
    path=getattr(owner._settings,'config_path',None)
    if not path:return {}
    try:return json.loads(Path(path).read_text(encoding='utf-8-sig')).get('legacy_commands',{})
    except (OSError,ValueError):return {}


class CommandState:
    def __init__(self,owner):
        self.path=owner._settings.memory.data_root()/'legacy-command-state.json'
        self.boundaries=json.loads(self.path.read_text(encoding='utf-8')) if self.path.is_file() else {}
    def key(self,agent,cid):return agent+'|'+cid
    def cutoff(self,agent,cid):return self.boundaries.get(self.key(agent,cid))
    def reset(self,agent,cid):
        from pero.core.timebase import now_iso
        value=now_iso()
        candidate={**self.boundaries,self.key(agent,cid):value}
        self.path.parent.mkdir(parents=True,exist_ok=True)
        tmp=self.path.with_suffix('.tmp');tmp.write_text(json.dumps(candidate),encoding='utf-8');tmp.replace(self.path)
        self.boundaries=candidate


def state(owner):
    if not hasattr(owner,'_legacy_command_state'):owner._legacy_command_state=CommandState(owner)
    return owner._legacy_command_state


def filter_history(owner,loaded,agent,cid):
    cutoff=state(owner).cutoff(agent,cid)
    if not cutoff:return loaded
    turns=tuple(t for t in loaded.turns if t.created_at>=cutoff)
    return replace(loaded,turns=turns,used_chars=sum(t.history_cost for t in turns))


async def refresh_reset_history(owner,session,cid,agent,memory):
    if not state(owner).cutoff(agent,cid):return False
    from pero.core.contracts import ModelMessage,MessageRole
    # v6：优先用聊天流水重建（重置边界之前的消息不再进入上下文，流水本身保留）。
    try:items=await memory.chat_window(agent_id=agent,conversation_id=cid,limit=owner._settings.memory.chat_l2_messages)
    except Exception:items=[]
    cutoff=state(owner).cutoff(agent,cid)
    items=[item for item in items if str(item.get('occurred_at') or '')>=cutoff]
    if items and hasattr(owner,'_render_chat_window'):
        block,_shown,_cut=owner._render_chat_window(items)
        session.history=[ModelMessage(role=MessageRole.USER,content=block)]
        return True
    loaded=filter_history(owner,await memory.load_history(agent_id=agent,conversation_id=cid),agent,cid)
    messages=[]
    keep=max(1,owner._settings.runtime.max_history_messages//4)
    for turn in loaded.turns[-keep:]:
        messages.append(ModelMessage(role=MessageRole.USER,content=turn.render_user_content()))
        if turn.spoken:messages.append(ModelMessage(role=MessageRole.ASSISTANT,content=turn.assistant_text))
    session.history=messages
    return True


async def cancel_activity(owner,route,binding):
    cid=route.conversation_id if hasattr(route,'conversation_id') else f'onebot11:{route.account}:{route.conversation_type}:{route.target}'
    task=owner._workers.pop(cid,None)
    if task is not None and task is not asyncio.current_task():
        task.cancel()
        await asyncio.gather(task,return_exceptions=True)
    queue=owner._queues.pop(cid,None)
    if queue is not None:queue.drain()
    binding.policy._windows.pop((route.agent_id,cid),None)


def clear_buffers(owner,agent,cid):
    count=0
    for buffer in (owner._background,getattr(owner,'_l10',None)):
        if buffer is not None:
            queue=buffer._entries.pop((agent,cid),())
            count+=len(queue)
    return count


class _NoGate:
    async def __aenter__(self):return None
    async def __aexit__(self,*exc):return False


_NO_GATE=_NoGate()


def gate(owner,cid):
    """清理侧与插件回报提交段共用的串行边界；宿主无此接口时退化为无锁。"""

    acquire=getattr(owner,'conversation_gate',None)
    return acquire(cid) if callable(acquire) else _NO_GATE


def clear_model_history(owner,agent,cid,*,mark=True):
    session=owner._sessions_by_conversation.get(cid)
    if session is not None:session.history.clear()
    for name in ('_last_reply','_last_question','_last_senders'):
        getattr(owner,name,{}).pop(cid,None)
    owner._archive_revisions.pop((agent,cid),None)
    # 清理切点：早于此刻提交的后台任务，回报时不得把结果写回已清空的会话。
    if mark:
        marker=getattr(owner,'mark_conversation_cleared',None)
        if callable(marker):marker(agent,cid)
    # 本轮装载缓存同样要清：否则清理后仍会显示旧的 L2/背景/检索快照。
    # 注意：`_plugin_usage`（费用）与 `_cleared_at`（切点）**保留**——它们是
    # 计量与安全证据，不属于"会话记忆内容"。
    for name in ('_l2_event_ids','_l10_event_ids','_l2_last_load',
                 '_bystander_last','_retrieval_last','_usage_last'):
        bucket=getattr(owner,name,None)
        if isinstance(bucket,dict):
            bucket.pop((agent,cid),None)


async def mutate_memory(memory,agent,cid,layer):
    def execute(store):
        with store._txn() as cur:
            scope=(agent,cid)
            if layer in {'L3','L4'}:
                table,key=('l3_fact','fact_id') if layer=='L3' else ('l4_memory','memory_id')
                cur.execute(f'DELETE FROM vector_chunk WHERE agent_id=? AND conversation_id=? AND record_id IN (SELECT {key} FROM {table} WHERE agent_id=? AND conversation_id=?)',scope+scope)
                cur.execute(f'DELETE FROM {table} WHERE agent_id=? AND conversation_id=?',scope)
                return cur.rowcount
            total=cur.execute('SELECT COUNT(*) FROM raw_message WHERE agent_id=? AND conversation_id=?',scope).fetchone()[0]
            cur.execute('DELETE FROM archive_task_source WHERE agent_id=? AND task_id IN (SELECT task_id FROM archive_task WHERE agent_id=? AND conversation_id=?)',(agent,agent,cid))
            # 清理必须覆盖现行的群聊记忆表（chat_event/l2_interaction/l2_archive/
            # memory_event），否则清空后旧交互仍会被 L2 计数、检索与事件读到。
            for table in ('archive_task','vector_chunk','l3_fact','l3_fact_history',
                          'l4_memory','event_journal','l2_turn','raw_message',
                          'agent_session','l2_interaction','l2_archive','memory_event',
                          'memory_event_history','chat_event','chat_cursor',
                          'chat_archive','chat_task'):
                cur.execute(f'DELETE FROM {table} WHERE agent_id=? AND conversation_id=?',scope)
            return total
    return await memory._write(execute)


async def screen(binding,message,action):
    result,_data=await binding.channel.control_screen(action=action,account_id=message.account_id)
    return getattr(result.status,'value',str(result.status))=='ok'


async def wake_after_reply(owner,batch,binding):
    if options(owner).get('wake_screen_on_at',False) and any(item.message.mentions for item in batch):
        try:await screen(binding,batch[0].message,'wake_screen')
        except Exception:pass


def enqueue_command(owner,message,route,binding):
    match=re.fullmatch(r'/(ga[a-z]+)(?:\s+([\s\S]*))?',(message.text or '').strip(),re.I)
    if not match or match[1].lower() not in COMMANDS:return False
    name,args=match[1].lower(),(match[2] or '').strip()
    sending=owner._orchestration
    if (not sending.auto_send_enabled or message.account_id not in sending.send_accounts
            or message.target_ref not in sending.send_targets):
        owner._counters['commands_send_disabled']=owner._counters.get('commands_send_disabled',0)+1
        return True
    tasks=getattr(owner,'_legacy_command_tasks',{})
    owner._legacy_command_tasks=tasks
    previous=tasks.get(message.conversation_id)
    async def run():
        if previous is not None:await asyncio.gather(previous,return_exceptions=True)
        try:
            await execute(owner,message,route,binding,name,args)
            logger.info('legacy_command_handled command=%s cid=%s',name,message.conversation_id)
        except Exception as exc:
            logger.warning('legacy_command_failed command=%s cid=%s error=%s',name,message.conversation_id,type(exc).__name__)
            try:await reply(binding,message,'这条指令未完成，请查看后台状态；没有自动重试。')
            except Exception:pass
    task=asyncio.create_task(run());tasks[message.conversation_id]=task
    def done(_):
        if tasks.get(message.conversation_id) is task:tasks.pop(message.conversation_id,None)
    task.add_done_callback(done)
    owner._counters['commands_received']=owner._counters.get('commands_received',0)+1
    return True


async def close_commands(owner):
    tasks=list(getattr(owner,'_legacy_command_tasks',{}).values())
    for task in tasks:task.cancel()
    if tasks:await asyncio.gather(*tasks,return_exceptions=True)


async def reply(binding,message,text):
    return await binding.channel.send_action(OutboundAction(conversation_id=message.conversation_id,text=text,
        action_id='command-'+uuid.uuid4().hex),account_id=message.account_id,
        conversation_type=message.conversation_type,target_ref=message.target_ref)


async def execute(owner,message,route,binding,name,args):
    cid,agent=message.conversation_id,route.agent_id
    memory=binding.memory
    if name not in {'garemember','gamemorypurge'} and args:
        await reply(binding,message,'用法：/'+name);return
    if name=='gahelp':
        await reply(binding,message,'已恢复指令：\n'+', '.join('/'+c for c in sorted(COMMANDS))+'\n/garemember 内容：明确保存；/gamemorypurge confirm：仅管理员完整清除本群记忆。');return
    if name in {'gaon','gaoff'}:
        off='正在熄屏 🌙 机器人继续运行；可发 /gaon 开屏。'
        if options(owner).get('wake_screen_on_at',False):off='正在熄屏 🌙 机器人继续运行；可发 /gaon 或 @ 我开屏。'
        receipt=await reply(binding,message,'屏幕这就亮 🫧' if name=='gaon' else off)
        if getattr(receipt.status,'value',None)=='sent':
            ok=await screen(binding,message,'wake_screen' if name=='gaon' else 'sleep_screen')
            if not ok:await reply(binding,message,'屏幕动作尚未确认，请查看宿主机状态；没有自动重试。')
        return
    if name=='gaw':
        windows=[w for w in binding.policy.snapshot(agent_id=agent) if w['conversation_id']==cid]
        await reply(binding,message,'本群当前休眠。' if not windows else '本群接话窗口剩余 '+str(windows[0]['remaining_seconds'])+' 秒。');return
    if name=='gactx':
        text=owner._background.render(agent,cid) if owner._background is not None else ''
        await reply(binding,message,'本群即时背景：\n'+text if text else '本群即时背景为空。');return
    if name in {'gasleep','gaclear','gareset','gaforget','gakbforget','gamemorypurge'}:
        if name=='gamemorypurge':
            admins={str(v) for v in options(owner).get('admin_users',[])}
            if not ({message.sender_ref,message.sender_raw_ref} & admins):
                await reply(binding,message,'仅管理员可完整清除记忆。');return
            if args!='confirm':
                await reply(binding,message,'此操作清除本群原文、事项、日记、L20和长期记忆；确定时使用 /gamemorypurge confirm。');return
        await cancel_activity(owner,route,binding)
        if name=='gasleep':await reply(binding,message,'本群已休眠，旧批次和临时任务已取消。');return
        if name=='gaclear':
            n=clear_buffers(owner,agent,cid)
            await reply(binding,message,f'已清空本群 L1/L10（{n} 条）并休眠；长期记忆保留。');return
        if name=='gareset':
            # 串行边界：切点失效与插件回报提交段互斥，回执发出后旧任务必然看到新代数。
            async with gate(owner,cid):
                state(owner).reset(agent,cid);clear_model_history(owner,agent,cid)
            await reply(binding,message,'已重置本群 L2 对话起点，后续不回放之前的多轮上下文。原文、L3/L4 保留，可明确查询。');return
        archive=getattr(owner,'_archive_service',None)
        # 删数据同样在边界内：先标记代数失效，再删，最后才回复完成；
        # 期间不允许旧任务“边删边回填”。
        async with gate(owner,cid):
            mark=getattr(owner,'mark_conversation_cleared',None)
            if callable(mark):mark(agent,cid)
            if archive is not None:
                async with archive._guard:n=await mutate_memory(memory,agent,cid,'L3' if name=='gaforget' else 'L4' if name=='gakbforget' else 'all')
            else:n=await mutate_memory(memory,agent,cid,'L3' if name=='gaforget' else 'L4' if name=='gakbforget' else 'all')
            clear_model_history(owner,agent,cid,mark=False)
            if name=='gamemorypurge':
                clear_buffers(owner,agent,cid);state(owner).reset(agent,cid)
        if name=='gamemorypurge':
            await reply(binding,message,f'已清除本群原文、事项、日记、L20和长期记忆（原文 {n} 条）；其他群、图片和表情资产不受影响。')
        else:await reply(binding,message,f'已清空本群 {"L3 人物事实" if name=="gaforget" else "L4 长期知识"}（{n} 条）；原文与其他层保留。')
        return
    if name in {'gamem','gakb'}:
        layer='L3' if name=='gamem' else 'L4'
        page=await memory.query_records(agent_id=agent,conversation_id=cid,layer=layer,limit=20)
        lines=[str(item.get('preview') or '') for item in page['items']]
        await reply(binding,message,f'本群 {layer} 共 {page["total"]} 条：\n'+('\n'.join(lines) or '当前为空。')+('\n仅列前20条。' if page['total']>20 else ''));return
    if name=='gareload':
        from pero.core.settings import load_settings
        loaded=load_settings(owner._settings.config_path)
        fresh={a.agent_id:a.persona for a in loaded.agents}
        for configured in owner._settings.agents:
            if configured.agent_id in fresh:configured.persona=fresh[configured.agent_id]
        await reply(binding,message,'人格已从当前 Pero 配置重新载入；群级覆盖仍按管理配置生效。');return
    if name=='garemember':
        if not args:await reply(binding,message,'用法：/garemember 要记住的内容');return
        ref=ConversationRef(conversation_id=cid,agent_id=agent,channel_id=message.channel_id,
            account_id=message.account_id,conversation_type=message.conversation_type,target_ref=message.target_ref)
        turn=await memory.begin_turn(ref=ref,messages=[InboundRecord(source_event_id=message.event_id,
            sender_ref=message.sender_ref,text=message.text,display_name=message.display_name,
            received_at=message.received_at,dedupe_scope=message.dedupe_scope)])
        if not turn.created:await reply(binding,message,'这条指令已处理，未重复写入。');return
        outcome=await owner._save_one(memory=memory,ref=ref,turn=turn,record_id=turn.record_ids[0],original=message.text,content=args)
        text=render_save_receipt(outcome)
        receipt=await reply(binding,message,text)
        sent=getattr(receipt.status,'value',None)=='sent'
        await memory.finish_turn(turn_id=turn.turn_id,agent_id=agent,run_id='command-'+uuid.uuid4().hex,
            status='sent' if sent else 'generated',assistant_text=text if sent else None,
            delivery_status=receipt.status.value,action_id=receipt.action_id,delivery_evidence=receipt.evidence,
            delivery_text=text,tools=())
