import React,{useEffect,useState} from 'react';
import {useQuery} from '@tanstack/react-query';
import {ChevronDown} from 'lucide-react';
import {newestFirst} from './display_order';

const enc=encodeURIComponent;
type Obj=Record<string,any>;
type Request=(path:string,method?:string,body?:unknown)=>Promise<any>;

/** manager 统一返回 envelope(scope, data)；页面只消费 data 部分。 */
export function unwrap(body:any){return body&&typeof body==='object'&&'data' in body?body.data:body}

async function get(path:string){
  const resp=await fetch('/manage/v1'+path,{credentials:'same-origin'});
  if(!resp.ok){const detail=await resp.json().catch(()=>({}));throw new Error(detail?.detail?.code||detail?.detail||`HTTP ${resp.status}`)}
  return unwrap(await resp.json());
}

function fmt(value:any){if(!value)return '';const d=new Date(value);return Number.isNaN(d.valueOf())?String(value):d.toLocaleString('zh-CN',{hour12:false})}

export function Bubble({item}:{item:Obj}){
  const mine=item.role==='assistant';
  const [open,set]=useState(false);
  const tools:Obj[]=item.tool_evidence||[];
  const name=mine?(item.display_name||'大肥鱼'):(item.display_name||item.sender_ref||'群成员');
  return <div style={{display:'flex',gap:8,justifyContent:mine?'flex-end':'flex-start',margin:'10px 0'}}>
    {!mine&&<span style={{width:30,height:30,borderRadius:15,background:'#e9dcff',color:'#5b3f92',display:'flex',alignItems:'center',justifyContent:'center',fontSize:12,flex:'0 0 30px'}}>{String(name)[0]}</span>}
    <div style={{maxWidth:'78%'}}>
      <div style={{fontSize:12,color:'#777',marginBottom:3,textAlign:mine?'right':'left'}}>
        {name} · {fmt(item.occurred_at)}{item.kind&&item.kind!=='text'?` · ${item.kind}`:''}
      </div>
      <div style={{padding:'9px 12px',borderRadius:12,whiteSpace:'pre-wrap',wordBreak:'break-word',lineHeight:1.55,
        background:mine?'#d9f5ea':'#f1eaff',border:mine?'1px solid #b7e6d5':'1px solid #ddd0ff'}}>
        {item.text}
      </div>
      {tools.length>0&&<div style={{marginTop:4}}>
        <button className="secondary" onClick={()=>set(!open)} aria-expanded={open} style={{fontSize:12}}>
          <ChevronDown size={13}/> 工具调用（{tools.length}）
        </button>
        {open&&<div style={{background:'#fff',border:'1px solid #e6e6e6',borderRadius:8,padding:8,marginTop:4}}>
          {tools.map((t,i)=><div key={i} style={{fontSize:12,margin:'3px 0'}}>
            <strong>{t.tool_name}</strong> · {t.status} · {t.duration_ms??0}ms
            {(t.started_at||t.finished_at||t.created_at)?` · ${fmt(t.started_at||t.finished_at||t.created_at)}`:''}
            {t.summary&&<div style={{color:'#555'}}>{String(t.summary).slice(0,180)}</div>}
            {t.error_code&&<small style={{color:'#b04'}}>{t.error_code}</small>}
          </div>)}
        </div>}
      </div>}
    </div>
    {mine&&<span style={{width:30,height:30,borderRadius:15,background:'#c9f0e2',color:'#1d6f57',display:'flex',alignItems:'center',justifyContent:'center',fontSize:12,flex:'0 0 30px'}}>{String(name)[0]}</span>}
  </div>
}

function ChatStream({scope}:{scope:string}){
  const limit=40;
  const win=useQuery({queryKey:['chat-window',scope,limit],queryFn:()=>get(`/scopes/${enc(scope)}/chat/window?limit=${limit}`),refetchInterval:5000});
  const items:Obj[]=(win.data?.items||[]);
  // 独立固定的历史边界：只在首次有窗口或用户主动切换时建立，不随 5 秒轮询漂移。
  const [boundary,setBoundary]=useState(0);
  const [offset,setOffset]=useState(0);
  useEffect(()=>{
    if(boundary===0&&items.length){setBoundary(Number(items[0].seq))}
  },[items,boundary]);
  const history=useQuery({
    queryKey:['chat-history',scope,boundary,offset],
    enabled:boundary>0,
    queryFn:()=>get(`/scopes/${enc(scope)}/chat/history?limit=20&offset=${offset}&before_seq=${boundary}`),
  });
  if(win.isPending)return <div className="empty">正在读取聊天…</div>;
  if(win.isError)return <div className="empty">读取失败：{win.error.message} <button onClick={()=>win.refetch()}>重试</button></div>;
  const windows=win.data?.windows||{};
  return <>
    <h2>原始聊天</h2>
    <p className="muted">最新 {items.length} 条 · 最新消息在前 · 共 {win.data?.counts?.messages??0} 条，较早消息在下方分页查看。</p>
    <div>{items.length===0?<div className="empty">还没有聊天记录。</div>:newestFirst(items,undefined,true).map((item:Obj)=><Bubble key={item.event_id||item.seq} item={item}/>)}</div>
    <h3>更早记录{boundary?`（早于序号 ${boundary}）`:''}</h3>
    {!boundary?<div className="empty">还没有更早的记录。</div>:history.isPending?<div className="empty">读取中…</div>:history.isError?<div className="empty">读取失败：{history.error.message}</div>:<>
      <div>{(history.data?.items||[]).map((item:Obj)=><Bubble key={'h'+item.event_id} item={item}/>)}
      {(history.data?.items||[]).length===0&&<div className="empty">没有更早的记录。</div>}</div>
      <div className="pager">
        <button disabled={!offset} onClick={()=>setOffset(Math.max(0,offset-20))}>上一页</button>
        <span>第 {offset/20+1} 页{history.data?.total!==undefined?` · 本边界内共 ${history.data.total} 条`:''}</span>
        <button disabled={(history.data?.items||[]).length<20} onClick={()=>setOffset(offset+20)}>下一页</button>
      </div>
    </>}
  </>;
}

export function Facts({scope,request}:{scope:string,request:Request}){
  const [status,setStatus]=useState('active');
  const q=useQuery({queryKey:['l3',scope,status],queryFn:()=>get(`/scopes/${enc(scope)}/chat/l3?status=${status}&limit=100`)});
  const [msg,setMsg]=useState('');
  const [busy,setBusy]=useState('');
  const items:Obj[]=newestFirst(q.data?.items||[],['updated_at','created_at']);
  async function change(fact:Obj,next:string){
    let replacedBy='';
    if(next==='replaced'){
      const options=items.filter((x:Obj)=>x.fact_id!==fact.fact_id)
        .map((x:Obj)=>`${x.fact_id} | ${String(x.content).slice(0,30)}`).join('\n');
      const picked=window.prompt('输入替代它的有效事实 ID（可选列表见下）：\n'+(options||'（当前没有其他有效事实）'),'');
      if(!picked){return}
      replacedBy=picked.trim();
    }
    const reason=window.prompt(next==='revoked'?'撤回原因（可留空）':'替代原因（可留空）','')??'';
    setBusy(fact.fact_id);setMsg('');
    try{
      await request(`/scopes/${enc(scope)}/chat/l3/${enc(fact.fact_id)}/status`,'POST',{status:next,reason,replaced_by:replacedBy});
      setMsg(`已更新为 ${next}；旧记录保留，检索只使用有效事实。`);
      await q.refetch();
    }catch(e){setMsg('更新失败：'+String(e))}finally{setBusy('')}
  }
  if(q.isPending)return <div className="empty">正在读取事实…</div>;
  if(q.isError)return <div className="empty">读取失败：{q.error.message}</div>;
  return <>
    <div className="tabs">{[['active','有效'],['candidate','候选'],['replaced','被替代'],['revoked','已撤回']].map(([v,t])=><button key={v} className={status===v?'selected':''} onClick={()=>setStatus(v)}>{t}</button>)}</div>
    {msg&&<p className="muted">{msg}</p>}
    {items.length===0&&<div className="empty">这一类别暂时没有事实。</div>}
    {items.map((f:Obj)=><article key={f.fact_id}>
      <small>{f.display_name||f.identity_id} · {f.status} · {f.record_mode||'explicit'} · {fmt(f.updated_at)}</small>
      <p>{f.content}</p>
      <small>证据：{f.evidence_quote}</small>
      {f.replaces_fact_id&&<small> · 替代了 {f.replaces_fact_id}</small>}
      {f.valid_until&&<small> · 失效于 {fmt(f.valid_until)}</small>}
      {f.status==='active'&&<div style={{marginTop:6}}>
        <button className="secondary" disabled={busy===f.fact_id} onClick={()=>change(f,'revoked')}>撤回</button>{' '}
        <button className="secondary" disabled={busy===f.fact_id} onClick={()=>change(f,'replaced')}>标记被替代</button>
      </div>}
    </article>)}
  </>;
}

export function Events({scope}:{scope:string}){
  const q=useQuery({queryKey:['events',scope],queryFn:()=>get(`/scopes/${enc(scope)}/chat/events?limit=50`),refetchInterval:10000});
  if(q.isPending)return <div className="empty">正在读取事件…</div>;
  if(q.isError)return <div className="empty">读取失败：{q.error.message}</div>;
  const items:Obj[]=newestFirst(q.data?.items||[]);
  if(!items.length)return <div className="empty">还没有整理出事件。</div>;
  return <>{items.map((e:Obj)=><article key={e.event_id}>
    <small>{e.status} · {fmt(e.planned_at||e.occurred_at||e.created_at)} · 参与者 {(e.participants||[]).map((p:string)=>String(p).includes(':')?String(p).split(':').pop():p).join('、')||'—'}</small>
    <strong>{e.title}</strong>
    <p>{e.detail}</p>
    <small>来源序号 {(e.source_seqs||[]).join(',')||'—'}</small>
  </article>)}</>;
}

export function ChatPage({scope,request}:{scope:string,request:Request}){
  return <ChatStream key={scope} scope={scope}/>;
}
export function RecoveryTasks({scope,request}:{scope:string,request:Request}){
 const [message,setMessage]=useState('');
 const q=useQuery({queryKey:['chat-tasks',scope],queryFn:()=>get(`/scopes/${enc(scope)}/chat/tasks`),refetchInterval:15000});
 return <details><summary>后台整理任务与重试</summary>{q.isPending?<p>读取中…</p>:q.isError?<p>读取失败：{q.error.message}</p>:(q.data?.items||[]).map((t:Obj)=><div key={t.kind}><span>{t.kind} · {t.state} · 失败 {t.attempts}/{t.max_attempts} · {t.error_code||'无错误'}</span>{(t.state==='failed'||t.state==='blocked')&&<button onClick={async()=>{if(!window.confirm('重试会重新允许后台模型请求，可能产生费用。确认？'))return;try{await request(`/scopes/${enc(scope)}/chat/tasks/retry`,'POST',{kind:t.kind});setMessage('已恢复为待处理，后台将按调度执行');await q.refetch()}catch(e){setMessage(String(e))}}}>允许重试</button>}</div>)}<p role="status">{message}</p></details>;
}
function ChatContent({scope,request}:{scope:string,request:Request}){
  const [tab,setTab]=useState<'chat'|'l3'|'events'>('chat');
  return <>
    <RecoveryTasks scope={scope} request={request}/>
    <div className="tabs">
      <button className={tab==='chat'?'selected':''} onClick={()=>setTab('chat')}>聊天流水</button>
      <button className={tab==='l3'?'selected':''} onClick={()=>setTab('l3')}>人物事实</button>
      <button className={tab==='events'?'selected':''} onClick={()=>setTab('events')}>事件</button>
    </div>
    {tab==='chat'&&<ChatStream scope={scope}/>}
    {tab==='l3'&&<Facts scope={scope} request={request}/>}
    {tab==='events'&&<Events scope={scope}/>}
  </>;
}
