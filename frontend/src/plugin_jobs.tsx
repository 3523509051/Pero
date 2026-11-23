import {useEffect,useState} from 'react';
import {Clock3,RefreshCw,CheckCircle2,AlertCircle,ChevronDown} from 'lucide-react';
import {newestFirst} from './display_order';

type Obj=Record<string,any>;
const activeStates=['queued','running','delivering'];
const processing:Obj={queued:'等待处理',running:'正在处理',delivering:'处理结束',completed:'处理完成',failed:'处理失败',cancelled:'已取消',interrupted:'重启中断'};
const delivery:Obj={pending:'等待回报',sent:'已发送回复',candidate:'等待发送（自动发送未开启）',permission_changed:'未回报：会话授权或服务状态变化',not_sent:'未发送回复',unknown:'发送结果待确认'};
const errors:Obj={job_timeout:'处理超时',delivery_timeout:'回报超时，未自动重发',delivery_failed:'回报异常，未自动重发',job_cancelled:'任务已取消',service_restarted:'服务重启导致中断'};
import{fmtTime,tsMs}from './time';
function format(value:string){return fmtTime(value,'时间未提供')}
function duration(job:Obj){
  const start=tsMs(job.created_at),end=activeStates.includes(job.state)?Date.now():tsMs(job.updated_at);
  if(start==null||end==null)return '耗时未知';
  const seconds=Math.max(0,Math.round((end-start)/1000));return seconds<60?`${seconds} 秒`:`${Math.floor(seconds/60)} 分 ${seconds%60} 秒`;
}
function attention(job:Obj){return !activeStates.includes(job.state)&&(job.state!=='completed'||job.delivery_state!=='sent')}

export function PluginJobsPanel({query,scopes,busy,onCancel,onDelete,onClear}:{query:any,scopes:Obj[],busy:boolean,onCancel:(id:string)=>Promise<void>,onDelete:(id:string)=>Promise<void>,onClear:()=>Promise<void>}){
  const [filter,setFilter]=useState('all'),[scope,setScope]=useState(''),[page,setPage]=useState(0);
  const items:Obj[]=newestFirst(query.data?.items||[],['created_at']);
  const running=items.filter(job=>activeStates.includes(job.state));
  const needsAttention=items.filter(attention);
  const filtered=items.filter(job=>(!scope||job.conversation_id===scope)&&(
    filter==='all'||(filter==='running'?activeStates.includes(job.state):filter==='attention'?attention(job):job.state==='completed'&&job.delivery_state==='sent')));
  const pages=Math.max(1,Math.ceil(filtered.length/10)),current=Math.min(page,pages-1);
  useEffect(()=>setPage(0),[filter,scope]);
  const scopeName=(cid:string)=>scopes.find(item=>item.conversation_id===cid)?.display_name||cid||'未知会话';
  const choices=[...new Set(items.map(job=>String(job.conversation_id||'')).filter(Boolean))];
  return <section className="card job-panel">
    <div className="job-heading"><div><h2>后台任务</h2><p className="muted">最新任务在前 · 每 2.5 秒更新 · 处理完成和回复送达分别显示</p></div>
      <div className="job-actions"><button className="secondary" disabled={busy||!items.some(job=>!activeStates.includes(job.state))} onClick={()=>{if(window.confirm('清空所有会话中已结束的任务记录？聊天、记忆和视频缓存都会保留；进行中的任务不受影响。'))void onClear()}}>清空已结束任务</button><button className="secondary" disabled={query.isFetching} onClick={()=>void query.refetch()} aria-label="刷新后台任务"><RefreshCw size={15} className={query.isFetching?'job-spin':''}/> 刷新</button></div></div>
    <div className="job-summary"><span><Clock3 size={15}/> {query.data?.active??running.length} 项进行中</span><span><CheckCircle2 size={15}/> {items.filter(job=>job.state==='completed'&&job.delivery_state==='sent').length} 项已回报</span><span><AlertCircle size={15}/> {needsAttention.length} 项待留意</span></div>
    <div className="job-toolbar"><div className="tabs" aria-label="任务状态筛选">{[['all','全部'],['running','进行中'],['attention','待留意'],['sent','已回报']].map(([value,title])=><button key={value} className={filter===value?'selected':''} onClick={()=>setFilter(value)}>{title}</button>)}</div>
      <label>会话 <select aria-label="筛选后台任务会话" value={scope} onChange={event=>setScope(event.target.value)}><option value="">全部会话</option>{choices.map(cid=><option value={cid} key={cid}>{scopeName(cid)}</option>)}</select></label></div>
    {query.error&&<p role="alert">读取任务失败：{String(query.error.message||query.error)} <button className="secondary" onClick={()=>void query.refetch()}>重试</button></p>}
    {query.isPending?<div className="empty">正在读取后台任务…</div>:<div className="job-list">{filtered.slice(current*10,current*10+10).map(job=>{
      const active=activeStates.includes(job.state),good=job.state==='completed'&&job.delivery_state==='sent';
      return <article className={'job-card '+(active?'active':good?'complete':'attention')} key={job.job_id}>
        <div className="job-heading"><div><strong>{job.tool_name||'插件任务'}</strong><p className="muted">{scopeName(job.conversation_id)} · {format(job.created_at)}</p></div><span className={'badge '+(good?'good':'')}>{processing[job.state]||job.state}</span></div>
        <div className="job-meta"><span>{active?<Clock3 size={15}/>:good?<CheckCircle2 size={15}/>:<AlertCircle size={15}/>} {job.state==='delivering'?'正在回报结果':delivery[job.delivery_state]||job.delivery_state||'回报状态未提供'}</span><span>总耗时 {duration(job)}</span>{typeof job.cache_hit==='boolean'&&<span>{job.cache_hit?'复用本地缓存':'本次处理'}</span>}</div>
        {(job.error_code||job.delivery_error_code)&&<p className="job-error">{[job.error_code,job.delivery_error_code].filter(Boolean).map(code=>errors[code]||code).join('；')}</p>}
        <div className="job-actions"><details><summary><ChevronDown size={14}/> 任务详情</summary><dl><dt>任务编号</dt><dd>{job.job_id}</dd><dt>插件</dt><dd>{job.plugin_id||'未提供'}</dd><dt>最后更新</dt><dd>{format(job.updated_at)}</dd><dt>投递状态</dt><dd>{job.delivery_state||'未提供'}</dd>{job.result_status&&<><dt>处理结果</dt><dd>{job.result_status==='partial'?'部分资料可用':job.result_status}</dd></>}{job.resource_refs?.length>0&&<><dt>结果资源</dt><dd>{job.resource_refs.join('、')}</dd></>}</dl></details>
          {active?<button className="secondary" disabled={busy} onClick={()=>{if(window.confirm('取消此后台任务？已发送的回复不会撤回。'))void onCancel(job.job_id)}}>取消任务</button>:<button className="secondary" disabled={busy} onClick={()=>{if(window.confirm('删除此任务的列表记录？聊天、记忆和视频缓存都会保留。'))void onDelete(job.job_id)}}>删除记录</button>}</div>
      </article>;
    })}{!filtered.length&&!query.error&&<div className="empty">{items.length?'没有符合筛选条件的任务。':'还没有后台任务。'}</div>}</div>}
    {filtered.length>10&&<div className="pager"><button className="secondary" disabled={current===0} onClick={()=>setPage(current-1)}>上一页</button><span>第 {current+1} / {pages} 页 · 共 {filtered.length} 项</span><button className="secondary" disabled={current>=pages-1} onClick={()=>setPage(current+1)}>下一页</button></div>}
  </section>;
}
