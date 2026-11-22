import {useEffect,useState} from 'react';
import {Link} from 'react-router-dom';
import {Bubble,unwrap} from './chat';
import {newestFirst} from './display_order';

type Obj=Record<string,any>;
type Req=(path:string,method?:string,body?:unknown)=>Promise<any>;

const STATE_TEXT:Obj={queued:'待处理',running:'处理中',completed:'已完成',failed:'失败',interrupted:'中断'};

/** L2 独立交互记忆：未归档交互 + 归档前情 + 会话级归档配置。
 *
 * 数据全部来自真实后端（`/memory/l2`、`/memory/l2/status`、`/memory/l2/config`），
 * 不把本地缓存/拼图缓存当成模型前缀缓存；后端没给的值显示"暂不可用"。
 */
export function L2Panel({scope,request}:{scope:string;request:Req}){
  const [status,setStatus]=useState<Obj|null>(null);
  const [config,setConfig]=useState<Obj|null>(null);
  const [items,setItems]=useState<Obj[]>([]);
  const [offset,setOffset]=useState(0);
  const [draft,setDraft]=useState({trigger:'',retain:''});
  const [message,setMessage]=useState('');
  const [busy,setBusy]=useState(false);
  const base='/scopes/'+encodeURIComponent(scope);

  async function load(next:number){
    setBusy(true);
    try{
      const [s,c,page]=await Promise.all([
        request(base+'/memory/l2/status'),
        request(base+'/memory/l2/config'),
        request(base+'/memory/l2?limit=20&offset='+next),
      ]);
      const statusData=unwrap(s),configData=unwrap(c),pageData=unwrap(page);
      setStatus(statusData);setConfig(configData);
      setItems(pageData.items||[]);setOffset(next);
      setDraft({
        trigger:statusData?.config?.trigger_count??'',
        retain:statusData?.config?.retain_count??'',
      });
      // 不清空 message：保存/冲突提示在刷新后仍可见。
    }catch(e){setMessage('读取失败：'+String(e))}
    finally{setBusy(false)}
  }

  useEffect(()=>{setOffset(0);setMessage('');void load(0)},[scope]);

  async function save(reset:boolean){
    if(!config)return;
    setBusy(true);
    try{
      const body:Obj={expected_revision:Number(config.revision||0)};
      if(reset){body.trigger_count=null;body.retain_count=null}
      else{
        body.trigger_count=draft.trigger===''?null:Number(draft.trigger);
        body.retain_count=draft.retain===''?null:Number(draft.retain);
        if(body.trigger_count!==null&&body.retain_count!==null
           &&!(body.retain_count>=1&&body.retain_count<body.trigger_count)){
          setMessage('保存前检查：需要 1 ≤ 保留条数 < 触发条数');setBusy(false);return;
        }
      }
      await request(base+'/memory/l2/config','PATCH',body);
      setMessage(reset?'已恢复默认（继承全局 80/30）':'已保存 · 下一次归档生效');
      await load(offset);
    }catch(e){
      setMessage('保存失败：'+String(e)+'（revision_conflict 表示版本冲突，请刷新后重试）');
    }finally{setBusy(false)}
  }

  const effective=status?.effective||config?.effective||{};
  const counts=status?.counts||{};
  const budget=status?.budget||{};
  const archives:Obj[]=status?.archives||[];
  const total=Number(counts.total||0);

  return <div className="l2-panel">
    <h2>L2 · 独立交互记忆</h2>
    <p className="muted">
      只包含机器人实际参与的交互（用户 1 条、已确认送达的回复 1 条）；闲聊与未确认发送的内容不计入。
      当前会话：<code>{scope}</code>
    </p>
    <div className="l2-summary">
      <span className="chip">未归档 {counts.active??0} / {effective.trigger??'-'}</span>
      <span className="chip">保留至少 {effective.retain??'-'}</span>
      <span className="chip">本次计划归档 {effective.batch??'-'}</span>
      <span className="chip">已归档 {counts.archived??0}</span>
      <span className="chip">
        {status?.l2_load?.available
          ?`本次装载 ${status.l2_load.loaded_items} 条 / ${status.l2_load.chars} 字`
            +(status.l2_load.omitted_items?` · 省略 ${status.l2_load.omitted_items}`:'')
            +(status.l2_load.truncated?' · 最新一条已截断':'')
          :'本次装载：暂不可用（尚未对话）'}
      </span>
      <span className="chip">{effective.enabled===false?'归档已关闭':'归档已开启'}</span>
      <span className="chip">
        {status?.cache_usage?.available
          ?`缓存 token：命中 ${status.cache_usage.cache_hit_tokens} / 未命中 ${status.cache_usage.cache_miss_tokens}`
          :'模型缓存统计：暂不可用（供应商未返回 usage）'}
      </span>
      <button className="secondary" onClick={()=>void load(offset)} disabled={busy}>刷新</button>
    </div>

    <form className="l2-config" onSubmit={e=>{e.preventDefault();void save(false)}}>
      <label>触发条数<input type="number" min="2" max="500" value={draft.trigger}
        placeholder={String(status?.defaults?.trigger??80)}
        onChange={e=>setDraft({...draft,trigger:e.target.value})}/></label>
      <label>保留条数<input type="number" min="1" max="499" value={draft.retain}
        placeholder={String(status?.defaults?.retain??30)}
        onChange={e=>setDraft({...draft,retain:e.target.value})}/></label>
      <button type="submit" disabled={busy}>保存本会话配置</button>
      <button type="button" className="secondary" disabled={busy} onClick={()=>void save(true)}>恢复默认</button>
      <p className="muted">
        版本 {config?.revision??0} · 空值继承默认；
        触发={effective.source?.trigger==='conversation'?'会话覆盖':'全局默认'}，
        保留={effective.source?.retain==='conversation'?'会话覆盖':'全局默认'}
        {message?` · ${message}`:''}
      </p>
    </form>

    <h3>归档批次</h3>
    {status?.cache_usage?.available && <p className="muted">
      缓存统计：输入 {status.cache_usage.prompt_tokens} · 命中 {status.cache_usage.cache_hit_tokens} ·
      未命中 {status.cache_usage.cache_miss_tokens} · 命中率 {status.cache_usage.hit_ratio ?? '—'}
      （近 {status.cache_usage.samples} 次真实返回）
    </p>}
    {status?.l2_load?.available && <p className="muted">
      装载详情：预算 {status.l2_load.budget} 字 · 快照 {String(status.l2_load.snapshot_id||'').slice(0,16)} ·
      更新 {String(status.l2_load.updated_at||'').slice(0,19).replace('T',' ')}
    </p>}
    <p className="muted">装载记录：进程累计 {budget.loaded_items??0} 条 / 省略 {budget.omitted_items??0} 条（跨会话口径，非本会话数值）</p>
    {archives.length===0?<div className="empty">还没有归档记录（未达到触发条数）。</div>:
      <table className="l2-archives"><thead><tr>
        <th>范围</th><th>条数</th><th>状态</th><th>L4 记录</th><th>更新时间</th>
      </tr></thead><tbody>
        {archives.map((a:Obj)=><tr key={a.archive_id}>
          <td>seq {a.from_seq}–{a.to_seq}</td>
          <td>{a.interaction_count}</td>
          <td>{STATE_TEXT[a.state]||a.state}{a.error_code?`（${a.error_code}）`:''}</td>
          <td>{a.memory_id?<code>{a.memory_id}</code>:'—'}</td>
          <td>{String(a.updated_at||'').slice(0,19).replace('T',' ')}</td>
        </tr>)}
      </tbody></table>}

    <h3>交互原文 <span className="muted">（共 {total} 条，最新消息在前）</span></h3>
    {items.length===0?<div className="empty">暂时没有交互记录。</div>:
      newestFirst(items,undefined,true).map((r:Obj)=>{const archived=r.state==='archived';return <div key={r.interaction_id||r.event_id}>
        {archived&&<div className="muted l2-state">
          已归档 · L4 记录 {r.memory_id?<code>{r.memory_id}</code>:'—'}
          <Link to="/memory?layer=L4">在 L4 · 长期记忆中查看</Link>
        </div>}
        <Bubble item={{
          ...r,
          // 气泡组件按 role 区分本人/他人；归档状态单独标记。
          display_name:r.role==='assistant'?(r.display_name||'大肥鱼'):r.display_name,
          kind:r.kind,
          tool_evidence:r.tool_evidence,
        }}/>
      </div>;})}

    <div className="l2-pager">
      <button className="secondary" disabled={busy||offset===0}
        onClick={()=>void load(Math.max(0,offset-20))}>上一页</button>
      <span className="muted">{offset+1}–{offset+items.length} / {total}</span>
      <button className="secondary" disabled={busy||offset+items.length>=total}
        onClick={()=>void load(offset+20)}>下一页</button>
    </div>
  </div>;
}
