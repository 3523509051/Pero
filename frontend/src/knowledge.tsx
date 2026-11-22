import React,{useState} from 'react';
import {useQuery} from '@tanstack/react-query';

const enc=encodeURIComponent;
type Obj=Record<string,any>;
type Request=(path:string,method?:string,body?:unknown)=>Promise<any>;

function unwrap(body:any){return body&&typeof body==='object'&&'data' in body?body.data:body}
async function get(path:string){const r=await fetch('/manage/v1'+path,{credentials:'same-origin'});if(!r.ok){const d=await r.json().catch(()=>({}));throw new Error(d?.detail?.code||`HTTP ${r.status}`)}return unwrap(await r.json())}
function fmt(v:any){if(!v)return '';const d=new Date(v);return Number.isNaN(d.valueOf())?String(v):d.toLocaleString('zh-CN',{hour12:false})}

function Docs({scope,request}:{scope:string,request:Request}){
  const [q,setQ]=useState(''),[offset,setOffset]=useState(0),[openId,setOpenId]=useState('');
  const list=useQuery({queryKey:['kb-docs',scope,q,offset],queryFn:()=>get(`/scopes/${enc(scope)}/knowledge/documents?query=${enc(q)}&limit=20&offset=${offset}`)});
  const [msg,setMsg]=useState('');
  async function act(doc:Obj,path:string,method:string,body?:unknown){
    setMsg('');
    try{await request(path,method,body);setMsg('已提交，列表将刷新。');await list.refetch()}
    catch(e){setMsg('操作失败：'+String(e))}
  }
  if(list.isPending)return <div className="empty">读取中…</div>;
  if(list.isError)return <div className="empty">读取失败：{list.error.message}</div>;
  const data=list.data||{};const items:Obj[]=data.items||[];
  return <>
    <div className="tabs">
      <input placeholder="按标题搜索" value={q} onChange={e=>{setQ(e.target.value);setOffset(0)}}/>
      <span className="muted">共 {data.total??0} 篇</span>
    </div>
    {msg&&<p className="muted">{msg}</p>}
    {items.length===0?<div className="empty">还没有知识文档。切到“新建”添加。</div>:items.map((d:Obj)=><article key={d.doc_id}>
      <small>{d.category||'未分类'} · {d.current_status||'pending'} · {d.chunk_count??0} 段 · {fmt(d.updated_at)}</small>
      <strong>{d.title}</strong>
      <p className="muted">{d.current_filename||'文本条目'} · 版本 {d.current_version?d.current_version.slice(-6):'—'}</p>
      <button className="secondary" onClick={()=>setOpenId(openId===d.doc_id?'':d.doc_id)}>{openId===d.doc_id?'收起片段':'查看片段'}</button>{' '}
      <button className="secondary" onClick={()=>act(d,`/scopes/${enc(scope)}/knowledge/documents/${enc(d.doc_id)}`,'PATCH',{enabled:!d.enabled})}>{d.enabled?'禁用':'启用'}</button>{' '}
      <button className="secondary" onClick={()=>act(d,`/scopes/${enc(scope)}/knowledge/documents/${enc(d.doc_id)}/reindex`,'POST')}>重建索引</button>{' '}
      <button className="secondary" onClick={()=>{if(window.confirm('确认删除该知识文档？（不会删除聊天资源）'))act(d,`/scopes/${enc(scope)}/knowledge/documents/${enc(d.doc_id)}`,'DELETE')}}>删除</button>
      {openId===d.doc_id&&<><DocumentVersion scope={scope} docId={d.doc_id} request={request} refresh={()=>{void list.refetch()}}/><Chunks scope={scope} docId={d.doc_id}/></>}
    </article>)}
    <div className="pager">
      <button disabled={!offset} onClick={()=>setOffset(Math.max(0,offset-20))}>上一页</button>
      <span>第 {offset/20+1} 页</span>
      <button disabled={offset+20>=(data.total??0)} onClick={()=>setOffset(offset+20)}>下一页</button>
    </div>
  </>;
}

function DocumentVersion({scope,docId,request,refresh}:{scope:string,docId:string,request:Request,refresh:()=>void}){
 const q=useQuery({queryKey:['kb-version',scope,docId],queryFn:()=>get(`/scopes/${enc(scope)}/knowledge/documents/${enc(docId)}`)});const [text,setText]=useState(''),[message,setMessage]=useState('');
 return <section className="config-editor"><h4>文档版本</h4>{q.isError?<p>{q.error.message}</p>:(q.data?.versions||[]).map((v:Obj)=><p key={v.version_id}>{v.filename} · {v.status} · {fmt(v.created_at)}</p>)}<label>新文本版本<textarea value={text} onChange={e=>setText(e.target.value)}/></label><button disabled={!text.trim()} onClick={async()=>{try{const r=unwrap(await request(`/scopes/${enc(scope)}/knowledge/documents/${enc(docId)}/versions`,'POST',{content:text,filename:'update.txt'}));setMessage(`版本状态 ${r.status}`);await q.refetch();refresh()}catch(e){setMessage(String(e))}}}>追加文本版本</button><p>{message}</p></section>
}
function Chunks({scope,docId}:{scope:string,docId:string}){
  const q=useQuery({queryKey:['kb-chunks',scope,docId],queryFn:()=>get(`/scopes/${enc(scope)}/knowledge/documents/${enc(docId)}/chunks?limit=10`)});
  if(q.isPending)return <div className="empty">读取片段…</div>;
  if(q.isError)return <div className="empty">读取失败：{q.error.message}</div>;
  const items:Obj[]=q.data?.items||[];
  return <div style={{marginTop:8}}>{items.map((c:Obj)=><div key={c.chunk_id} style={{background:'#f7f4ff',borderRadius:8,padding:8,margin:'6px 0',fontSize:13}}>
    <small>#{c.ordinal}{c.heading?` · ${c.heading}`:''} · 字符 {c.char_start}-{c.char_end}</small>
    <div style={{whiteSpace:'pre-wrap'}}>{String(c.text).slice(0,400)}</div>
  </div>)}</div>;
}

function Create({scope,request,onDone}:{scope:string,request:Request,onDone:()=>void}){
  const [title,setTitle]=useState(''),[category,setCategory]=useState(''),[content,setContent]=useState('');
  const [msg,setMsg]=useState(''),[busy,setBusy]=useState(false),[resource,setResource]=useState('');
  const resources=useQuery({queryKey:['kb-resource-picker',scope],queryFn:()=>get(`/scopes/${enc(scope)}/resources?limit=100`)});
  async function submit(){setBusy(true);setMsg('');try{
    const r=await request(`/scopes/${enc(scope)}/knowledge/documents/text`,'POST',{title,category,content});
    setMsg(`已建立索引（${(r.data?.chunks??0)} 段）。`);setTitle('');setContent('');onDone();
  }catch(e){setMsg('失败：'+String(e))}finally{setBusy(false)}}
  async function upload(file:File){setBusy(true);setMsg('');try{
    if(file.size>104857600)throw Error('文件超过100MiB');
    const session=await get('/auth/session');
    const response=await fetch(`/manage/v1/scopes/${enc(scope)}/knowledge/documents/upload-stream?filename=${enc(file.name)}&title=${enc(title||file.name)}&category=${enc(category)}`,{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/octet-stream','X-Pero-CSRF':session.csrf_token},body:file});
    if(!response.ok)throw Error(`上传失败 HTTP ${response.status}`);const r=await response.json();
    setMsg(`已保存原件（状态 ${r.data?.status}，${(r.data?.chunks??0)} 段）。`);onDone();
  }catch(e){setMsg('失败：'+String(e))}finally{setBusy(false)}}
  return <>
    <label>标题<input value={title} onChange={e=>setTitle(e.target.value)} maxLength={200}/></label>
    <label>分类<input value={category} onChange={e=>setCategory(e.target.value)} maxLength={60}/></label>
    <label>正文<textarea rows={10} value={content} onChange={e=>setContent(e.target.value)}/></label>
    <label>从现有群文件加入<select value={resource} onChange={e=>setResource(e.target.value)}><option value="">选择文件</option>{(resources.data?.items||[]).filter((x:Obj)=>x.kind==='document').map((x:Obj)=><option key={x.resource_id} value={x.resource_id}>{x.name}</option>)}</select></label><button disabled={!resource||busy} onClick={async()=>{setBusy(true);try{const r=unwrap(await request(`/scopes/${enc(scope)}/knowledge/documents/from-resource`,'POST',{resource_id:resource,title,category}));setMsg(`入库状态 ${r.status}`);onDone()}catch(e){setMsg(String(e))}finally{setBusy(false)}}}>加入知识库</button>
    <button disabled={busy||!title.trim()||!content.trim()} onClick={submit}>创建文本条目</button>
    <label>上传文件（文本、PDF、DOCX、PPTX、XLSX、EPUB 可解析；扫描件需另行OCR）
      <input type="file" disabled={busy} onChange={e=>{const f=e.target.files?.[0];if(f)void upload(f)}}/>
    </label>
    {msg&&<p className="muted">{msg}</p>}
  </>;
}

function Search({scope,request}:{scope:string,request:Request}){
  const [q,setQ]=useState(''),[result,setResult]=useState<any>(null),[busy,setBusy]=useState(false),[error,setError]=useState('');
  async function run(){setBusy(true);setError('');try{const r=await request(`/scopes/${enc(scope)}/knowledge/search`,'POST',{query:q,limit:5});setResult(unwrap(r))}catch(e){setError(String(e))}finally{setBusy(false)}}
  return <>
    <label>问题<input value={q} onChange={e=>setQ(e.target.value)}/></label>
    <button disabled={busy||!q.trim()} onClick={run}>检索</button>
    {error&&<p role="alert">{error}</p>}
    {result&&<><p className="muted">状态 {result.state} · 模式 {result.mode} · 命中 {(result.items||[]).length} 条</p>
      {(result.items||[]).map((item:Obj)=><article key={item.chunk_id}>
        <small>{item.title} · 版本 {String(item.version_id||'').slice(-6)} · #{item.ordinal}{item.heading?` · ${item.heading}`:''}{item.score!=null?` · 命中 ${item.score}`:''}</small>
        <p>{item.snippet}</p>
      </article>)}</>}
  </>;
}

function Settings({scope,request,onDone}:{scope:string,request:Request,onDone:()=>void}){
  const cfg=useQuery({queryKey:['kb-config',scope],queryFn:()=>get(`/scopes/${enc(scope)}/knowledge/config`)});
  const [msg,setMsg]=useState(''),[busy,setBusy]=useState(false);
  if(cfg.isPending)return <div className="empty">读取配置…</div>;
  if(cfg.isError)return <div className="empty">读取失败：{cfg.error.message}</div>;
  const c=cfg.data||{};
  async function save(values:Obj){setBusy(true);setMsg('');try{
    await request(`/scopes/${enc(scope)}/knowledge/config?revision=${c.revision??0}`,'PATCH',values);
    setMsg('已保存（revision 已更新）。');await cfg.refetch();onDone();
  }catch(e){setMsg('保存失败：'+String(e))}finally{setBusy(false)}}
  return <>
    <p className="muted">revision {c.revision??0} · 检索模式 {c.retrieval_mode}</p>
    <EmbeddingSettings key={scope+':'+c.revision} value={c.embedding} request={request} save={save}/>
    <label>检索模式<select value={c.retrieval_mode||'lexical'} onChange={e=>save({retrieval_mode:e.target.value})}><option value="lexical">关键词</option><option value="hybrid">关键词＋向量（需配置嵌入服务并重建索引）</option></select></label>
    <label><input type="checkbox" checked={!!c.enabled} onChange={e=>save({enabled:e.target.checked})}/> 启用本群知识检索</label>
    <label><input type="checkbox" checked={!!c.automatic_retrieval} onChange={e=>save({automatic_retrieval:e.target.checked})}/> 回答前自动检索</label>
    <label>top_k<input type="number" min={1} max={20} defaultValue={c.top_k??5} onBlur={e=>save({top_k:Number(e.target.value)})}/></label>
    <label>上下文预算<input type="number" min={1000} max={20000} defaultValue={c.context_char_budget??6000} onBlur={e=>save({context_char_budget:Number(e.target.value)})}/></label>
    <label>切分大小<input type="number" min={200} max={4000} defaultValue={c.chunk_size??800} onBlur={e=>save({chunk_size:Number(e.target.value)})}/></label>
    <label>重叠<input type="number" min={0} max={1000} defaultValue={c.chunk_overlap??100} onBlur={e=>save({chunk_overlap:Number(e.target.value)})}/></label>
    {busy&&<p className="muted">保存中…</p>}
    {msg&&<p className="muted">{msg}</p>}
  </>;
}

function EmbeddingSettings({value,request,save}:{value:any,request:Request,save:(v:Obj)=>Promise<void>}){
 const [local,setLocal]=useState(!value||String(value.base_url).startsWith('http://')),[url,setUrl]=useState(value?.base_url||'http://127.0.0.1:11434'),[model,setModel]=useState(value?.model||''),[dim,setDim]=useState(value?.dimension||1024),[key,setKey]=useState(''),[ref,setRef]=useState(value?.credential_ref||''),[busy,setBusy]=useState(false),[msg,setMsg]=useState('');
 async function submit(){setBusy(true);setMsg('');try{let next=local?'':ref;if(!local&&key.trim()){const creds=unwrap(await request('/credentials')).items||[];const old=creds.find((x:Obj)=>x.ref===ref);const r=unwrap(await request(old?'/credentials/'+ref:'/credentials',old?'PUT':'POST',{label:'知识库嵌入服务',value:key.trim(),expected_revision:old?.revision||0}));next=r.ref;setRef(next);setKey('')};if(!local&&!next)throw Error('需要嵌入API Key');await save({embedding:{base_url:local?'http://127.0.0.1:11434':url,model,dimension:Number(dim),credential_ref:next}});setMsg('已提交配置，需重建文档索引')}catch(e){setMsg(String(e))}finally{setBusy(false)}}
 return <details><summary>群级嵌入服务</summary><label>提供方<select value={local?'ollama':'remote'} onChange={e=>{setLocal(e.target.value==='ollama');setUrl(e.target.value==='ollama'?'http://127.0.0.1:11434':'')}}><option value="ollama">Ollama 本地（无需 Key）</option><option value="remote">远端 OpenAI 兼容</option></select></label>{local?<p className="muted">使用 Pero 所在服务器的 Ollama 11434 端口，向量索引与相似度搜索在本地 SQLite 完成。</p>:<label>OpenAI 兼容 API 地址<input value={url} onChange={e=>setUrl(e.target.value)} placeholder="https://example.com/v1"/></label>}<label>嵌入模型<input value={model} onChange={e=>setModel(e.target.value)} placeholder={local?'例如已安装的 embeddinggemma':'模型 ID'}/></label><label>向量维度<input type="number" value={dim} onChange={e=>setDim(Number(e.target.value))}/></label>{!local&&<label>API Key<input type="password" value={key} onChange={e=>setKey(e.target.value)} placeholder={ref?'已配置，留空保留':'输入嵌入Key'}/></label>}<button disabled={busy||(!local&&!url)||!model} onClick={()=>void submit()}>保存嵌入配置</button><button disabled={busy} onClick={()=>void save({embedding:null})}>恢复服务器默认嵌入配置</button><p role="status">{msg}</p></details>
}

function Jobs({scope,request,onDone}:{scope:string,request:Request,onDone:()=>void}){
  const q=useQuery({queryKey:['kb-jobs',scope],queryFn:()=>get(`/scopes/${enc(scope)}/knowledge/jobs?limit=30`),refetchInterval:5000});
  const [msg,setMsg]=useState('');
  if(q.isPending)return <div className="empty">读取任务…</div>;
  if(q.isError)return <div className="empty">读取失败：{q.error.message}</div>;
  const items:Obj[]=q.data?.items||[];
  return <>{msg&&<p className="muted">{msg}</p>}
    {items.length===0?<div className="empty">还没有索引任务。</div>:items.map((j:Obj)=><article key={j.job_id}>
      <small>{j.kind} · {j.state} · {j.progress}% · 尝试 {j.attempts} · {fmt(j.updated_at)}</small>
      <p>{j.error_code||'无错误'}</p>
      {j.state==='failed'&&<button className="secondary" onClick={async()=>{try{await request(`/scopes/${enc(scope)}/knowledge/jobs/${enc(j.job_id)}/retry`,'POST');setMsg('已重试。');await q.refetch();onDone()}catch(e){setMsg('重试失败：'+String(e))}}}>重试</button>}
    </article>)}</>;
}

export function KnowledgePage({scope,request}:{scope:string,request:Request}){
  const [tab,setTab]=useState<'docs'|'create'|'search'|'settings'|'jobs'>('docs');
  const status=useQuery({queryKey:['kb-status',scope],queryFn:()=>get(`/scopes/${enc(scope)}/knowledge/status`)});
  const s=status.data||{};
  return <>
    <div className="card" style={{marginBottom:10}}>
      <strong>知识库{ s.enabled?'已启用':'未启用'}</strong>
      <p className="muted">文档 {s.documents??0} · 可检索版本 {s.searchable_versions??0} · 片段 {s.chunks??0} · 失败任务 {s.failed_jobs??0} · 模式 {s.retrieval_mode||'lexical'}</p>
      <p className="muted">可索引格式：文本/Markdown/CSV/JSON 等；PDF/Office/EPUB 未接入解析器（保存原件但标记解析不可用）。</p>
    </div>
    <div className="tabs">
      {([['docs','文档'],['create','新建'],['search','检索体验'],['settings','设置'],['jobs','任务']] as const).map(([v,t])=>
        <button key={v} className={tab===v?'selected':''} onClick={()=>setTab(v)}>{t}</button>)}
    </div>
    {tab==='docs'&&<Docs scope={scope} request={request}/>}
    {tab==='create'&&<Create scope={scope} request={request} onDone={()=>{void status.refetch()}}/>}
    {tab==='search'&&<Search scope={scope} request={request}/>}
    {tab==='settings'&&<Settings scope={scope} request={request} onDone={()=>{void status.refetch()}}/>}
    {tab==='jobs'&&<Jobs scope={scope} request={request} onDone={()=>{void status.refetch()}}/>}
  </>;
}
