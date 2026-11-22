import React,{useState} from 'react';
import {useQuery,useQueryClient} from '@tanstack/react-query';
import {PluginJobsPanel} from './plugin_jobs';

type Obj=Record<string,any>;
type Req=(path:string,method?:string,body?:unknown)=>Promise<any>;
const STATUS:Obj={'enabled':'运行中','disabled':'已停用','failed':'故障','recovering':'待恢复'};

function SchemaFields({schema,value,onChange}:{schema:Obj,value:Obj,onChange:(key:string,v:any)=>void}){
  const props:Obj=schema?.properties||{};
  return <div className="form-grid">{Object.entries(props).map(([key,def]:[string,any])=>{
    const current=value[key]??def.default;
    if(def.type==='boolean')return <label key={key}>{def.title||key}<input type="checkbox" checked={!!current} onChange={e=>onChange(key,e.target.checked)}/></label>;
    if(def.type==='integer'||def.type==='number')return <label key={key}>{def.title||key}<input type="number" min={def.minimum} max={def.maximum} value={current??''} onChange={e=>onChange(key,e.target.value===''?undefined:Number(e.target.value))}/></label>;
    if(def.type==='string'&&Array.isArray(def.enum))return <label key={key}>{def.title||key}<select value={current??''} onChange={e=>onChange(key,e.target.value)}>{def.enum.map((x:string)=><option key={x} value={x}>{x}</option>)}</select></label>;
    return <label key={key}>{def.title||key}<input type="text" value={current??''} onChange={e=>onChange(key,e.target.value)}/></label>;
  })}</div>;
}

export default function PluginsPage({request}:{request:Req}){
  const qc=useQueryClient();
  const list=useQuery({queryKey:['/plugins'],queryFn:()=>request('/plugins'),retry:false});
  const scopes=useQuery({queryKey:['/scopes'],queryFn:()=>request('/scopes'),retry:false});
  const jobs=useQuery({queryKey:['/plugins/jobs'],queryFn:()=>request('/plugins/jobs'),retry:false,refetchInterval:2500});
  const [source,setSource]=useState('');
  const [hash,setHash]=useState('');
  const [error,setError]=useState('');
  const [busy,setBusy]=useState(false);
  const [draft,setDraft]=useState<Record<string,Obj>>({});
  const [scopeDraft,setScopeDraft]=useState<Record<string,Obj>>({});
  const [versionText,setVersionText]=useState<Record<string,string>>({});
  const [scope,setScope]=useState('');
  const [repoUrl,setRepoUrl]=useState('');
  const [repoRef,setRepoRef]=useState('');
  const [sourcePreview,setSourcePreview]=useState<Obj|null>(null);
  const [sourceError,setSourceError]=useState('');

  async function act(fn:()=>Promise<any>){
    setBusy(true);setError('');
    try{await fn();await qc.invalidateQueries({queryKey:['/plugins']})}
    catch(e){setError(String((e as Error).message||e))}
    finally{setBusy(false)}
  }
  const items:Obj[]=list.data?.items||[];
  const scopeItems:Obj[]=scopes.data?.items||[];

  return <>
    {error&&<p role="alert">{error}</p>}
    <PluginJobsPanel query={jobs} scopes={scopeItems} busy={busy}
      onCancel={id=>act(async()=>{await request('/plugins/jobs/'+id+'/cancel','POST',{});await jobs.refetch()})}
      onDelete={id=>act(async()=>{await request('/plugins/jobs/'+id,'DELETE');await jobs.refetch()})}
      onClear={()=>act(async()=>{await request('/plugins/jobs/finished','DELETE');await jobs.refetch()})}/>
    <section className="card">
      <h2>本地插件管理</h2>
      <p className="muted">这里管理已核对的本地/可信包；远程安装只接受下方明确指定的 GitHub 固定版本。安装默认停用，启用后从下一轮生效；不执行安装脚本。</p>
      <div className="form-row">
        <input aria-label="插件包目录" placeholder="服务器本机插件包目录，例如 D:\pero\example-plugin" value={source} onChange={e=>setSource(e.target.value)}/>
        <input aria-label="清单哈希" placeholder="plugin.json SHA256（可选）" value={hash} onChange={e=>setHash(e.target.value)}/>
        <button disabled={busy||!source.trim()} onClick={()=>act(()=>request('/plugins/install','POST',{source:source.trim(),expected_sha256:hash.trim()||null}))}>安装</button>
      </div>
      <p className="muted">“插件包目录”是 Core 进程所在服务器的本机路径，不是浏览器客户端路径。</p>
    </section>
    <section className="card">
      <h2>从 GitHub 固定版本安装</h2>
      <p className="muted">只接受明确的 HTTPS 仓库 URL 与 tag/commit；不接受默认分支。解析后显示真实 commit、摘要、能力与依赖，确认前不会安装或执行代码。远程包可以预览和安装，启用需要受支持的隔离运行环境。</p>
      <div className="form-row">
        <input aria-label="GitHub 仓库地址" placeholder="https://github.com/owner/repo" value={repoUrl} onChange={e=>setRepoUrl(e.target.value)}/>
        <input aria-label="固定版本" placeholder="tag 或 commit（必填）" value={repoRef} onChange={e=>setRepoRef(e.target.value)}/>
        <button disabled={busy||!repoUrl.trim()||!repoRef.trim()} onClick={async()=>{
          setBusy(true);setSourceError('');setSourcePreview(null);
          try{setSourcePreview(await request('/plugins/source/preview','POST',{url:repoUrl.trim(),ref:repoRef.trim()}))}
          catch(e){setSourceError(String((e as Error).message||e))}
          finally{setBusy(false)}
        }}>解析来源</button>
      </div>
      {sourceError&&<p role="alert">{sourceError}</p>}
      {sourcePreview&&<div className="detail">
        <p><strong>{sourcePreview.name}</strong> · {sourcePreview.id} · v{sourcePreview.version} · {sourcePreview.license}</p>
        <p className="muted">{sourcePreview.owner}/{sourcePreview.repo} · commit {sourcePreview.commit}</p>
        <p className="muted">工具：{(sourcePreview.tools||[]).join('、')} · 能力：{(sourcePreview.capabilities||[]).join('、')||'无'}</p>
        <p className="muted">后台工具：{(sourcePreview.background_tools||[]).join('、')||'无'}</p><p className="muted">公开网络域名：{(sourcePreview.network_hosts||[]).join('、')||'无'}</p>
        <p className="muted">依赖：{(sourcePreview.dependencies||[]).join('、')||'无'} · 包摘要 {String(sourcePreview.package_sha256||'').slice(0,16)}…</p>
        <button disabled={busy} onClick={()=>act(()=>request('/plugins/install-github','POST',{
          url:sourcePreview.owner&&sourcePreview.repo?`https://github.com/${sourcePreview.owner}/${sourcePreview.repo}`:repoUrl.trim(),
          commit:sourcePreview.commit, package_sha256:sourcePreview.package_sha256, manifest_sha256:sourcePreview.manifest_sha256}))}>确认安装（默认停用）</button>
      </div>}
    </section>
    <h2>已安装插件（{list.data?.installed??0} · 实际运行 {list.data?.enabled??0}）</h2>
    <p className="muted">SDK 版本 v1 · 工具名全局唯一 · 期望启用与实际注册分别显示，故障不挂空工具。</p>
    {list.isPending?<div className="empty">正在读取插件状态…</div>:list.isError?<div className="empty">读取失败：{String(list.error?.message||list.error)}</div>:
      items.length?<div className="rows">{items.map(p=>{
        const current=draft[p.id]??{...p.config};
        const overrides:Obj=p.scopes||{};
        return <article key={p.id} className="card">
        <header><strong>{p.name}</strong> <span className={'badge '+((p.status==='enabled')?'good':'')}>{STATUS[p.status]||p.status}</span>{p.desired_enabled&&!p.enabled&&<span className="badge">期望启用</span>}</header>
        <p>{p.description||'暂无简介'} · {p.id} · v{p.version} · {p.license} · 来源 {p.trust}</p>
        <p className="muted">工具：{(p.tools||[]).join('、')} · 能力：{(p.capabilities||[]).join('、')||'无'}</p>
        {p.network_hosts?.length>0&&<details><summary>公开网络授权（停用后修改）</summary>{p.network_hosts.map((host:string)=><label key={host}>{host}<input type="checkbox" disabled={busy||p.enabled} checked={(p.approved_network_hosts||[]).includes(host)} onChange={e=>act(()=>request('/plugins/'+encodeURIComponent(p.id)+'/network','PUT',{hosts:e.target.checked?[...(p.approved_network_hosts||[]),host]:(p.approved_network_hosts||[]).filter((h:string)=>h!==host)}))}/></label>)}</details>}
        {p.execution_blocked&&<p role="status">已安装，等待独立安全运行环境，暂不能启用。</p>}
        <p className="muted">实际注册：{(p.runtime?.tools||[]).join('、')||'无'} · {p.last_error?('最近故障：'+p.last_error):'状态正常'}</p>
        <details>
          <summary>插件配置（按声明 schema 渲染，保存后下一轮生效）</summary>
          <SchemaFields schema={p.config_schema} value={current} onChange={(k,v)=>setDraft({...draft,[p.id]:{...current,[k]:v}})}/>
          <button disabled={busy} onClick={()=>act(()=>request('/plugins/'+encodeURIComponent(p.id)+'/config','PATCH',{config:current}))}>保存配置</button>
        </details>
        <details>
          <summary>会话覆盖、版本更新与继承</summary>
          <p className="muted">未单独设置的会话继承实例启用状态；下列为显式覆盖：</p>
          <pre>{Object.keys(overrides).length?JSON.stringify(overrides,null,2):'无覆盖（全部继承）'}</pre>
          <label className="scope">会话 <select value={scope} onChange={e=>setScope(e.target.value)}>
            <option value="">请选择会话</option>
            {scopeItems.map(s=><option key={s.scope_id} value={s.scope_id}>{s.display_name||s.scope_id}</option>)}
          </select></label>
          <button disabled={busy||!scope} onClick={()=>act(()=>request('/scopes/'+encodeURIComponent(scope)+'/plugins/'+encodeURIComponent(p.id),'PUT',{enabled:false}))}>本会话停用</button>
          {' '}<button disabled={busy||!scope} onClick={()=>act(()=>request('/scopes/'+encodeURIComponent(scope)+'/plugins/'+encodeURIComponent(p.id),'PUT',{enabled:true}))}>本会话启用</button>
          {scope&&<div className="config-editor"><h4>当前会话独立配置</h4>
            <SchemaFields schema={p.config_schema} value={{...current,...(p.scope_configs?.[scopeItems.find(x=>x.scope_id===scope)?.conversation_id]||{}),...(scopeDraft[p.id+scope]||{})}} onChange={(k,v)=>setScopeDraft({...scopeDraft,[p.id+scope]:{...(p.scope_configs?.[scopeItems.find(x=>x.scope_id===scope)?.conversation_id]||{}),...(scopeDraft[p.id+scope]||{}),[k]:v}})}/>
            <button disabled={busy} onClick={()=>act(()=>request('/scopes/'+encodeURIComponent(scope)+'/plugins/'+encodeURIComponent(p.id),'PUT',{enabled:p.scopes?.[scopeItems.find(x=>x.scope_id===scope)?.conversation_id]??null,config:scopeDraft[p.id+scope]||p.scope_configs?.[scopeItems.find(x=>x.scope_id===scope)?.conversation_id]||{}}))}>保存会话配置</button>
            <button className="secondary" disabled={busy} onClick={()=>act(async()=>{await request('/scopes/'+encodeURIComponent(scope)+'/plugins/'+encodeURIComponent(p.id),'PUT',{enabled:null,config:{}});setScopeDraft({...scopeDraft,[p.id+scope]:{}})})}>恢复继承</button>
          </div>}
          <div className="form-row">
            <input aria-label={p.id+' 更新版本'} placeholder="目标版本" value={versionText[p.id]||''} onChange={e=>setVersionText({...versionText,[p.id]:e.target.value})}/>
            <button disabled={busy||!(versionText[p.id]||'').trim()} onClick={()=>act(()=>request('/plugins/'+encodeURIComponent(p.id)+'/update','POST',{version:versionText[p.id].trim()}))}>更新</button>
          </div>
        </details>
        <p>
          <button disabled={busy||(!p.desired_enabled&&!!p.execution_blocked)} onClick={()=>act(()=>request('/plugins/'+encodeURIComponent(p.id)+(p.desired_enabled?'/disable':'/enable'),'POST',p.desired_enabled?{}:{version:p.version}))}>{p.desired_enabled?'停用':'启用'}</button>
          {' '}<button className="secondary" disabled={busy} onClick={()=>{
            if(!window.confirm('确认卸载插件 '+p.id+'？插件数据目录会保留。'))return;
            void act(()=>request('/plugins/'+encodeURIComponent(p.id),'DELETE'));
          }}>卸载</button>
        </p>
      </article>})}</div>:<div className="empty">还没有安装插件。先安装上面的本地示例包进行验收。</div>}
  </>;
}
