import {newRequestId} from './request_id';
import React,{useEffect,useState} from 'react';
import {useQuery,useQueryClient} from '@tanstack/react-query';
type Obj=Record<string,any>;type Request=(path:string,method?:string,body?:unknown)=>Promise<any>;
export function ProviderPanel({request}:{request:Request}){
 const [mode,setMode]=useState('deepseek');
 return <><div className="tabs"><button className={mode==='deepseek'?'selected':''} onClick={()=>setMode('deepseek')}>DeepSeek 官方</button><button className={mode==='custom'?'selected':''} onClick={()=>setMode('custom')}>自定义兼容接口</button><button className={mode==='amap'?'selected':''} onClick={()=>setMode('amap')}>高德地图</button></div>{mode==='amap'?<AmapForm request={request}/>:<ModelForm key={mode} request={request} custom={mode==='custom'}/>}</>;
}
function ModelForm({request,custom}:{request:Request,custom:boolean}){
 const id=custom?'custom':'default',path='/models/'+id+'/config';const qc=useQueryClient();
 const data=useQuery({queryKey:['provider-form',id],queryFn:async()=>{const [doc,models,credentials,catalog]=await Promise.all([request(path),request('/models'),request('/credentials'),request('/provider-options')]);return {doc,models,credentials,catalog}},retry:false});
 const [model,setModel]=useState('deepseek-flash'),[url,setUrl]=useState(''),[key,setKey]=useState(''),[thinking,setThinking]=useState(false),[busy,setBusy]=useState(false),[message,setMessage]=useState(''),[ref,setRef]=useState('');
 useEffect(()=>{if(!data.data)return;const d=data.data;const value={...d.models.runtime?.profiles?.[id],...d.doc.value};setModel(value.model||(custom?'':'deepseek-flash'));setUrl(value.base_url||'https://api.deepseek.com');setThinking(value.reply_thinking===true);setRef(value.credential_ref||'')},[data.data,custom,id]);
 if(data.isPending)return <p>正在读取模型配置…</p>;if(data.isError)return <p role="alert">读取失败：{data.error.message} <button onClick={()=>data.refetch()}>重试</button></p>;
 const d=data.data;const credentials=d.credentials.items||[];const existing=credentials.find((x:Obj)=>x.ref===ref);const runtime=d.models.runtime?.profiles?.[id];
 async function save(){setBusy(true);setMessage('保存中…');let activeRef=ref;try{
   if(key.trim()){const old=credentials.find((x:Obj)=>x.ref===activeRef);const saved=await request(old?'/credentials/'+old.ref:'/credentials',old?'PUT':'POST',{label:custom?'自定义模型':'DeepSeek 官方',value:key.trim(),expected_revision:old?.revision||0});activeRef=saved.ref;setRef(activeRef);setKey('');await qc.invalidateQueries({queryKey:['/credentials']})}
   if(!activeRef&&!runtime?.credential_configured)throw Error('请先输入 API Key');
   const fresh=await request(path);const changes:Obj={model,base_url:custom?url:'https://api.deepseek.com',reply_thinking:thinking};if(activeRef)changes.credential_ref=activeRef;
   const result=await request(path,'PATCH',{expected_revision:fresh.revision,request_id:newRequestId(),changes});
   const policy=await request('/model-policy/config');const applied=await request('/model-policy/config','PATCH',{expected_revision:policy.revision,request_id:newRequestId(),changes:{default_profile:id}});
   setMessage(result.application?.state==='applied'&&applied.application?.state==='applied'?'已保存并应用，下轮回复生效':'已保存，等待后端应用');await data.refetch();await qc.invalidateQueries({queryKey:['/models']});
 }catch(e){setMessage('保存未完成：'+String(e)+'；已保存的 Key 不会自动删除，请重试配置保存。')}finally{setBusy(false)}}
 return <section className="config-editor"><h3>{custom?'自定义 OpenAI Chat Completions 兼容接口':'DeepSeek 官方模型'}</h3><p className="muted">当前默认配置：{d.models.policy?.default_profile||'default'}。保存此页将启用本页配置。</p>{custom&&<><label>API 地址<input disabled={busy} value={url} onChange={e=>setUrl(e.target.value)} placeholder="https://example.com/v1"/></label><p className="muted">仅接入兼容 Chat Completions 的服务；图像、工具和思考须由该服务支持。DeepSeek 文件、网页工具仍使用其独立服务凭据。</p></>}
 <label>模型{custom?<input disabled={busy} value={model} onChange={e=>setModel(e.target.value)} placeholder="服务商提供的模型 ID"/>:<select disabled={busy} value={model} onChange={e=>setModel(e.target.value)}>{d.catalog.deepseek.models.map((x:Obj)=><option key={x.id} value={x.id}>{x.label}</option>)}{!d.catalog.deepseek.models.some((x:Obj)=>x.id===model)&&<option value={model}>{model}（当前配置）</option>}</select>}</label>
 <label>API Key<input disabled={busy} type="password" autoComplete="new-password" value={key} onChange={e=>setKey(e.target.value)} placeholder={existing||runtime?.credential_configured?'已配置，留空继续使用；输入新 Key 替换':'输入 API Key'}/></label><small>{existing?'已保存加密凭据 '+existing.mask:runtime?.credential_configured?'后端已有环境凭据，可直接继续使用':'尚未配置凭据'}</small>
 <label>普通回复思考<select disabled={busy} value={String(thinking)} onChange={e=>setThinking(e.target.value==='true')}><option value="false">关闭思考</option><option value="true">开启思考</option></select></label><p className="muted">工具独立分析的思考设置在对应工具页调整。</p><button disabled={busy||!model.trim()||(custom&&!url.trim())} onClick={()=>void save()}>保存并使用</button><span role="status">{message}</span></section>
}
function AmapForm({request}:{request:Request}){
 const names=['amap_weather','amap_places','amap_route'];const labels=['天气','地点搜索','路线规划'];const [key,setKey]=useState(''),[busy,setBusy]=useState(false),[message,setMessage]=useState(''),[savedRef,setRef]=useState('');
 const data=useQuery({queryKey:['amap-key-form'],queryFn:async()=>({docs:await Promise.all(names.map(n=>request('/tools/'+n+'/config'))),credentials:(await request('/credentials')).items}),retry:false});
 if(data.isPending)return <p>读取高德工具配置…</p>;if(data.isError)return <p role="alert">高德配置读取失败：{data.error.message}</p>;
 async function save(){setBusy(true);setMessage('保存中…');try{let ref=savedRef;const d=data.data!;const refs=d.docs.map((x:Obj)=>x.desired_configuration?.credential_ref);const existing=refs[0]&&refs.every((x:string)=>x===refs[0])?d.credentials.find((x:Obj)=>x.ref===refs[0]):null;
  if(key.trim()){const result=await request(existing?'/credentials/'+existing.ref:'/credentials',existing?'PUT':'POST',{label:'高德 Web 服务 Key',value:key.trim(),expected_revision:existing?.revision||0});ref=result.ref;setRef(ref);setKey('')}
  if(!ref)throw Error('请先输入高德 Web 服务 Key');const states=[];
  for(let i=0;i<names.length;i++){const path='/tools/'+names[i]+'/config';try{const doc=await request(path);const result=await request(path,'PATCH',{expected_revision:doc.revision,request_id:newRequestId(),changes:{credential_ref:ref}});states.push(labels[i]+'：'+(result.application?.state==='applied'?'已应用':'待应用'))}catch(e){states.push(labels[i]+'：失败 '+String(e))}}
  setMessage(states.join('；'));await data.refetch();
 }catch(e){setMessage('保存未完成：'+String(e))}finally{setBusy(false)}}
 return <section className="config-editor"><h3>高德地图 · Web 服务 Key</h3><p>用于天气、地点和路线三个工具。各群独立工具凭据覆盖如已设置，仍优先于此处默认值。</p><label>高德 Key<input type="password" autoComplete="new-password" disabled={busy} value={key} onChange={e=>setKey(e.target.value)} placeholder="输入高德 Web 服务 Key"/></label>{data.data.docs.map((d:Obj,i:number)=><small key={names[i]}>{labels[i]}：{d.desired_configuration?.credential_ref?'已有独立凭据绑定':'沿用后端环境凭据（若已配置）'}</small>)}<button disabled={busy||(!key.trim()&&!savedRef)} onClick={()=>void save()}>保存到三个高德工具</button><span role="status">{message}</span></section>
}
