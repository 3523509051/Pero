import React,{useRef,useState} from 'react';
import {useQuery} from '@tanstack/react-query';
type Obj=Record<string,any>;
type Requester=(path:string,method?:string,body?:unknown)=>Promise<any>;
export default function StickerManager({scope,request}:{scope:string;request:Requester}){
 const base='/scopes/'+encodeURIComponent(scope)+'/stickers';
 const [offset,setOffset]=useState(0),[busy,setBusy]=useState(false),[drag,setDrag]=useState(false),[message,setMessage]=useState(''),[error,setError]=useState('');
 const input=useRef<HTMLInputElement>(null);
 const q=useQuery({queryKey:[base,offset],queryFn:()=>request(base+'?limit=100&offset='+offset),retry:false});
 const data=q.data?.data||{},items:Obj[]=data.items||[];
 async function upload(files:File[]){
  if(busy||!files.length)return;
  setBusy(true);setError('');let saved=0;const failures:string[]=[];
  for(let i=0;i<files.length;i++){
   const file=files[i];setMessage(`正在上传并命名 ${i+1}/${files.length}：${file.name}`);
   try{
    const result=await request(base+'/upload?filename='+encodeURIComponent(file.name)+'&auto_name=true','POST',file);
    saved++;
    if(result.naming_state==='ai_name_failed')failures.push(file.name+'：已保存，AI命名未完成，可手动改名');
   }catch(e){failures.push(file.name+'：'+String(e))}
  }
  setMessage(`已添加 ${saved}/${files.length} 张表情`);setError(failures.join('\n'));setBusy(false);setOffset(0);void q.refetch();
 }
 async function rename(item:Obj){
  const name=window.prompt('修改表情名称',item.name);if(name===null||!name.trim()||name===item.name)return;
  setBusy(true);setError('');try{await request(base+'/'+item.asset_id,'PATCH',{name:name.trim()});setMessage('名称已更新，下次选表情时生效');await q.refetch()}catch(e){setError(String(e))}finally{setBusy(false)}
 }
 async function remove(item:Obj){
  if(!window.confirm(`从当前会话表情库删除“${item.name}”？`))return;
  setBusy(true);setError('');try{await request(base+'/'+item.asset_id,'DELETE');setMessage('已从当前会话表情库删除');if(items.length===1&&offset>0)setOffset(Math.max(0,offset-100));else await q.refetch()}catch(e){setError(String(e))}finally{setBusy(false)}
 }
 return <><div className={'sticker-drop'+(drag?' dragging':'')} onDragOver={e=>{e.preventDefault();if(!busy)setDrag(true)}} onDragLeave={()=>setDrag(false)} onDrop={e=>{e.preventDefault();setDrag(false);void upload(Array.from(e.dataTransfer.files))}}>
  <strong>把喜欢的表情拖到这里 ✦</strong><p>仅添加到当前会话 · 自动看图命名 · 可一次选择多张</p>
  <button disabled={busy} onClick={()=>input.current?.click()}>{busy?'正在整理表情…':'选择图片'}</button>
  <input ref={input} type="file" accept="image/*" multiple hidden onChange={e=>{void upload(Array.from(e.target.files||[]));e.target.value=''}}/>
  <small>每张图片调用一次 AI 命名，关闭思考；失败不自动重试，上传成功的图片可手动改名。</small>
 </div>{message&&<p role="status">{message}</p>}{(error||q.error)&&<p className="error" role="alert" style={{whiteSpace:'pre-wrap'}}>{error||String(q.error)}</p>}
 {q.isPending?<p>正在打开表情库…</p>:<><p className="muted">当前会话共 {data.total||0} 张表情</p><div className="resource-grid">{items.map(item=><article className="card sticker-card" key={item.asset_id}>
  <img className="resource-thumb" loading="lazy" src={'/manage/v1/scopes/'+encodeURIComponent(scope)+'/resources/'+encodeURIComponent(item.source_resource_id)+'/image'} alt={item.name}/>
  <strong>{item.name}</strong><div className="sticker-actions"><button className="secondary" disabled={busy} onClick={()=>void rename(item)}>改名</button><button className="secondary" disabled={busy} onClick={()=>void remove(item)}>删除</button></div>
 </article>)}</div>{!items.length&&<p>还没有表情，拖入第一张吧。</p>}<div className="tabs"><button disabled={busy||offset===0} onClick={()=>setOffset(Math.max(0,offset-100))}>上一页</button><button disabled={busy||!data.has_more} onClick={()=>setOffset(offset+100)}>下一页</button></div></>}
 </>;
}
