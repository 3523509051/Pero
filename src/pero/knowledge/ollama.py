"""Local Ollama embeddings; retrieval itself stays in the scoped SQLite index."""
import json,math
from types import SimpleNamespace
from urllib.parse import urlsplit
import httpx

def is_local_ollama(url):
 p=urlsplit(url)
 return p.scheme=='http' and p.hostname in {'localhost','127.0.0.1','::1'} and p.port==11434 and not (p.username or p.password or p.query or p.fragment) and p.path in {'','/'}

class OllamaEmbedding:
 configured=True
 def __init__(self,url,model,dimension):
  if not is_local_ollama(url):raise ValueError('invalid_local_ollama_url')
  self.url=url.rstrip('/');self._settings=SimpleNamespace(model=model,dimension=int(dimension));self.provider_model=f'ollama|{self.url}|{model}|{dimension}'
 async def embed(self,texts):
  async with httpx.AsyncClient(trust_env=False,follow_redirects=False,timeout=60) as client:
   async with client.stream('POST',self.url+'/api/embed',json={'model':self._settings.model,'input':list(texts),'truncate':False,'dimensions':self._settings.dimension}) as r:
    r.raise_for_status();data=bytearray()
    async for chunk in r.aiter_bytes():
     data.extend(chunk)
     if len(data)>8*1024*1024:raise ValueError('embedding_response_limit')
  vectors=json.loads(data)['embeddings']
  if not isinstance(vectors,list) or len(vectors)!=len(texts):raise ValueError('invalid_embedding_count')
  for v in vectors:
   if not isinstance(v,list) or len(v)!=self._settings.dimension or any(type(x) not in (int,float) or not math.isfinite(x) for x in v) or not any(v):raise ValueError('invalid_embedding_vector')
  return vectors
