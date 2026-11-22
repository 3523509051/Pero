type Row=Record<string,any>;

/** Display-only ordering. Never mutate backend arrays or model history. */
export function newestFirst<T extends Row>(items:T[],fields=['occurred_at','created_at'],sequence=false):T[]{
  return [...items].sort((a,b)=>{
    if(sequence&&Number.isFinite(Number(a.seq))&&Number.isFinite(Number(b.seq))&&a.seq!=null&&b.seq!=null){
      const difference=Number(b.seq)-Number(a.seq);
      if(difference)return difference;
    }
    const timestamp=(row:T)=>{
      for(const field of fields){const parsed=Date.parse(row[field]||'');if(Number.isFinite(parsed))return parsed}
      return 0;
    };
    return timestamp(b)-timestamp(a);
  });
}
