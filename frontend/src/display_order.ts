import{tsMs}from './time';

type Row=Record<string,any>;

/** Display-only ordering. Never mutate backend arrays or model history. */
export function newestFirst<T extends Row>(items:T[],fields=['occurred_at','created_at'],sequence=false):T[]{
  return [...items].sort((a,b)=>{
    if(sequence&&Number.isFinite(Number(a.seq))&&Number.isFinite(Number(b.seq))&&a.seq!=null&&b.seq!=null){
      const difference=Number(b.seq)-Number(a.seq);
      if(difference)return difference;
    }
    // 与显示同一套绝对时刻解析：naive 旧值按北京时间，不按浏览器时区。
    const timestamp=(row:T)=>{
      for(const field of fields){const parsed=tsMs(row[field]);if(parsed!=null)return parsed}
      return 0;
    };
    return timestamp(b)-timestamp(a);
  });
}
