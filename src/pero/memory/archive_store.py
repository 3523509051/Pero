"""Transactional, bounded archive snapshots. Called only via repository threads."""
from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone

from pero.memory.models import StoredTurn, HistoryLoad


def _now():
    return datetime.now(timezone.utc).isoformat()


def _digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


class ArchiveStoreMixin:
    def archive_conversations(self, *, agent_id, limit=32, after_id=''):
        """Bounded cursor page; callers cycle after the last id, reset on empty."""
        with self._txn() as cur:
            return [r[0] for r in cur.execute(
                "SELECT DISTINCT conversation_id FROM l2_turn WHERE agent_id=? AND status<>'pending' "
                "AND conversation_id>? ORDER BY conversation_id LIMIT ?",
                (agent_id, after_id, min(max(int(limit), 1), 256))).fetchall()]

    def _archive_snapshot(self, cur, agent_id, turn_ids):
        """Sources and controlled evidence are immutable input, not temporary model notes."""
        snapshots = []
        for tid in turn_ids:
            row = cur.execute(
                "SELECT * FROM l2_turn WHERE agent_id=? AND turn_id=?", (agent_id, tid)).fetchone()
            if row is None or row['status'] == 'pending':
                return None
            sources = [dict(r) for r in cur.execute(
                "SELECT m.record_id,m.sender_identity_id,m.received_at FROM l2_turn_source s "
                "JOIN raw_message m ON m.record_id=s.record_id WHERE s.turn_id=? ORDER BY s.position",
                (tid,)).fetchall()]
            evidence = self._evidence_for_turns(cur, [tid], 4).get(tid, ())
            stored = StoredTurn(turn_id=tid, status=row['status'], user_text=row['user_text'],
                assistant_text=row['assistant_text'] if row['status'] == 'sent' else None,
                created_at=row['created_at'], tool_evidence=tuple(evidence),
                interrupted_reason=row['interrupt_reason'])
            snapshots.append({'turn_id': tid, 'status': row['status'], 'created_at': row['created_at'],
                'updated_at': row['updated_at'], 'user': stored.render_user_content(),
                'assistant': stored.assistant_text, 'sources': sources})
        return snapshots

    def prepare_archive(self, *, agent_id, conversation_id, trigger_turns, trigger_chars,
                        retain_turns, max_input_chars, max_turns, max_pending):
        with self._txn() as cur:
            pending = cur.execute("SELECT COUNT(*) FROM archive_task WHERE state IN ('queued','running')").fetchone()[0]
            if pending >= max_pending:
                return None
            totals = cur.execute(
                "SELECT COUNT(*),COALESCE(SUM(length(t.user_text)+length(COALESCE(t.assistant_text,''))),0) "
                "FROM l2_turn t WHERE t.agent_id=? AND t.conversation_id=? AND t.status<>'pending' "
                "AND NOT EXISTS(SELECT 1 FROM archive_task_source s WHERE s.agent_id=t.agent_id AND s.turn_id=t.turn_id)",
                (agent_id, conversation_id)).fetchone()
            if totals[0] < trigger_turns and (trigger_chars <= 0 or totals[1] < trigger_chars):
                return None
            recent = cur.execute(
                "SELECT created_at,turn_id FROM l2_turn WHERE agent_id=? AND conversation_id=? "
                "ORDER BY created_at DESC,turn_id DESC LIMIT 1 OFFSET ?",
                (agent_id, conversation_id, max(int(retain_turns)-1, 0))).fetchone()
            if recent is None:
                return None
            rows = cur.execute(
                "SELECT turn_id,status FROM l2_turn t WHERE agent_id=? AND conversation_id=? "
                "AND (created_at,turn_id)<(?,?) AND NOT EXISTS(SELECT 1 FROM archive_task_source s "
                "WHERE s.agent_id=t.agent_id AND s.turn_id=t.turn_id) "
                "ORDER BY created_at,turn_id LIMIT ?",
                (agent_id, conversation_id, recent['created_at'], recent['turn_id'], min(max_turns, 128))).fetchall()
            ids, selected, input_text = [], [], ''
            oversized = False
            for row in rows:
                if row['status'] == 'pending':
                    break
                candidate = self._archive_snapshot(cur, agent_id, [row['turn_id']])[0]
                trial = selected + [candidate]
                rendered = json.dumps(trial, ensure_ascii=False, separators=(',', ':'))
                if len(rendered) > max_input_chars:
                    if not selected:
                        # Persist a bounded failure marker instead of retrying the same oversized row forever.
                        ids = [row['turn_id']]
                        selected = [candidate]
                        oversized = True
                    break
                selected, input_text = trial, rendered
                ids.append(row['turn_id'])
            if not ids:
                return None
            task_id = 'ar-' + uuid.uuid4().hex
            fingerprint = _digest(selected)
            now = _now()
            state = 'failed' if oversized else 'queued'
            cur.execute(
                "INSERT INTO archive_task(task_id,agent_id,conversation_id,state,error_code,fingerprint,"
                "input_text,source_turn_ids,created_at,updated_at,from_time,to_time,version) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,1)",
                (task_id,agent_id,conversation_id,state,'input_too_large' if oversized else '',fingerprint,
                 '' if oversized else input_text,json.dumps(ids),now,now,selected[0]['created_at'],selected[-1]['created_at']))
            for position, tid in enumerate(ids):
                cur.execute("INSERT INTO archive_task_source(agent_id,turn_id,task_id,position) VALUES(?,?,?,?)",
                            (agent_id,tid,task_id,position))
            return {'task_id':task_id,'agent_id':agent_id,'conversation_id':conversation_id,
                    'state':state,'input_text':input_text if not oversized else '',
                    'fingerprint':fingerprint,'source_turn_ids':ids}

    def claim_archive(self, *, task_id, agent_id):
        with self._txn() as cur:
            cur.execute("UPDATE archive_task SET state='running',updated_at=? WHERE task_id=? AND agent_id=? AND state='queued'",
                        (_now(),task_id,agent_id))
            return cur.rowcount == 1

    def settle_archive(self, *, task_id, agent_id, state, error_code='', summary=None,
                       model='', purpose='conversation_summary', summary_limit=2000):
        if state not in {'completed','blocked','failed','interrupted'}:
            raise ValueError('invalid_archive_state')
        with self._txn() as cur:
            row = cur.execute("SELECT * FROM archive_task WHERE task_id=? AND agent_id=?",(task_id,agent_id)).fetchone()
            if row is None or row['state'] not in {'queued','running'}:
                return False
            memory_id = None
            now = _now()
            if state == 'completed':
                if row['state'] != 'running' or not isinstance(summary,str) or not summary.strip() or len(summary)>summary_limit:
                    state,error_code = 'failed','invalid_summary'
                else:
                    ids = json.loads(row['source_turn_ids'])
                    snapshot = self._archive_snapshot(cur,agent_id,ids)
                    if snapshot is None or _digest(snapshot) != row['fingerprint']:
                        state,error_code = 'failed','snapshot_changed'
                    else:
                        conversation = cur.execute("SELECT * FROM conversation WHERE conversation_id=? AND agent_id=?",
                                                   (row['conversation_id'],agent_id)).fetchone()
                        if conversation is None:
                            state,error_code = 'failed','conversation_missing'
                        else:
                            memory_id = 'l4-' + uuid.uuid4().hex
                            cur.execute(
                                "INSERT INTO l4_memory(memory_id,agent_id,conversation_id,channel_id,account_id,kind,status,"
                                "content,content_chars,subject_identity_id,subject_label,request_text,request_record_id,"
                                "turn_id,dedupe_key,occurred_at,saved_at,updated_at) VALUES(?,?,?,?,?,'archive_summary','saved',?,?,NULL,NULL,'',NULL,?,?,?,?,?)",
                                (memory_id,agent_id,row['conversation_id'],conversation['channel_id'],conversation['account_id'],
                                 summary.strip(),len(summary.strip()),ids[0],'archive:'+task_id,row['from_time'],now,now))
                            position=0
                            for item in snapshot:
                                cur.execute("INSERT INTO l4_memory_source VALUES(?,?,?,?)",(memory_id,'l2_turn',item['turn_id'],position));position+=1
                                for source in item['sources']:
                                    cur.execute("INSERT OR IGNORE INTO l4_memory_source VALUES(?,?,?,?)",(memory_id,'raw_message',source['record_id'],position));position+=1
            cur.execute("UPDATE archive_task SET state=?,error_code=?,memory_id=?,model=?,purpose=?,updated_at=? WHERE task_id=?",
                        (state,str(error_code)[:80],memory_id,model,purpose,now,task_id))
            return state == 'completed' if summary is not None else True

    def recover_archives(self):
        with self._txn() as cur:
            cur.execute("UPDATE archive_task SET state='interrupted',error_code='process_interrupted',updated_at=? WHERE state IN ('queued','running')",(_now(),))
            return cur.rowcount

    def archive_status(self, *, agent_id=None, limit=32):
        where,params = (' WHERE agent_id=?',[agent_id]) if agent_id else ('',[])
        with self._txn() as cur:
            counts={r['state']:r['n'] for r in cur.execute('SELECT state,COUNT(*) n FROM archive_task'+where+' GROUP BY state',params)}
            rows=cur.execute('SELECT task_id,agent_id,conversation_id,state,error_code,memory_id,model,purpose,version,from_time,to_time,created_at,updated_at FROM archive_task'+where+' ORDER BY created_at DESC,task_id DESC LIMIT ?',params+[min(max(int(limit),1),128)]).fetchall()
            return {'counts':counts,'tasks':[dict(r) for r in rows]}

    def archive_history(self, *, agent_id, conversation_id, char_budget, turn_limit, evidence_per_turn):
        """One transaction gives completed summaries plus uncovered recent turns and a revision."""
        with self._txn() as cur:
            archives=cur.execute(
                "SELECT a.task_id,a.source_turn_ids,a.from_time,a.to_time,l.memory_id,l.content "
                "FROM archive_task a JOIN l4_memory l ON l.memory_id=a.memory_id "
                "WHERE a.agent_id=? AND a.conversation_id=? AND a.state='completed' "
                "ORDER BY a.to_time DESC,a.task_id DESC LIMIT 32",(agent_id,conversation_id)).fetchall()
            selected=[];used=0
            # Reserve at least half the context for recent complete turns.
            for row in archives:
                text=f"[系统整理，非用户原话｜{row['memory_id']}｜{row['from_time']}至{row['to_time']}｜来源轮次={','.join(json.loads(row['source_turn_ids']))}]\n{row['content']}"
                if used+len(text)>char_budget//2:
                    break
                selected.append({'memory_id':row['memory_id'],'content':row['content'],'text':text,
                                 'source_turn_ids':json.loads(row['source_turn_ids']),
                                 'from_time':row['from_time'],'to_time':row['to_time']})
                used+=len(text)
            # Only exclude ranges represented in the selected summaries. Other records remain eligible.
            excluded=[tid for item in selected for tid in item['source_turn_ids']]
            sql="SELECT * FROM l2_turn WHERE agent_id=? AND conversation_id=? AND status<>'pending'"
            params=[agent_id,conversation_id]
            if excluded:
                sql+=' AND turn_id NOT IN ('+','.join('?' for _ in excluded)+')';params+=excluded
            rows=cur.execute(sql+' ORDER BY created_at DESC,turn_id DESC LIMIT ?',params+[min(turn_limit,128)]).fetchall()
            evidence=self._evidence_for_turns(cur,[r['turn_id'] for r in rows],evidence_per_turn)
            turns=[]
            for row in rows:
                turn=StoredTurn(turn_id=row['turn_id'],status=row['status'],user_text=row['user_text'],
                    assistant_text=row['assistant_text'] if row['status']=='sent' else None,
                    created_at=row['created_at'],tool_evidence=tuple(evidence.get(row['turn_id'],())),
                    interrupted_reason=row['interrupt_reason'])
                if used+turn.history_cost>char_budget:
                    break
                turns.append(turn);used+=turn.history_cost
            turns.reverse();selected.reverse()
            revision=_digest([(r['task_id'],r['memory_id']) for r in archives])
            reason = 'char_budget' if len(turns) < len(rows) else ('ok' if turns else 'no_turns')
            return {'summaries':selected,'history':HistoryLoad(turns=tuple(turns),reason=reason,
                    budget_chars=max(char_budget-sum(len(s['text']) for s in selected),0),
                    used_chars=sum(t.history_cost for t in turns),dropped_turns=len(rows)-len(turns)),
                    'revision':revision,'used_chars':used}
