"""Manager-owned configuration SQLite; never open the conversational database."""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import uuid
from pero.configuration.models import canonical, ConfigurationPayload


class ConfigurationConflict(Exception):
    pass


class ConfigStore:
    def __init__(self, directory):
        self.path = Path(directory) / 'manager-config.sqlite3'

    def _open(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(str(self.path), timeout=5, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('CREATE TABLE IF NOT EXISTS config_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
        db.execute('CREATE TABLE IF NOT EXISTS config_document(kind TEXT,scope TEXT,item TEXT,revision INTEGER NOT NULL,value TEXT NOT NULL,PRIMARY KEY(kind,scope,item))')
        db.execute('CREATE TABLE IF NOT EXISTS config_request(request_id TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,revision INTEGER NOT NULL,generation INTEGER NOT NULL)')
        db.execute('INSERT OR IGNORE INTO config_meta VALUES(?,?)', ('source_id', uuid.uuid4().hex))
        db.execute('INSERT OR IGNORE INTO config_meta VALUES(?,?)', ('generation', '0'))
        return db

    @staticmethod
    def _document(db, kind, scope, item):
        row = db.execute('SELECT revision,value FROM config_document WHERE kind=? AND scope=? AND item=?', (kind,scope,item)).fetchone()
        return {'revision':row['revision'], 'value':json.loads(row['value'])} if row else {'revision':0,'value':{}}

    def get(self, kind, scope, item):
        with closing(self._open()) as db:
            doc=self._document(db,kind,scope,item)
            if kind=='tool':
                from pero.configuration.prompt_files import override_path
                path=override_path(self.path.parent,scope,item)
                if path.exists():doc['value']['prompt']=path.read_text(encoding='utf-8')
            return doc

    def save(self, kind, scope, item, request, changes):
        fingerprint = hashlib.sha256(canonical([kind,scope,item,request.expected_revision,changes]).encode()).hexdigest()
        with closing(self._open()) as db:
            db.execute('BEGIN IMMEDIATE')
            try:
                prior = db.execute('SELECT * FROM config_request WHERE request_id=?', (request.request_id,)).fetchone()
                if prior:
                    if prior['fingerprint'] != fingerprint:
                        raise ConfigurationConflict('request_id_reused')
                    result = {'request_id':request.request_id,'saved_revision':prior['revision'],
                              'saved_generation':prior['generation'],'idempotent':True}
                else:
                    current = self._document(db,kind,scope,item)
                    if current['revision'] != request.expected_revision:
                        raise ConfigurationConflict('revision_conflict')
                    value = dict(current['value'])
                    for key, change in changes.items():
                        if change is None:value.pop(key,None)
                        else:value[key]=change
                    revision = current['revision'] + 1
                    db.execute('INSERT INTO config_document VALUES(?,?,?,?,?) ON CONFLICT(kind,scope,item) DO UPDATE SET revision=excluded.revision,value=excluded.value',
                               (kind,scope,item,revision,canonical(value)))
                    db.execute("UPDATE config_meta SET value=CAST(value AS INTEGER)+1 WHERE key='generation'")
                    generation = int(db.execute("SELECT value FROM config_meta WHERE key='generation'").fetchone()[0])
                    db.execute('INSERT INTO config_request VALUES(?,?,?,?)', (request.request_id,fingerprint,revision,generation))
                    result = {'request_id':request.request_id,'saved_revision':revision,'saved_generation':generation,'idempotent':False}
                if kind=='tool' and 'prompt' in changes:
                    from pero.configuration.prompt_files import override_path
                    path=override_path(self.path.parent,scope,item)
                    if changes['prompt'] is None:
                        if path.exists():path.unlink()
                    else:
                        path.parent.mkdir(parents=True,exist_ok=True);tmp=path.with_suffix('.pending');tmp.write_text(changes['prompt'],encoding='utf-8');tmp.replace(path)
                db.execute('COMMIT')
                return result
            except BaseException:
                db.execute('ROLLBACK')
                raise

    def snapshot(self):
        with closing(self._open()) as db:
            db.execute('BEGIN')
            meta = dict(db.execute('SELECT key,value FROM config_meta').fetchall())
            personas, tools, models, policy, wake = {}, {}, {}, {}, {}
            for row in db.execute('SELECT kind,scope,item,value FROM config_document'):
                value = json.loads(row['value'])
                if row['kind']=='persona':personas[row['scope']]=value
                elif row['kind']=='tool':
                    from pero.configuration.prompt_files import override_path
                    path=override_path(self.path.parent,row['scope'],row['item'])
                    if path.exists():value['prompt']=path.read_text(encoding='utf-8')
                    tools.setdefault(row['scope'],{})[row['item']]=value
                elif row['kind']=='model':models[row['item']]=value
                elif row['kind']=='policy':policy.update(value)
                elif row['kind']=='wake':wake[row['scope']]=value
            db.execute('COMMIT')
        return ConfigurationPayload(source_id=meta['source_id'],generation=int(meta['generation']),personas=personas,tools=tools,models=models,policy=policy,wake=wake)

    def mark_applied(self, payload):
        with closing(self._open()) as db:
            db.execute("INSERT INTO config_meta VALUES('last_applied',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (canonical(payload.model_dump()),))

    def last_applied(self):
        with closing(self._open()) as db:
            row = db.execute("SELECT value FROM config_meta WHERE key='last_applied'").fetchone()
            return ConfigurationPayload.model_validate(json.loads(row[0])) if row else None
