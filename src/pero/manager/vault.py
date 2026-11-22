"""Windows user-bound DPAPI credential vault. Metadata APIs never return old keys."""
from contextlib import closing
import ctypes
from ctypes import wintypes
from pathlib import Path
import re
import sqlite3
import sys
import uuid


class VaultError(Exception):
    pass


class DPAPI:
    @staticmethod
    def transform(value, decrypt=False):
        if sys.platform!='win32':raise VaultError('credential_crypto_unavailable')
        class Blob(ctypes.Structure):
            _fields_=[('size',wintypes.DWORD),('data',ctypes.POINTER(ctypes.c_byte))]
        buffer=ctypes.create_string_buffer(value)
        source=Blob(len(value),ctypes.cast(buffer,ctypes.POINTER(ctypes.c_byte)))
        result=Blob()
        function=ctypes.windll.crypt32.CryptUnprotectData if decrypt else ctypes.windll.crypt32.CryptProtectData
        function.argtypes=[ctypes.POINTER(Blob),ctypes.c_void_p,ctypes.c_void_p,ctypes.c_void_p,ctypes.c_void_p,wintypes.DWORD,ctypes.POINTER(Blob)]
        function.restype=wintypes.BOOL
        if not function(ctypes.byref(source),None,None,None,None,1,ctypes.byref(result)):
            raise VaultError('credential_crypto_failed')
        try:return ctypes.string_at(result.data,result.size)
        finally:
            ctypes.windll.kernel32.LocalFree.argtypes=[ctypes.c_void_p]
            ctypes.windll.kernel32.LocalFree(ctypes.cast(result.data,ctypes.c_void_p))


class CredentialVault:
    def __init__(self,directory):self.path=Path(directory)/'credentials.sqlite3'
    def _open(self):
        self.path.parent.mkdir(parents=True,exist_ok=True)
        db=sqlite3.connect(str(self.path),timeout=5)
        db.row_factory=sqlite3.Row
        db.execute('CREATE TABLE IF NOT EXISTS credential(ref TEXT PRIMARY KEY,label TEXT NOT NULL,revision INTEGER NOT NULL,cipher BLOB NOT NULL)')
        return db
    @staticmethod
    def validate_ref(ref):
        if not re.fullmatch(r'cred-[0-9a-f]{32}',ref):raise VaultError('credential_not_found')
    def metadata(self):
        with closing(self._open()) as db:
            return [dict(row,configured=True,mask='••••••••') for row in db.execute('SELECT ref,label,revision FROM credential ORDER BY label,ref')]
    def put(self,label,value,ref=None,expected_revision=0):
        if not isinstance(label,str) or not label.strip() or len(label)>80:raise VaultError('credential_invalid')
        if not isinstance(value,str) or not value.strip() or len(value)>4096:raise VaultError('credential_invalid')
        if type(expected_revision) is not int or expected_revision<0:raise VaultError('credential_invalid')
        cipher=DPAPI.transform(value.strip().encode())
        fresh=ref is None
        ref=ref or 'cred-'+uuid.uuid4().hex
        self.validate_ref(ref)
        with closing(self._open()) as db,db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT revision FROM credential WHERE ref=?',(ref,)).fetchone()
            if not fresh and row is None:raise VaultError('credential_not_found')
            revision=row[0] if row else 0
            if revision!=expected_revision:raise VaultError('credential_revision_conflict')
            db.execute('INSERT INTO credential VALUES(?,?,?,?) ON CONFLICT(ref) DO UPDATE SET label=excluded.label,revision=excluded.revision,cipher=excluded.cipher',
                       (ref,label.strip(),revision+1,cipher))
        return {'ref':ref,'label':label.strip(),'revision':revision+1,'configured':True,'mask':'••••••••'}
    def resolve(self,ref):
        self.validate_ref(ref)
        with closing(self._open()) as db:
            row=db.execute('SELECT cipher FROM credential WHERE ref=?',(ref,)).fetchone()
        if row is None:raise VaultError('credential_not_found')
        return DPAPI.transform(row[0],True).decode()
    def delete(self,ref,expected_revision):
        self.validate_ref(ref)
        with closing(self._open()) as db,db:
            cursor=db.execute('DELETE FROM credential WHERE ref=? AND revision=?',(ref,expected_revision))
            if cursor.rowcount!=1:raise VaultError('credential_revision_conflict')
