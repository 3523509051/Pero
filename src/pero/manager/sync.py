"""Idempotent configuration synchronization; no model retry or tool execution."""
import asyncio
import os
from pero.configuration.auth import TOKEN_ENV
from .upstream import CoreUnavailable


class ConfigurationSync:
    def __init__(self, store, reader):
        self.store, self.reader = store, reader
        self.lock = asyncio.Lock()
        self.last_error = None

    async def state(self):
        desired = await asyncio.to_thread(self.store.snapshot)
        try:
            actual = await self.reader.management('GET', '/config-state')
        except CoreUnavailable as exc:
            return {'state':'pending_apply','generation':desired.generation,
                    'applied_generation':None,'reason':exc.code,'effective_from':'next_turn'}
        matched = (actual.get('source_id')==desired.source_id and actual.get('generation')==desired.generation
                   and actual.get('digest')==desired.digest())
        return {'state':'applied' if matched else 'pending_apply','generation':desired.generation,
                'applied_generation':actual.get('generation'),'instance_id':actual.get('instance_id'),
                'last_apply_error':None if matched else self.last_error,'effective_from':'next_turn'}

    async def apply(self):
        async with self.lock:
            desired = await asyncio.to_thread(self.store.snapshot)
            try:
                actual = await self.reader.management('PUT','/config-snapshot',desired.model_dump())
                if not (actual.get('state')=='applied' and actual.get('source_id')==desired.source_id
                        and actual.get('generation')==desired.generation and actual.get('digest')==desired.digest()):
                    raise CoreUnavailable('invalid_apply_receipt')
                self.last_error = None
                await asyncio.to_thread(self.store.mark_applied,desired)
            except CoreUnavailable as exc:
                self.last_error = exc.code
                # If a restarted core rejects an offline edit, restore the last
                # acknowledged snapshot rather than silently using empty defaults.
                try:
                    actual = await self.reader.management('GET','/config-state')
                    last = await asyncio.to_thread(self.store.last_applied)
                    if actual.get('source_id') is None and last is not None:
                        await self.reader.management('PUT','/config-snapshot',last.model_dump())
                except CoreUnavailable:
                    pass
        return await self.state()

    async def run(self):
        while True:
            if os.environ.get(TOKEN_ENV,'').strip():
                try:
                    state = await self.state()
                    if state['state']!='applied':await self.apply()
                except Exception:
                    # No payload/credentials in logs; explicit API reports unavailable store.
                    self.last_error = 'configuration_sync_unavailable'
            await asyncio.sleep(3)
