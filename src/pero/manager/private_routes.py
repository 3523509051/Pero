"""Authenticated, fixed-path proxy; Core alone owns private access storage."""
import asyncio
import os
import re

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pero.configuration.auth import TOKEN_ENV, require_management

router = APIRouter(prefix='/manage/v1/private', tags=['private-chat'],
                   dependencies=[Depends(require_management)])
_OPERATIONS = {
    'GET': r'(tokens|grants|grants/[A-Za-z0-9_-]+|owner|owner/group-access|policy)',
    'POST': r'(tokens|tokens/[A-Za-z0-9_-]+/revoke|owner/transfer)',
    'PATCH': r'(grants/[A-Za-z0-9_-]+|owner/group-access|policy)',
}


@router.api_route('/{operation:path}', methods=['GET', 'POST', 'PATCH'])
async def private_proxy(operation: str, request: Request):
    if not re.fullmatch(_OPERATIONS[request.method], operation):
        raise HTTPException(404, detail={'code': 'private_operation_not_found'})
    payload = None
    if request.method != 'GET':
        body = await request.body()
        if len(body) > 32768:
            raise HTTPException(413, detail={'code': 'private_request_too_large'})
        if body:
            try:
                payload = await request.json()
            except ValueError:
                raise HTTPException(422, detail={'code': 'invalid_json'}) from None
            if not isinstance(payload, dict):
                raise HTTPException(422, detail={'code': 'invalid_request'})
    reader = request.app.state.core_reader
    try:
        response = await reader.client.request(
            request.method, '/internal/v1/private/manage/' + operation,
            params=request.query_params.multi_items(), json=payload,
            headers={'Authorization': 'Bearer ' + os.environ.get(TOKEN_ENV, '').strip()})
    except httpx.TimeoutException:
        raise HTTPException(503, detail={'code': 'core_timeout'}) from None
    except httpx.HTTPError:
        raise HTTPException(503, detail={'code': 'core_unreachable'}) from None
    try:
        result = response.json()
        if not isinstance(result, dict):
            raise ValueError()
    except ValueError:
        raise HTTPException(503, detail={'code': 'core_invalid_response'}) from None
    if request.method == 'GET' and operation == 'owner/group-access' and response.status_code == 200:
        scopes = await asyncio.to_thread(request.app.state.scope_directory.load)
        labels = {scope.conversation_id: scope for scope in scopes if scope.conversation_type == 'group'}
        for group in result.get('groups', []):
            scope = labels.get(group.get('conversation_id'))
            if scope is not None:
                group.update(display_name=scope.display_name,
                             display_name_source=scope.display_name_source,
                             agent_display_name=scope.agent_display_name)
    return JSONResponse(result, status_code=response.status_code,
                        headers={'Cache-Control': 'no-store'})
