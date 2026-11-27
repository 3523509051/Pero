from fastapi import APIRouter,Depends,HTTPException,Request
from pero.configuration.auth import require_management
from .services import ServiceError


async def observed_services(operations):
    """Runtime observations are independent of permission to control processes."""
    try:observed=await operations.reader.status()
    except Exception:observed={}
    result=[]
    for name in ('pero','bridge'):
        if name in operations.bindings:
            try:state=await operations.observe(name)
            except Exception:state={'running':None,'ready':False,'state':'unknown'}
        elif name=='pero':
            core=observed.get('pero',{})
            healthy=core.get('state')=='http_ready'
            state={'running':True if healthy else None,'ready':bool(core.get('ready')),
                   'state':'http_ready' if healthy else 'unknown','status_source':'core_health'}
        else:
            connected=observed.get('bridge',{}).get('connected')
            state={'running':True if connected is True else None,'ready':connected is True,
                   'connected':connected,'state':'connected' if connected is True else 'not_connected' if connected is False else 'unknown',
                   'status_source':'onebot_connection'}
        result.append({'service_id':name,'configured':name in operations.bindings,**state})
    return result


def service_router(operations):
    router=APIRouter(prefix='/manage/v1',dependencies=[Depends(require_management)])
    @router.get('/services')
    async def services():
        return {'items':await observed_services(operations)}
    @router.post('/operations',status_code=202)
    async def submit(request:Request):
        try:
            body=await request.json()
            if not isinstance(body,dict) or set(body)-{'target','action','force','request_id'}:raise ValueError()
            return operations.submit(body.get('target'),body.get('action'),body.get('force',False),body.get('request_id'))
        except (ValueError,TypeError):raise HTTPException(422,detail={'code':'invalid_operation'}) from None
        except ServiceError as exc:raise HTTPException(409,detail={'code':str(exc)}) from None
    @router.get('/operations')
    async def listing():return {'items':list(operations.items.values())[::-1]}
    @router.get('/operations/{operation_id}')
    async def detail(operation_id:str):
        item=operations.items.get(operation_id)
        if item is None:raise HTTPException(404,detail={'code':'operation_not_found'})
        return item
    return router
