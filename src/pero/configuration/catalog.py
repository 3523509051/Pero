"""Real builtin definitions, including definitions of currently unregistered tools."""
from types import SimpleNamespace
from pero.tools.builtin import GetCurrentTimeTool
from pero.tools.memory_tools import RememberTool, RecallTool
from pero.tools.profile import LookProfileTool
from pero.tools.web import WebTool
from pero.tools.files import FileTool
from pero.tools.images import ImageTool
from pero.tools.amap_tools import AmapTool
from pero.tools.stickers import StickerTool
from pero.knowledge.tools import KnowledgeTool
from pero.tools.validation import validate_arguments


def builtin_specs():
    sources = [(GetCurrentTimeTool, ['get_current_time']), (RememberTool, ['remember']),
        (RecallTool, ['recall']), (LookProfileTool, ['look_profile']),
        (WebTool, ['web_search', 'read_website']), (FileTool, ['list_files', 'read_file']),
        (ImageTool, ['list_images', 'look_image']),
        (AmapTool, ['amap_weather', 'amap_places', 'amap_route']),
        (StickerTool, ['list_stickers', 'save_sticker', 'rename_sticker']), (KnowledgeTool,['search_knowledge','read_knowledge'])]
    result = {}
    for cls, names in sources:
        for name in names:
            # Call definition-only methods, never construct/register a fake runnable tool.
            spec = cls.spec(SimpleNamespace(tool_id=name, _SPECS=getattr(cls, '_SPECS', {})))
            result[name] = spec
    return result


CREDENTIAL_TOOLS={'web_search','read_website','read_file','amap_weather','amap_places','amap_route'}

#: 插件工具由安装器在启用/停用时登记真实声明。Manager 进程没有 Core 的
#: ToolRegistry，因此用这份受控镜像让配置详情和校验 Mirrors 实际注册状态。
_PLUGIN_TOOLS: dict[str, dict] = {}


def register_plugin_tool_schemas(plugin_id, tools, config_schema, parameter_schemas):
    for tool_id in tuple(tools):
        _PLUGIN_TOOLS[tool_id] = {
            'plugin_id': plugin_id,
            'config_schema': dict(config_schema or {}),
            'parameter_schema': dict(parameter_schemas.get(tool_id) or {}),
        }


def unregister_plugin_tool_schemas(plugin_id):
    for tool_id in [name for name, value in _PLUGIN_TOOLS.items() if value['plugin_id'] == plugin_id]:
        _PLUGIN_TOOLS.pop(tool_id, None)


def plugin_tool_info(tool_id):
    return _PLUGIN_TOOLS.get(tool_id)


def validate_defaults(tool_id, config, registry=None):
    from .prompt_files import ANALYSIS_TOOLS
    if tool_id not in ANALYSIS_TOOLS and (config.get('analysis_enabled') or config.get('thinking_mode') in {'enabled','disabled'} or config.get('analysis_prompt') or config.get('model_profile')):
        raise ValueError('tool_has_no_analysis_stage')
    if config.get('credential_ref') and tool_id not in CREDENTIAL_TOOLS:raise ValueError('tool_has_no_external_credential')
    if registry is not None and registry.has(tool_id):
        spec = registry.get(tool_id).spec()
    else:
        spec = builtin_specs().get(tool_id)
    if spec is None:
        plugin = _PLUGIN_TOOLS.get(tool_id)
        if plugin is None or not plugin['parameter_schema']:
            raise ValueError('unknown_tool')
        spec = SimpleNamespace(name=tool_id, description='', parameters=plugin['parameter_schema'])
    defaults = config.get('defaults')
    if defaults is not None:
        schema = dict(spec.parameters)
        schema['required'] = []
        if validate_arguments(defaults, schema):
            raise ValueError('invalid_tool_defaults')
    return spec


def backend_enabled(settings, name):
    groups = {'web':{'web_search','read_website'}, 'files':{'list_files','read_file'},
              'images':{'list_images','look_image'}, 'amap':{'amap_weather','amap_places','amap_route'},
              'stickers':{'list_stickers','save_sticker','rename_sticker'}}
    if name=='look_profile':return settings.memory.l3_enabled
    for group, names in groups.items():
        if name in names:return getattr(settings, group).enabled
    return True


def tool_catalog(registry, snapshot, settings):
    specs = builtin_specs()
    specs.update({spec.name: spec for spec in registry.specs()})
    result = []
    for name, spec in sorted(specs.items()):
        from .prompt_files import default_prompt,ANALYSIS_TOOLS
        from pathlib import Path
        from pero.core.settings import repo_root
        prompt=default_prompt(name,spec.description)
        configured = snapshot.tool(name)
        registered = registry.has(name)
        plugin = _PLUGIN_TOOLS.get(name) if name not in builtin_specs() else None
        tool=registry.get(name) if registered else None
        backend=getattr(tool,'backend',None)
        backend_settings=getattr(backend,'settings',None)
        code_settings=backend_settings.model_dump() if hasattr(backend_settings,'model_dump') else {}
        code_settings={k:v for k,v in code_settings.items() if not any(x in k.lower() for x in ('key','token','secret','password','credential'))}
        timeout=min(float(getattr(tool,'timeout_seconds',settings.runtime.tool_timeout_seconds)),240.0)
        if plugin is not None:
            editable=['enabled','prompt','timeout_seconds']
        else:
            editable=['enabled','prompt']+(['defaults'] if spec.parameters.get('properties') else [])+['timeout_seconds']+(['analysis_enabled','analysis_prompt','thinking_mode','model_profile'] if name in ANALYSIS_TOOLS else [])+(['credential_ref'] if name in CREDENTIAL_TOOLS else [])
        labels={'enabled':('启用工具','boolean'),'prompt':('工具调用 Prompt','textarea'),'defaults':('默认参数覆盖（JSON 对象）','json'),'timeout_seconds':('执行器超时（秒）','number'),'analysis_enabled':('独立结果分析','boolean'),'analysis_prompt':('独立分析 Prompt','textarea'),'thinking_mode':('独立分析模型思考','thinking'),'model_profile':('分析模型配置名称','text'),'credential_ref':('凭据引用（Key 在模型与 API Key 页设置）','text')}
        ui_fields=[{'key':key,'label':labels[key][0],'type':labels[key][1]} for key in editable]
        defaults={'enabled':registered and backend_enabled(settings,name),'prompt':prompt,'defaults':{},'timeout_seconds':timeout}
        if name in ANALYSIS_TOOLS:defaults.update(analysis_enabled=False,thinking_mode='inherit')
        backend_ready = True if plugin is not None else backend_enabled(settings,name)
        result.append({'tool_id': name, 'description': spec.description,
            'parameters': dict(spec.parameters), 'registered': registered,
            'enabled': registered and backend_ready and configured.get('enabled') is not False,
            'backend_enabled': backend_ready,
            'model_visible': registered and configured.get('enabled') is not False,
            'configuration_source': 'plugin' if plugin is not None else 'builtin',
            'plugin_id': plugin['plugin_id'] if plugin else None,
            'plugin_config_path': '/manage/v1/plugins/' + plugin['plugin_id'] if plugin else None,
            'plugin_config_schema': plugin['config_schema'] if plugin else None,
            'configuration': configured,
            'editable_fields':editable,'ui_fields':ui_fields,
            'code_configuration':{'defaults':defaults,'backend_settings':code_settings,'parameter_schema':dict(spec.parameters)},
            'capabilities':{'provider':'deepseek' if name in ANALYSIS_TOOLS else 'amap' if name.startswith('amap_') else 'local','independent_analysis':name in ANALYSIS_TOOLS,'native_tool_thinking':False},
            'default_prompt':prompt,'prompt_source':'config/tool-prompts/'+name+'.md',
            'effective_prompt':configured.get('prompt') or prompt,
            'prompt_role': 'model_visible_tool_description', 'reasoning_editable': name in ANALYSIS_TOOLS,
            'analysis_enabled':configured.get('analysis_enabled',False)})
    return result
