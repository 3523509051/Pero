"""Explicit UTF-8 tool prompt files, separate from Python source code."""
import hashlib
from pathlib import Path
from pero.core.settings import repo_root
ANALYSIS_TOOLS=frozenset({'web_search','read_website','read_file'})
def default_prompt(name,fallback):
    path=repo_root()/'config'/'tool-prompts'/(name+'.md')
    if path.exists():return path.read_text(encoding='utf-8')
    path.parent.mkdir(parents=True,exist_ok=True);path.write_text(fallback,encoding='utf-8');return fallback
def override_path(directory,scope,name):
    return Path(directory)/'tool-prompts'/hashlib.sha256(scope.encode()).hexdigest()/(name+'.md')
