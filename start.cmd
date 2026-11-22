@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  echo Python environment missing. Follow README to create .venv and install dependencies.
  pause
  exit /b 1
)
".venv\Scripts\python.exe" "scripts\launcher.py" start %*
set "PERO_EXIT_CODE=%ERRORLEVEL%"
if not "%PERO_EXIT_CODE%"=="0" pause
exit /b %PERO_EXIT_CODE%
