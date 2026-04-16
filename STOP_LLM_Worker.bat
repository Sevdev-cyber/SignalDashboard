@echo off
TITLE Stop Wizjoner LLM Worker
cd /d C:\SignalDashboard

echo Stopping Wizjoner LLM Worker...
echo Project: C:\SignalDashboard
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File C:\SignalDashboard\stop_llm_worker_headless.ps1

echo.
echo Command finished. Press any key to close.
pause >nul
