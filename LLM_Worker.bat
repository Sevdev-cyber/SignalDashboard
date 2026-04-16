@echo off
TITLE Wizjoner LLM Worker
cd /d C:\SignalDashboard

echo Starting Wizjoner LLM Worker...
echo Project: C:\SignalDashboard
echo Mode   : windowed
echo.
echo This will open the LLM worker in a visible PowerShell window.
echo Use STOP_LLM_Worker.bat to stop it.
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File C:\SignalDashboard\start_llm_worker_windowed.ps1

echo.
echo Command finished. Press any key to close.
pause >nul
