@echo off
TITLE Lookacz Master Starter
color 0A

echo ========================================================
echo        URUCHAMIANIE SYSTEMU "LOOKACZ" (HSB v11.5)
echo ========================================================
echo.

:: Pobranie sciezki do folderu, w ktorym znajduje sie ten plik
set "PROJECT_ROOT=%~dp0"
cd /d "%PROJECT_ROOT%"

echo [1/2] Uruchamianie Oczu (Signal Server)...
start "Lookacz Oczy (Signal Engine)" cmd /k "set DASHBOARD_BAR_TF_MIN=1&& set SIGNAL_ENGINE_MODE=final_mtf_v3&& python signal_server.py --port 5557 --ws-port 8080"

:: Czekamy 3 sekundy, zeby serwer na spokojnie wstal
timeout /t 3 /nobreak >nul

echo [2/2] Uruchamianie Mozgu Makro (LLM Worker)...
start "Lookacz Mozg (LLM Worker)" cmd /k "python llm_context_worker.py --daily-llm --intraday-llm --archive"

echo.
echo ========================================================
echo  GOTOWE! Procesy rezyduja w dwoch wlasnych oknach.
echo  Powyzszy skaner czuwa nad Szokiem Zmiennosci 60 pkt.
echo ========================================================
pause
