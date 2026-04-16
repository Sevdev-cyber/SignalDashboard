@echo off
TITLE Zatrzymywanie Lookacza
color 0C

echo ========================================================
echo         ZATRZYMYWANIE PROCESOW LOOKACZA...
echo ========================================================
echo.

:: Zabija procesy, których tytuły okien zaczynają się od "Lookacz"
taskkill /FI "WINDOWTITLE eq Lookacz Oczy*" /T /F
taskkill /FI "WINDOWTITLE eq Lookacz Mozg*" /T /F

echo.
echo ========================================================
echo Wszystkie okna Lookacza zostaly wyczyszczone.
echo ========================================================
pause
