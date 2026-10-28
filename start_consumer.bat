@echo off
cd /d "%~dp0"

echo ======================================================================
echo EnergyStream - Starting Spark Streaming Pipeline (Consumer)
echo ======================================================================

if exist "venv\Scripts\activate.bat" (
    call "venv\Scripts\activate.bat"
)

python consumer.py
pause
