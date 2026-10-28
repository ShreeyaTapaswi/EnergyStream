@echo off
cd /d "%~dp0"

echo ======================================================================
echo EnergyStream - Starting IoT Simulator & Kafka Producer
echo ======================================================================

if exist "venv\Scripts\activate.bat" (
    call "venv\Scripts\activate.bat"
)

python producer.py
pause
