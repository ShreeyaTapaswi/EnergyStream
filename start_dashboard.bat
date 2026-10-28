@echo off
cd /d "%~dp0"

echo ======================================================================
echo EnergyStream - Starting Streamlit Web Dashboard
echo ======================================================================

if exist "venv\Scripts\activate.bat" (
    call "venv\Scripts\activate.bat"
)

streamlit run dashboard.py
pause
