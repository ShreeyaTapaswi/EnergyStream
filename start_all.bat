@echo off
cd /d "%~dp0"

echo ======================================================================
echo EnergyStream - Starting Full Real-Time Analytics Stack
echo (Kafka Broker + Spark Consumer + IoT Producer + Streamlit Dashboard)
echo ======================================================================

echo.
echo [1/4] Starting Kafka Broker...
start "EnergyStream - [1] Kafka Broker" cmd /k "call start_kafka.bat"
echo Waiting 6 seconds for Kafka broker initialization...
timeout /t 6 /nobreak >nul

echo.
echo [2/4] Starting Spark Streaming Pipeline (Consumer)...
start "EnergyStream - [2] Spark Consumer" cmd /k "call start_consumer.bat"
echo Waiting 3 seconds for Spark session init...
timeout /t 3 /nobreak >nul

echo.
echo [3/4] Starting IoT Energy Producer...
start "EnergyStream - [3] IoT Producer" cmd /k "call start_producer.bat"
echo Waiting 2 seconds...
timeout /t 2 /nobreak >nul

echo.
echo [4/4] Starting Streamlit Dashboard...
start "EnergyStream - [4] Streamlit Dashboard" cmd /k "call start_dashboard.bat"

echo.
echo ======================================================================
echo All EnergyStream services have been launched!
echo Streamlit Dashboard URL : http://localhost:8501
echo Kafka Broker Endpoint   : localhost:9092
echo Topic                   : energy-data
echo ======================================================================
pause
