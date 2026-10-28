@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

echo ======================================================================
echo EnergyStream - Starting Apache Kafka Broker
echo ======================================================================

:: Attempt to locate Java if JAVA_HOME is not set
if not defined JAVA_HOME (
    for /d %%D in ("C:\Program Files\Java\jdk*") do (
        set "JAVA_HOME=%%D"
    )
)

if defined JAVA_HOME (
    echo Using JAVA_HOME: %JAVA_HOME%
) else (
    echo [WARNING] JAVA_HOME not detected. Ensure Java 17+ is installed and on PATH.
)

:: Check if Kafka folder exists locally
if exist "kafka\bin\windows\kafka-server-start.bat" (
    echo Found local Kafka in .\kafka
    if exist "kafka\config\server.properties" (
        call "kafka\bin\windows\kafka-server-start.bat" "kafka\config\server.properties"
    ) else if exist "kafka\config\kraft\server.properties" (
        call "kafka\bin\windows\kafka-server-start.bat" "kafka\config\kraft\server.properties"
    )
) else if exist "D:\kafka\bin\windows\kafka-server-start.bat" (
    echo Found Kafka in D:\kafka
    call "D:\kafka\bin\windows\kafka-server-start.bat" "D:\kafka\config\server.properties"
) else (
    echo [ERROR] Local Kafka binary not found in .\kafka or D:\kafka!
    echo.
    echo Options:
    echo 1. Run via Docker Compose: docker compose up -d
    echo 2. Download Apache Kafka 3.x/4.x binary and extract into .\kafka
    echo See SETUP_INSTRUCTIONS.md for full details.
    pause
)
