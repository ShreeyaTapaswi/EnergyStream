# 🚀 EnergyStream — Setup & Execution Instructions

> **Note for AI Agents & Developers**: This document contains deterministic, step-by-step instructions to set up, configure, and execute the **EnergyStream** real-time IoT energy analytics platform from a clean clone.

---

## 📌 Quick Summary for AI Agents

| Parameter | Value |
| :--- | :--- |
| **Python Version** | Python 3.10 – 3.12 (or 3.14 with pre-built wheels) |
| **Java Runtime** | Java JDK 17, 21, or 24 (`JAVA_HOME` auto-discovered) |
| **Hadoop Winutils** | Bundled in repository (`hadoop/bin/winutils.exe`) — no setup needed on Windows |
| **Kafka Broker** | `localhost:9092` (Topic: `energy-data`) |
| **Streamlit Dashboard** | `http://localhost:8501` |
| **Offline / Test Mode** | Supported without Kafka: `python spark_pipeline.py --batch --input data/spark_sample.csv` |

---

## 🛠️ Step 1: System Prerequisites

Before running the python scripts, ensure the following are installed on the host machine:

### 1. Python (3.10+)
Check your installation:
```bash
python --version
```

### 2. Java JDK (17+)
Apache Spark and Apache Kafka require a Java Virtual Machine (JVM).
Check if Java is installed:
```bash
java -version
```
- **Windows**: If Java is missing, install via `winget install Oracle.JDK.21` or download from [Oracle](https://www.oracle.com/java/technologies/downloads/) / [Eclipse Temurin](https://adoptium.net/).
- **Ubuntu/Debian**: `sudo apt update && sudo apt install -y openjdk-17-jdk`
- **macOS**: `brew install openjdk@17`

### 3. Windows Hadoop Binaries (Winutils)
PySpark requires `winutils.exe` and `hadoop.dll` when running on Windows.
> **Good news**: These binaries are **already bundled inside `hadoop/bin/` in this repository!** The `spark_pipeline.py` script automatically configures `HADOOP_HOME` and system `PATH` on launch, so no manual environment configuration is required.

---

## 📦 Step 2: Virtual Environment & Dependencies

From the repository root directory, create and activate a dedicated virtual environment:

### Windows (PowerShell)
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### Windows (Command Prompt `cmd.exe`)
```cmd
python -m venv venv
venv\Scripts\activate.bat
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### Linux / macOS (Bash / Zsh)
```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### Verification
Verify that required libraries are properly imported:
```bash
python -c "import kafka, pyspark, streamlit, sklearn, plotly, pandas; print('✅ All dependencies verified successfully!')"
```

---

## ⚡ Step 3: Start Apache Kafka (3 Options)

Kafka must be listening at `localhost:9092`. Choose **one** of the three methods below:

### Option A: Docker (Fastest & Recommended for Agents)
If Docker / Docker Desktop is installed, start Kafka in one command using the included `docker-compose.yml`:
```bash
docker compose up -d
```
*To verify it is listening*:
```bash
# Check container status
docker ps
```

---

### Option B: Local Apache Kafka Binary (KRaft mode)

If using a native Kafka installation:

#### 1. Download & Extract (if not already downloaded)
- Download Apache Kafka from [kafka.apache.org](https://kafka.apache.org/downloads) (e.g. `kafka_2.13-3.7.2.tgz` or newer).
- Extract it into a folder named `kafka` inside the project root (or `D:\kafka`).

#### 2. Format KRaft Storage Directory (First time only)
Generate a cluster ID and format storage:
- **Windows**:
  ```cmd
  kafka\bin\windows\kafka-storage.bat random-uuid > cluster_id.txt
  set /p CLUSTER_ID=<cluster_id.txt
  kafka\bin\windows\kafka-storage.bat format -t %CLUSTER_ID% -c kafka\config\server.properties
  ```
- **Linux / macOS**:
  ```bash
  KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
  bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/server.properties
  ```

#### 3. Start Kafka Broker
- **Windows**: Run `start_kafka.bat` or:
  ```cmd
  kafka\bin\windows\kafka-server-start.bat kafka\config\server.properties
  ```
- **Linux / macOS**:
  ```bash
  kafka/bin/kafka-server-start.sh kafka/config/server.properties
  ```

---

### Option C: Offline / Batch Mode (Zero Kafka Required!)
If you do not have Kafka or want to immediately test the Spark ML pipeline and Streamlit dashboard:
```bash
# 1. Run Spark ML anomaly detection pipeline in batch mode
python spark_pipeline.py --batch --input data/spark_sample.csv --output data/energy_data.csv

# 2. Launch dashboard
streamlit run dashboard.py
```
This processes sample IoT data through the complete PySpark feature engineering and scikit-learn Isolation Forest model, producing `data/energy_data.csv` for the dashboard.

---

## 🎬 Step 4: Run the Real-Time Stack

To run the live streaming system, run the components in **4 separate terminals** (or use the one-click scripts).

### Quick 1-Click Launch (Windows)
Double-click or run:
```cmd
start_all.bat
```
*(This automatically spawns Kafka, Consumer, Producer, and Dashboard in separate console windows with appropriate startup delays.)*

---

### Manual Multi-Terminal Execution (Step-by-Step)

Always follow this startup order:

#### Terminal 1 — Kafka Broker
Ensure Kafka is running on `localhost:9092`:
```cmd
# Windows
start_kafka.bat

# Or Docker
docker compose up -d
```

#### Terminal 2 — Spark Streaming Consumer Pipeline
```bash
# Activate venv first
python consumer.py
```
*Expected output*:
```text
Cleaned and watermarked streaming DataFrame created.
Starting structured streaming query writing to data/energy_data.csv...
Batch 0 processed: ... rows
```

#### Terminal 3 — IoT Energy Simulator & Producer
```bash
# Activate venv first
python producer.py
```
*Expected output*:
```text
============================================================
ENERGY STREAM - KAFKA PRODUCER
============================================================
[SENT] DEV_001 | Air Conditioner | Power: 1421.32 W
[SENT] DEV_002 | Refrigerator | Power: 248.11 W
...
```

#### Terminal 4 — Streamlit Interactive Dashboard
```bash
# Activate venv first
streamlit run dashboard.py
```
*Expected output*:
```text
Local URL: http://localhost:8501
Network URL: http://...:8501
```

Open **`http://localhost:8501`** in your browser to view live real-time metrics, anomaly alert badges, power trends, and device telemetry.

---

## 🔍 Step 5: Verification & Health Checks for Agents

An AI agent validating the setup can execute these health checks:

### 1. Check Port Bindings
```powershell
# Windows PowerShell
Get-NetTCPConnection -LocalPort 9092, 8501 -ErrorAction SilentlyContinue | Select-Object LocalAddress, LocalPort, State
```
- Port `9092` should show state `Listen` (Kafka).
- Port `8501` should show state `Listen` (Streamlit).

### 2. Check Data Ingestion in CSV
```powershell
# Windows PowerShell
(Get-Content data/energy_data.csv -Tail 5)
```
Ensure rows are being appended every few seconds with columns:
`timestamp, device_id, device, location, voltage, current, power, energy, temperature, base_power, power_ratio, anomaly_score, is_anomaly, status, alert`

### 3. Check Streamlit HTTP Status
```powershell
(Invoke-WebRequest -Uri "http://localhost:8501" -UseBasicParsing).StatusCode
# Expected: 200
```

---

## 🛑 Step 6: Stopping Services

- In any running terminal, press `Ctrl + C` to terminate that process.
- If running Kafka via Docker: `docker compose down`.
- To reset live generated data:
  ```bash
  # Delete generated CSV and Spark checkpoints
  python -c "import os; [os.remove(f) for f in ['data/energy_data.csv'] if os.path.exists(f)]"
  ```
  Restarting `consumer.py` and `producer.py` will regenerate fresh data automatically.

---

## 🩺 Troubleshooting Guide

| Issue | Cause | Fix |
| :--- | :--- | :--- |
| `NoBrokersAvailable: NoBrokersAvailable` | Kafka is not running on port 9092 | Start Kafka via `docker compose up -d` or `start_kafka.bat` before launching consumer/producer. |
| `JAVA_HOME is not set` | JVM not detected by Spark | Set `JAVA_HOME` pointing to JDK directory (e.g. `set JAVA_HOME=C:\Program Files\Java\jdk-21`). |
| `java.io.IOException: Could not locate executable ... winutils.exe` | Windows Hadoop binary missing | Verify `hadoop/bin/winutils.exe` exists in repo. `spark_pipeline.py` configures this automatically. |
| `Waiting for Spark pipeline output...` in Dashboard | `data/energy_data.csv` is not yet created | Start `consumer.py` and `producer.py`, or run `python spark_pipeline.py --batch --input data/spark_sample.csv` to seed data immediately. |
| Port `8501` already in use | Another Streamlit instance is running | Run on a different port: `streamlit run dashboard.py --server.port 8502`. |
