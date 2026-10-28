#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR"

echo "======================================================================"
echo "EnergyStream - Starting Full Real-Time Analytics Stack (Linux / macOS)"
echo "======================================================================"

if [ -d "venv" ]; then
    source venv/bin/activate
fi

# 1. Start Kafka (check docker or local kafka)
if command -v docker &> /dev/null && [ -f "docker-compose.yml" ]; then
    echo "[1/4] Starting Kafka via Docker Compose..."
    docker compose up -d
elif [ -d "kafka/bin" ]; then
    echo "[1/4] Starting local Kafka broker..."
    kafka/bin/kafka-server-start.sh kafka/config/server.properties &
else
    echo "[WARNING] No Kafka binary or Docker found. Ensure Kafka is running on localhost:9092."
fi

sleep 4

echo "[2/4] Starting Spark Streaming Consumer..."
python consumer.py &
CONSUMER_PID=$!
sleep 2

echo "[3/4] Starting IoT Energy Producer..."
python producer.py &
PRODUCER_PID=$!
sleep 2

echo "[4/4] Starting Streamlit Dashboard..."
streamlit run dashboard.py &
DASHBOARD_PID=$!

echo "======================================================================"
echo "EnergyStream is running! Dashboard: http://localhost:8501"
echo "Press Ctrl+C to stop all background processes."
echo "======================================================================"

trap "kill $CONSUMER_PID $PRODUCER_PID $DASHBOARD_PID 2>/dev/null || true; exit 0" SIGINT SIGTERM
wait
