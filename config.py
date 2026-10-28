import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "energy-data"

DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_CSV = os.path.join(DATA_DIR, "energy_data.csv")
CHECKPOINT_DIR = os.path.join(DATA_DIR, "spark_checkpoint")
MODEL_DIR = os.path.join(BASE_DIR, "models")
MODEL_PATH = os.path.join(MODEL_DIR, "isolation_forest.joblib")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
os.makedirs(MODEL_DIR, exist_ok=True)

HIGH_POWER_MULTIPLIER = 1.8
ISOLATION_CONTAMINATION = 0.05
ISOLATION_ESTIMATORS = 100
MIN_TRAIN_SAMPLES = 80
RETRAIN_EVERY_BATCHES = 20
TRAIN_BUFFER_SIZE = 4000

FEATURE_COLUMNS = [
    "voltage",
    "current",
    "power",
    "temperature",
    "power_ratio",
]

PROCESSED_COLUMNS = [
    "timestamp",
    "device_id",
    "device",
    "location",
    "voltage",
    "current",
    "power",
    "energy",
    "temperature",
    "base_power",
    "power_ratio",
    "anomaly_score",
    "is_anomaly",
    "status",
    "alert",
]
