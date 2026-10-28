"""
Apache Spark pipeline for Energy Stream.

Stages:
  1. Ingest Kafka JSON (streaming) or CSV (batch)
  2. Clean and type-cast readings
  3. Enrich with device base power
  4. Engineer features (Spark SQL / MLlib VectorAssembler)
  5. Score anomalies with Isolation Forest
  6. Apply rule-based HIGH consumption labels
  7. Write processed rows for the dashboard
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import deque

import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, hour, to_timestamp, when
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)
from sklearn.ensemble import IsolationForest

from config import (
    BASE_DIR,
    CHECKPOINT_DIR,
    FEATURE_COLUMNS,
    HIGH_POWER_MULTIPLIER,
    ISOLATION_CONTAMINATION,
    ISOLATION_ESTIMATORS,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    MIN_TRAIN_SAMPLES,
    MODEL_PATH,
    PROCESSED_COLUMNS,
    PROCESSED_CSV,
    RETRAIN_EVERY_BATCHES,
    TRAIN_BUFFER_SIZE,
)
from energy_simulator import DEVICES


READING_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("device", StringType(), True),
        StructField("location", StringType(), True),
        StructField("voltage", DoubleType(), True),
        StructField("current", DoubleType(), True),
        StructField("power", DoubleType(), True),
        StructField("energy", DoubleType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("alert", StringType(), True),
    ]
)


_train_buffer: deque[list[float]] = deque(maxlen=TRAIN_BUFFER_SIZE)
_isolation_model: IsolationForest | None = None
_batch_count = 0


def _ensure_java_home() -> None:
    current = os.environ.get("JAVA_HOME", "")
    if current and os.path.exists(os.path.join(current, "bin", "java.exe")):
        return

    import subprocess

    try:
        result = subprocess.run(
            ["java", "-XshowSettings:properties", "-version"],
            capture_output=True,
            text=True,
            check=False,
        )
        blob = f"{result.stdout}\n{result.stderr}"
        for line in blob.splitlines():
            if "java.home" in line and "=" in line:
                os.environ["JAVA_HOME"] = line.split("=", 1)[1].strip()
                return
    except OSError:
        pass


def _ensure_hadoop_home() -> None:
    hadoop_home = os.path.join(BASE_DIR, "hadoop")
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["hadoop.home.dir"] = hadoop_home
    hadoop_bin = os.path.join(hadoop_home, "bin")
    os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")


def build_spark_session(include_kafka: bool = True) -> SparkSession:
    _ensure_java_home()
    _ensure_hadoop_home()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    import pyspark

    spark_version = pyspark.__version__
    scala = "2.13" if int(spark_version.split(".")[0]) >= 4 else "2.12"
    kafka_package = (
        f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{spark_version}"
    )

    builder = (
        SparkSession.builder.appName("EnergyStreamPipeline")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if include_kafka:
        builder = builder.config("spark.jars.packages", kafka_package)
    return builder.getOrCreate()


def device_catalog_df(spark: SparkSession) -> DataFrame:
    rows = [
        (d["device_id"], float(d["base_power"]))
        for d in DEVICES
    ]
    return spark.createDataFrame(rows, ["device_id", "base_power"])


def clean_and_enrich(
    readings: DataFrame,
    catalog: DataFrame,
    streaming: bool = False,
) -> DataFrame:
    numeric_cols = ["voltage", "current", "power", "energy", "temperature"]

    cleaned = readings
    for name in numeric_cols:
        cleaned = cleaned.withColumn(name, col(name).cast(DoubleType()))

    cleaned = (
        cleaned.withColumn("event_time", to_timestamp(col("timestamp")))
        .filter(col("event_time").isNotNull())
        .filter(col("device_id").isNotNull())
        .filter(col("power").isNotNull() & (col("power") >= 0))
        .filter(
            col("voltage").isNull()
            | ((col("voltage") >= 180) & (col("voltage") <= 260))
        )
    )

    if streaming:
        cleaned = cleaned.withWatermark("event_time", "2 minutes")

    cleaned = cleaned.dropDuplicates(["timestamp", "device_id"])

    enriched = (
        cleaned.join(catalog, on="device_id", how="left")
        .na.fill({"base_power": 100.0})
        .withColumn(
            "power_ratio",
            when(
                col("base_power") > 0,
                col("power") / col("base_power"),
            ).otherwise(0.0),
        )
        .withColumn("hour_of_day", hour(col("event_time")))
        .withColumn(
            "rule_high",
            col("power") > (col("base_power") * HIGH_POWER_MULTIPLIER),
        )
    )
    return enriched


def assemble_features(frame: DataFrame) -> DataFrame:
    imputed_cols = [f"{name}_imputed" for name in FEATURE_COLUMNS]
    imputer = Imputer(
        inputCols=FEATURE_COLUMNS,
        outputCols=imputed_cols,
        strategy="mean",
    )
    assembler = VectorAssembler(
        inputCols=imputed_cols,
        outputCol="features",
        handleInvalid="keep",
    )
    pipeline = Pipeline(stages=[imputer, assembler])
    return pipeline.fit(frame).transform(frame)


def _fit_isolation_forest(samples: list[list[float]]) -> IsolationForest:
    model = IsolationForest(
        n_estimators=ISOLATION_ESTIMATORS,
        contamination=ISOLATION_CONTAMINATION,
        random_state=42,
        n_jobs=1,
    )
    model.fit(samples)
    try:
        import joblib

        joblib.dump(model, MODEL_PATH)
    except Exception:
        pass
    return model


def _score_with_isolation_forest(pdf: pd.DataFrame) -> pd.DataFrame:
    global _isolation_model, _batch_count

    features = (
        pdf[FEATURE_COLUMNS]
        .apply(pd.to_numeric, errors="coerce")
        .fillna(0.0)
        .to_numpy()
    )
    _train_buffer.extend(features.tolist())
    _batch_count += 1

    if len(_train_buffer) >= MIN_TRAIN_SAMPLES:
        should_train = (
            _isolation_model is None
            or _batch_count % RETRAIN_EVERY_BATCHES == 0
        )
        if should_train:
            _isolation_model = _fit_isolation_forest(list(_train_buffer))

        pdf["anomaly_score"] = _isolation_model.decision_function(features)
        pdf["is_anomaly"] = _isolation_model.predict(features) == -1
    else:
        pdf["anomaly_score"] = 0.0
        pdf["is_anomaly"] = False

    return pdf


def _label_alerts(pdf: pd.DataFrame) -> pd.DataFrame:
    rule_high = pdf["rule_high"].astype(bool)
    is_anomaly = pdf["is_anomaly"].astype(bool)

    status = []
    alerts = []
    for high, anomaly, device, power in zip(
        rule_high,
        is_anomaly,
        pdf["device"],
        pdf["power"],
    ):
        if high:
            status.append("HIGH")
            alerts.append(
                f"High energy consumption detected: {device} ({power:.2f} W)"
            )
        elif anomaly:
            status.append("ANOMALY")
            alerts.append(
                f"Isolation Forest anomaly: {device} ({power:.2f} W)"
            )
        else:
            status.append("NORMAL")
            alerts.append("")

    pdf["status"] = status
    pdf["alert"] = alerts
    pdf["is_anomaly"] = is_anomaly | rule_high
    return pdf


def write_processed(pdf: pd.DataFrame) -> None:
    if pdf.empty:
        return

    output = pdf.copy()
    output["timestamp"] = output["timestamp"].astype(str)
    output = output.reindex(columns=PROCESSED_COLUMNS)

    header = not os.path.exists(PROCESSED_CSV) or os.path.getsize(PROCESSED_CSV) == 0
    output.to_csv(PROCESSED_CSV, mode="a", header=header, index=False)


def process_microbatch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return

    featured = assemble_features(batch_df)
    pdf = featured.toPandas()
    if pdf.empty:
        return

    pdf = _score_with_isolation_forest(pdf)
    pdf = _label_alerts(pdf)
    write_processed(pdf)

    anomalies = int(pdf["is_anomaly"].sum())
    print(
        f"[SPARK batch {batch_id}] "
        f"rows={len(pdf)} anomalies={anomalies}"
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return raw.select(
        from_json(col("value").cast("string"), READING_SCHEMA).alias("reading")
    ).select("reading.*")


def read_batch_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    )


def run_streaming_pipeline() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    catalog = device_catalog_df(spark)

    print("=" * 70)
    print("ENERGY STREAM - APACHE SPARK PIPELINE")
    print("=" * 70)
    print("Source: Kafka", KAFKA_TOPIC)
    print("Sink:", PROCESSED_CSV)
    print("Model: Isolation Forest")
    print("=" * 70)

    readings = read_kafka_stream(spark)
    featured = clean_and_enrich(readings, catalog, streaming=True)

    query = (
        featured.writeStream.foreachBatch(process_microbatch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


def run_batch_pipeline(
    input_csv: str = PROCESSED_CSV,
    output_csv: str | None = None,
) -> None:
    spark = build_spark_session(include_kafka=False)
    spark.sparkContext.setLogLevel("WARN")
    catalog = device_catalog_df(spark)

    if not os.path.exists(input_csv):
        raise FileNotFoundError(f"No input data at {input_csv}")

    print("=" * 70)
    print("ENERGY STREAM - SPARK BATCH PIPELINE")
    print("=" * 70)
    print("Input:", input_csv)
    print("Model: Isolation Forest")
    print("=" * 70)

    readings = read_batch_csv(spark, input_csv)
    featured = clean_and_enrich(readings, catalog)
    scored_spark = assemble_features(featured)
    pdf = scored_spark.toPandas()
    pdf = _score_with_isolation_forest(pdf)
    pdf = _label_alerts(pdf)

    dest = output_csv or PROCESSED_CSV
    staging = dest + ".spark.tmp"
    pdf.reindex(columns=PROCESSED_COLUMNS).to_csv(staging, index=False)
    os.replace(staging, dest)

    print(
        f"Wrote {len(pdf)} rows to {dest} | "
        f"HIGH={(pdf['status'] == 'HIGH').sum()} | "
        f"ANOMALY={(pdf['status'] == 'ANOMALY').sum()}"
    )
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Energy Stream Spark pipeline")
    parser.add_argument(
        "--batch",
        action="store_true",
        help="Process existing CSV instead of Kafka streaming",
    )
    parser.add_argument(
        "--input",
        default=PROCESSED_CSV,
        help="CSV path for batch mode",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output CSV path for batch mode",
    )
    args = parser.parse_args()

    if args.batch:
        run_batch_pipeline(args.input, args.output)
    else:
        run_streaming_pipeline()


if __name__ == "__main__":
    main()
