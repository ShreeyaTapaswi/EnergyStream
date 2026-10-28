"""Kafka analytics consumer — processing now runs in Apache Spark."""

from spark_pipeline import run_streaming_pipeline


if __name__ == "__main__":
    run_streaming_pipeline()
