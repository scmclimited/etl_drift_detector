# ETL Module for Schema Drift Detection and Kafka Integration

This module is part of the **FedRAMP-SIM** project, designed to simulate log ingestion pipelines with schema drift detection using Apache Spark. It includes logic for both real and simulated Kafka producers, drift correction, and reprocessing to Parquet.

## Features

- Detects schema drift between expected and actual Spark DataFrames.
- Applies auto-correction for known drift cases (e.g., renamed columns).
- Supports reprocessing of logs from JSON or JSONL to Parquet.
- Compliant with strict type-checking modes (Pyright-compatible).
- Supports both simulated and real Kafka producers.

## Directory Structure

ETL/

├── schema.py              # Expected Spark schema definition using StructType

├── drift_detector.py      # Logic for schema drift detection

├── reprocess.py           # Main ETL script to handle drift and convert to Parquet

## Schema Drift Detection

The `drift_detector.py` file compares the schema of a DataFrame loaded from raw logs against the expected schema.

Usage:
    python reprocess.py

This command will:
- Load JSON logs from `random_logs/drift.jsonl`
- Detect schema drift vs. `expected_log_schema` in `schema.py`
- Fix common drift patterns (like "time" → "timestamp")
- Write the cleaned data to `random_logs/curated_logs.parquet`

## Expected Schema Format

Defined in `schema.py`:

```python
expected_log_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("event", StringType(), True),
    StructField("user", StringType(), True),
    StructField("source", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("message", StringType(), True),
    StructField("exec_time_ms", IntegerType(), True),
])
