from pyspark.sql import SparkSession
from ETL.schema import expected_log_schema
from ETL.drift_detector import detect_schema_drift

def reprocess_logs(input_path: str, output_path: str):
    spark = SparkSession.builder.appName("SchemaDriftReprocessor").getOrCreate()
    
    # Load JSON logs
    df = spark.read.json(input_path)

    # Detect schema drift
    drift = detect_schema_drift(df, expected_log_schema)
    if drift:
        print("Schema Drift Detected:")
        for field, issue in drift:
            print(f"  - {field}: {issue}")
    else:
        print("No schema drift detected.")

    # Apply a fix for renamed field 'time' -> 'timestamp'
    if 'time' in df.columns and 'timestamp' not in df.columns:
        df = df.withColumnRenamed("time", "timestamp")

    # Write to Parquet
    df.write.mode("overwrite").parquet(output_path)
    print(f"Logs reprocessed and written to {output_path}")

if __name__ == "__main__":
    reprocess_logs("random_logs/drift.jsonl", "random_logs/curated_logs.parquet")
