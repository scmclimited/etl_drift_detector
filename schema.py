from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

expected_log_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("event", StringType(), True),
    StructField("user", StringType(), True),
    StructField("source", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("message", StringType(), True),
    StructField("exec_time_ms", IntegerType(), True),
])
