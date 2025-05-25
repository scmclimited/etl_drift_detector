from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType, DataType

from typing import List, Tuple, Dict

def detect_schema_drift(df: "DataFrame", expected_schema: "StructType") -> List[Tuple[str, str]]:
    """
    Compare actual schema with expected and return drift details.
    Returns a list of (field_name, issue_description) tuples.
    """
    drift: List[Tuple[str, str]] = []

    expected_fields: Dict[str, "DataType"] = {
        f.name: f.dataType for f in expected_schema.fields
    }

    actual_fields: Dict[str, "DataType"] = {
        f.name: f.dataType for f in df.schema.fields
    }

    for name, dtype in expected_fields.items():
        if name not in actual_fields:
            drift.append((name, "Missing field"))
        elif actual_fields[name] != dtype:
            drift.append((name, f"Type mismatch: expected {dtype}, found {actual_fields[name]}"))

    for name in actual_fields:
        if name not in expected_fields:
            drift.append((name, "Unexpected field"))

    return drift
