
import re
from pyspark.sql import functions as F


def column_map(item):
    cols = item["columns"]
    if isinstance(cols, dict):
        return {str(name).strip(): dtype for name, dtype in cols.items()}
    return {str(c["name"]).strip(): c["type"] for c in cols}


def source_and_table_from_path(path, storage_root, config):
    """Source is first folder; table is matched from the configured table folder."""
    root = storage_root.rstrip("/") + "/"
    if not path.startswith(root):
        raise ValueError(f"File is outside landing root: {path}")
    parts = [p for p in path[len(root):].split("/") if p]
    if len(parts) < 3 or not parts[-1].lower().endswith(".parquet"):
        raise ValueError(f"Unexpected file path: {path}")
    source = parts[0].lower()
    folders = [p for p in parts[1:-1] if not re.match(r"^(year|month|day)=", p, re.I)]
    normalize = lambda x: re.sub(r"[^a-z0-9]", "", x.lower())
    matches = [item for item in config["tables"]
               if source in [s.lower() for s in item["source_names"]]
               and normalize(item["table_name"]) in [normalize(f) for f in folders]]
    if len(matches) != 1:
        raise ValueError(f"Cannot uniquely map source/table from path: {path}")
    return source, matches[0]


def select_configured_columns(df, item):
    """Bronze: configured columns and casts only; no PK/DQ checks."""
    expected = column_map(item)
    actual = {c.lower(): c for c in df.columns}
    missing = [c for c in expected if c.lower() not in actual]
    if missing:
        raise ValueError(f"Configured columns missing in {item['table_name']}: {missing}")
    return df.select(*[
        F.col("`" + actual[name.lower()].replace("`", "``") + "`").cast(dtype).alias(name)
        for name, dtype in expected.items()
    ])


def add_lineage(df, path, source):
    return (df.withColumn("_source_file", F.lit(path))
              .withColumn("_source_system", F.lit(source))
              .withColumn("_ingested_at", F.current_timestamp()))


def move_file(dbutils, path, storage_root, destination_root):
    relative = path.replace(storage_root.rstrip("/") + "/", "", 1)
    destination = f"{destination_root.rstrip('/')}/{relative}"
    dbutils.fs.mkdirs(destination.rsplit("/", 1)[0])
    dbutils.fs.mv(path, destination)
    return destination


def log_event(spark, table, row):
    """Append one structured run/error event to a Delta log table."""
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
    schema = StructType([
        StructField("event_time", TimestampType(), False),
        StructField("pipeline", StringType(), False),
        StructField("table_name", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("file_path", StringType(), True),
        StructField("status", StringType(), False),
        StructField("rows_read", LongType(), True),
        StructField("rows_written", LongType(), True),
        StructField("rows_rejected", LongType(), True),
        StructField("message", StringType(), True),
    ])
    data = [(row.get("event_time"), row.get("pipeline", "unknown"), row.get("table_name"),
             row.get("source_system"), row.get("file_path"), row.get("status", "UNKNOWN"),
             row.get("rows_read"), row.get("rows_written"), row.get("rows_rejected"),
             (row.get("message") or "")[:4000])]
    spark.createDataFrame(data, schema).write.format("delta").mode("append").saveAsTable(table)


def standardize_column_names(df):
    names = [c.strip() for c in df.columns]
    if len(names) != len(set(names)):
        raise ValueError("Trimming column names creates duplicate column names")
    df = df.toDF(*names)
    return df


def trim_string_columns(df):
    for name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(name, F.trim(F.col(name)))
    return df


def remove_exact_duplicates(df, item):
    return df.dropDuplicates(list(column_map(item)))


def run_silver_quality_checks(df, item):
    df = standardize_column_names(df)
    df = trim_string_columns(df)
    expected = list(column_map(item))
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"Silver schema check failed; missing columns: {missing}")
    df = remove_exact_duplicates(df, item)
    condition = None
    for key in item["primary_key"]:
        part = F.col(key).isNull()
        condition = part if condition is None else condition | part
    if condition is None:
        return df, df.limit(0)
    return df.filter(~condition), df.filter(condition)



def log_rejected_rows(spark, table, df, table_name, reason):
    """Save rejected Silver rows as JSON so the bad row can be investigated."""
    from pyspark.sql import functions as F
    rejected = (df.withColumn("table_name", F.lit(table_name))
                  .withColumn("rejected_at", F.current_timestamp())
                  .withColumn("reject_reason", F.lit(reason))
                  .withColumn("row_data", F.to_json(F.struct(*[F.col(c) for c in df.columns])))
                  .select("rejected_at", "table_name", "reject_reason", "row_data"))
    if rejected.limit(1).count():
        (rejected.write.format("delta").mode("append").saveAsTable(table))
