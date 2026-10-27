# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///

import json
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys

COMMON_PATH = "/Workspace/Users/dg2305960@gmail.com/azure-data-engineering-adventureworks/Databricks"

if COMMON_PATH not in sys.path:
    sys.path.insert(0, COMMON_PATH)

from common_functions import column_map, run_silver_quality_checks, log_event, log_rejected_rows

CONFIG_PATH = "/Workspace/Users/dg2305960@gmail.com/azure-data-engineering-adventureworks/Databricks/table_config.json"
BRONZE_SCHEMA, SILVER_SCHEMA = "practice.bronze", "practice.silver"
CHECKPOINT_TABLE = f"{SILVER_SCHEMA}.silver_watermark"
PROCESS_LOG, ERROR_LOG = f"{SILVER_SCHEMA}.silver_process_log", f"{SILVER_SCHEMA}.silver_error_log"
REJECTED_ROWS = f"{SILVER_SCHEMA}.silver_rejected_rows"
ACTIVE, EFF_FROM, EFF_TO = "is_active", "effective_from", "effective_to"
LINEAGE = ["_source_system", "_source_file", "_ingested_at"]

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = json.load(f)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
 table_name STRING, last_watermark TIMESTAMP, updated_at TIMESTAMP
) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {REJECTED_ROWS} (
      rejected_at TIMESTAMP, table_name STRING, reject_reason STRING, row_data STRING
    ) USING DELTA""")
for table in [PROCESS_LOG, ERROR_LOG]:
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} (
      event_time TIMESTAMP, pipeline STRING, table_name STRING, source_system STRING,
      file_path STRING, status STRING, rows_read BIGINT, rows_written BIGINT,
      rows_rejected BIGINT, message STRING
    ) USING DELTA""")


def get_watermark(name):
    safe = name.replace("'", "''")
    rows = spark.sql(f"SELECT last_watermark FROM {CHECKPOINT_TABLE} WHERE table_name='{safe}' LIMIT 1").collect()
    return rows[0][0] if rows and rows[0][0] is not None else None


def save_watermark(name, watermark):
    safe = name.replace("'", "''")
    value = watermark.strftime("%Y-%m-%d %H:%M:%S.%f")
    spark.sql(f"""MERGE INTO {CHECKPOINT_TABLE} t
      USING (SELECT '{safe}' table_name, TIMESTAMP '{value}' last_watermark) s
      ON t.table_name=s.table_name
      WHEN MATCHED THEN UPDATE SET t.last_watermark=s.last_watermark,t.updated_at=current_timestamp()
      WHEN NOT MATCHED THEN INSERT (table_name,last_watermark,updated_at)
      VALUES (s.table_name,s.last_watermark,current_timestamp())""")


def merge_scd2(source, silver, item, business, lineage):
    keys = item["primary_key"]
    on = " AND ".join([f"t.`{k}` = s.`{k}`" for k in keys] + [f"t.{ACTIVE}=true"])
    changed_cols = [c for c in business if c not in keys]
    changed = " OR ".join([f"NOT (t.`{c}` <=> s.`{c}`)" for c in changed_cols]) or "false"

 
    DeltaTable.forName(spark, silver).alias("t").merge(source.alias("s"), on) \
        .whenMatchedUpdate(condition=changed, set={ACTIVE: "false", EFF_TO: "current_timestamp()"}) \
        .execute()

   
    values = {c: f"s.`{c}`" for c in business + lineage if c in source.columns}
    values.update({ACTIVE: "true", EFF_FROM: "current_timestamp()", EFF_TO: "cast(null as timestamp)"})
    DeltaTable.forName(spark, silver).alias("t").merge(source.alias("s"), on) \
        .whenNotMatchedInsert(values=values).execute()


def process_table(item):
    started = datetime.utcnow()
    name = item["table_name"].lower()
    bronze, silver = f"{BRONZE_SCHEMA}.{name}", f"{SILVER_SCHEMA}.{name}"
    if not spark.catalog.tableExists(bronze):
        return

    business = list(column_map(item))
    bronze_df = spark.table(bronze)
    if "_ingested_at" not in bronze_df.columns:
        raise ValueError("Bronze _ingested_at column is required for watermark incremental load")
    missing = [c for c in business if c not in bronze_df.columns]
    if missing:
        raise ValueError(f"Bronze missing configured columns: {missing}")

    max_wm = bronze_df.agg(F.max("_ingested_at")).first()[0]
    if max_wm is None:
        return
    wm = get_watermark(name)

   
    if wm is None:
        batch = bronze_df
    else:
        batch = bronze_df.filter(F.col("_ingested_at") >= F.lit(wm))

    selected = batch.select(*business, *[c for c in LINEAGE if c in bronze_df.columns])
    clean, rejected = run_silver_quality_checks(selected, item)
    rejected_count = rejected.count()
    if rejected_count:
        log_rejected_rows(spark, REJECTED_ROWS, rejected, name, "NULL primary key")
    clean = clean.dropDuplicates(business)

    keys = item["primary_key"]
    order = []
    if "ModifiedDate" in clean.columns:
        order.append(F.col("ModifiedDate").desc_nulls_last())
    order.append(F.col("_ingested_at").desc_nulls_last())
    clean = clean.withColumn("_rn", F.row_number().over(
        Window.partitionBy(*keys).orderBy(*order)
    )).filter("_rn = 1").drop("_rn")

    if not spark.catalog.tableExists(silver):
        (clean.limit(0).withColumn(ACTIVE, F.lit(True))
            .withColumn(EFF_FROM, F.lit(None).cast("timestamp"))
            .withColumn(EFF_TO, F.lit(None).cast("timestamp"))
            .write.format("delta").mode("overwrite").saveAsTable(silver))

    rows_written = clean.count()
    if rows_written:
        merge_scd2(clean, silver, item, business, [c for c in LINEAGE if c in clean.columns])
    save_watermark(name, max_wm)

    row = {"event_time": started, "pipeline": "silver", "table_name": name,
        "status": "SUCCESS" if rejected_count == 0 else "SUCCESS_WITH_REJECTS",
        "rows_read": batch.count(), "rows_written": rows_written,
        "rows_rejected": rejected_count, "message": f"Watermark {wm} -> {max_wm}"}
    log_event(spark, PROCESS_LOG, row)
    if rejected_count:
        log_event(spark, ERROR_LOG, {**row, "status": "ROWS_REJECTED",
            "message": f"Rejected row details saved in {REJECTED_ROWS}; reason: NULL primary key"})


for item in config["tables"]:
    try:
        process_table(item)
    except Exception as exc:
        row = {"event_time": datetime.utcnow(), "pipeline": "silver",
            "table_name": item.get("table_name"), "status": "FAILED", "message": str(exc)}
        log_event(spark, PROCESS_LOG, row)
        log_event(spark, ERROR_LOG, row)
        print(f"FAILED {item.get('table_name')}: {str(exc)[:1000]}")

print("Silver processing finished.")