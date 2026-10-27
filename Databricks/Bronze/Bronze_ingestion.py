# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///

import json
import time
import sys

COMMON_PATH = "/Workspace/Users/dg2305960@gmail.com/azure-data-engineering-adventureworks/Databricks"
if COMMON_PATH not in sys.path:
    sys.path.insert(0, COMMON_PATH)
from common_functions import( source_and_table_from_path, select_configured_columns, add_lineage, move_file, log_event)
from datetime import datetime

STORAGE_ROOT = "abfss://landing@divya00.dfs.core.windows.net"
BRONZE_SCHEMA = "practice.bronze"
CONFIG_PATH = "/Workspace/Users/dg2305960@gmail.com/azure-data-engineering-adventureworks/Databricks/table_config.json"
ARCHIVE_ROOT, QUARANTINE_ROOT = f"{STORAGE_ROOT}/archive", f"{STORAGE_ROOT}/quarantine"
PROCESS_LOG, ERROR_LOG = f"{BRONZE_SCHEMA}.bronze_process_log", f"{BRONZE_SCHEMA}.bronze_error_log"
MAX_RETRIES = 3

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = json.load(f)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
for table in [PROCESS_LOG, ERROR_LOG]:
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} (
      event_time TIMESTAMP, pipeline STRING, table_name STRING, source_system STRING,
      file_path STRING, status STRING, rows_read BIGINT, rows_written BIGINT,
      rows_rejected BIGINT, message STRING
    ) USING DELTA""")


def discover_files():
    found = []
    sources = sorted({s.lower() for t in config["tables"] for s in t["source_names"]})
    stack = [f"{STORAGE_ROOT}/{s}/" for s in sources]
    while stack:
        folder = stack.pop()
        try:
            for item in dbutils.fs.ls(folder):
                path = item.path.rstrip("/")
                if "/archive/" in path.lower() or "/quarantine/" in path.lower():
                    continue
                if item.isDir():
                    stack.append(item.path)
                elif path.lower().endswith(".parquet"):
                    found.append(path)
        except Exception as exc:
            log_event(spark, ERROR_LOG, {"event_time": datetime.utcnow(), "pipeline": "bronze",
                "status": "SCAN_ERROR", "file_path": folder, "message": str(exc)})
    return sorted(set(found))


for path in discover_files():
    started, source, table_name = datetime.utcnow(), None, None
    rows_read = rows_written = 0
    try:
        source, item = source_and_table_from_path(path, STORAGE_ROOT, config)
        table_name = item["table_name"].lower()
        target = f"{BRONZE_SCHEMA}.{table_name}"
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                raw = spark.read.parquet(path)
                rows_read = raw.count()
                selected = select_configured_columns(raw, item)
                rows_written = selected.count()
                if rows_written:
                    add_lineage(selected, path, source).write.format("delta").mode("append").saveAsTable(target)
                break
            except Exception:
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(attempt * 2)
        destination = move_file(dbutils, path, STORAGE_ROOT, ARCHIVE_ROOT)
        log_event(spark, PROCESS_LOG, {"event_time": started, "pipeline": "bronze",
            "table_name": table_name, "source_system": source, "file_path": path,
            "status": "SUCCESS", "rows_read": rows_read, "rows_written": rows_written,
            "rows_rejected": 0, "message": f"Archived to {destination}"})
        print(f"SUCCESS {table_name}: read={rows_read}, written={rows_written}")
    except Exception as exc:
        message = str(exc)
        try:
            destination = move_file(dbutils, path, STORAGE_ROOT, QUARANTINE_ROOT)
            message += f" | quarantined_to={destination}"
        except Exception as move_exc:
            message += f" | quarantine_move_failed={move_exc}"
        row = {"event_time": started, "pipeline": "bronze", "table_name": table_name,
            "source_system": source, "file_path": path, "status": "FAILED",
            "rows_read": rows_read, "rows_written": rows_written, "rows_rejected": 0,
            "message": message}
        log_event(spark, PROCESS_LOG, row)
        log_event(spark, ERROR_LOG, row)
        print(f"FAILED {path}: {message[:1000]}")

print("Bronze ingestion finished.")