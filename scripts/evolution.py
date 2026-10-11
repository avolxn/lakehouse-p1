import argparse
import json
from pathlib import Path
from trino_api import query

TABLE = "iceberg.silver.snapshot_demo"

def demonstrate_evolution(execute=query):
    def count(suffix=""):
        return query(f"SELECT count(*) FROM {TABLE}{suffix}")["rows"][0][0]

    execute(f"CREATE TABLE {TABLE} WITH (format='PARQUET', compression_codec='ZSTD') AS SELECT * FROM iceberg.silver.order_lines")
    before = count()
    sid = query('SELECT snapshot_id FROM iceberg.silver."snapshot_demo$snapshots" ORDER BY committed_at DESC LIMIT 1')["rows"][0][0]
    execute(f"ALTER TABLE {TABLE} ADD COLUMN source_system VARCHAR")
    execute(f"ALTER TABLE {TABLE} RENAME COLUMN source_system TO data_origin")
    nulls = query(f"SELECT count(*) FROM {TABLE} WHERE data_origin IS NULL")["rows"][0][0]
    assert nulls == before
    execute(f"DELETE FROM {TABLE} WHERE orderkey=(SELECT min(orderkey) FROM {TABLE})")
    after = count()
    historical = count(f" FOR VERSION AS OF {sid}")
    assert after < before and historical == before
    execute(f"ALTER TABLE {TABLE} EXECUTE rollback_to_snapshot({sid})")
    restored = count()
    assert restored == before
    return {"table": TABLE, "snapshot_id": sid, "before": before, "after_delete": after,
            "historical": historical, "restored": restored, "new_column_nulls": nulls,
            "bytes": query('SELECT sum(file_size_in_bytes) FROM iceberg.silver."snapshot_demo$files"')["rows"][0][0],
            "schema": query(f"DESCRIBE {TABLE}")["rows"]}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    def logged_execute(sql):
        print(sql, flush=True)
        return query(sql)
    result = demonstrate_evolution(logged_execute)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(result, ensure_ascii=False))
