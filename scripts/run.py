import argparse
import csv
from datetime import datetime, timezone
import json
import platform
import random
import statistics
import subprocess
from pathlib import Path
from trino_api import query
from evolution import demonstrate_evolution

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results" / "local"
PROJECTIONS = {
    "customer": "custkey,mktsegment",
    "orders": "orderkey,custkey,orderdate,shippriority",
    "lineitem": "orderkey,linenumber,extendedprice,discount,shipdate",
}
CONFIGS = [("parquet_none", "PARQUET", "NONE"),
           ("parquet_snappy", "PARQUET", "SNAPPY"),
           ("parquet_gzip", "PARQUET", "GZIP"),
           ("parquet_zstd", "PARQUET", "ZSTD"),
           ("orc_zstd", "ORC", "ZSTD")]

def save(name, data):
    (RESULTS / name).write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")

def execute(sql):
    print(sql.splitlines()[0][:100], flush=True)
    with (RESULTS / "executed.sql").open("a") as f:
        f.write(sql.rstrip(";\n") + ";\n\n")
    return query(sql)

def count(table):
    return query(f"SELECT count(*) FROM {table}")["rows"][0][0]

def table_size(schema, table):
    return query(f'SELECT coalesce(sum(file_size_in_bytes),0) FROM iceberg.{schema}."{table}$files"')["rows"][0][0]

def check_equal(actual, expected):
    if actual != expected:
        raise AssertionError(f"Q3 mismatch: {actual!r} != {expected!r}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=int, default=1)
    parser.add_argument("--write-repeats", type=int, default=3)
    parser.add_argument("--query-repeats", type=int, default=5)
    args = parser.parse_args()
    if min(args.scale, args.write_repeats, args.query_repeats) < 1:
        parser.error("All numeric arguments must be positive")
    RESULTS.mkdir(parents=True, exist_ok=True)
    if (RESULTS / "run.json").exists():
        raise RuntimeError("Use a fresh Compose project/warehouse and results directory for another run")
    info = json.loads(subprocess.check_output(["docker", "info", "--format", "{{json .}}"], text=True))
    hardware = {key: info[key] for key in ("ServerVersion", "NCPU", "MemTotal", "Architecture")}
    started = datetime.now(timezone.utc).isoformat()
    save("run.json", {"status": "running", "parameters": vars(args), "host": platform.platform(),
                      "docker": hardware, "started_utc": started})
    for schema in ("bronze", "silver", "gold", "bench"):
        execute(f"CREATE SCHEMA IF NOT EXISTS iceberg.{schema}")
    lineage = []
    for table, columns in PROJECTIONS.items():
        out = execute(f"CREATE TABLE iceberg.bronze.{table} WITH (format='PARQUET', compression_codec='ZSTD') AS SELECT {columns} FROM tpch.sf{args.scale}.{table}")
        actual = count(f"iceberg.bronze.{table}")
        expected = count(f"tpch.sf{args.scale}.{table}")
        assert actual == expected
        lineage.append({"layer": "bronze", "table": table, "rows": actual,
                        "bytes": table_size("bronze", table), "write_seconds": out["wall_seconds"]})
    checks = {}
    validations = {
        "customer_duplicate_keys": "SELECT count(*)-count(DISTINCT custkey) FROM iceberg.bronze.customer",
        "orders_duplicate_keys": "SELECT count(*)-count(DISTINCT orderkey) FROM iceberg.bronze.orders",
        "lineitem_duplicate_keys": "SELECT count(*) FROM (SELECT orderkey,linenumber FROM iceberg.bronze.lineitem GROUP BY 1,2 HAVING count(*)>1)",
        "invalid_customer": "SELECT count(*) FROM iceberg.bronze.customer WHERE custkey IS NULL OR mktsegment IS NULL",
        "invalid_orders": "SELECT count(*) FROM iceberg.bronze.orders WHERE orderkey IS NULL OR custkey IS NULL OR orderdate IS NULL OR shippriority IS NULL",
        "invalid_lineitem": "SELECT count(*) FROM iceberg.bronze.lineitem WHERE orderkey IS NULL OR linenumber IS NULL OR shipdate IS NULL OR extendedprice IS NULL OR discount IS NULL OR extendedprice<0 OR discount NOT BETWEEN 0 AND 1",
        "orphan_orders": "SELECT count(*) FROM iceberg.bronze.orders o LEFT JOIN iceberg.bronze.customer c ON c.custkey=o.custkey WHERE c.custkey IS NULL",
        "orphan_lineitem": "SELECT count(*) FROM iceberg.bronze.lineitem l LEFT JOIN iceberg.bronze.orders o ON o.orderkey=l.orderkey WHERE o.orderkey IS NULL",
    }
    for name, sql in validations.items():
        checks[name] = query(sql)["rows"][0][0]
        assert checks[name] == 0, (name, checks[name])
    save("quality.json", checks)
    for layer, table in [("silver", "order_lines"), ("gold", "shipping_priority")]:
        execute((ROOT / "sql" / f"{layer}.sql").read_text())
        lineage.append({"layer": layer, "table": table, "rows": count(f"iceberg.{layer}.{table}"),
                        "bytes": table_size(layer, table)})
    save("layers.json", lineage)
    q3 = (ROOT / "sql/q3.sql").read_text()
    reference = query(q3.format(schema=f"tpch.sf{args.scale}"))["rows"]
    gold = query("SELECT * FROM iceberg.gold.shipping_priority ORDER BY revenue DESC,orderdate,orderkey")
    check_equal(gold["rows"], reference)
    save("gold.json", gold)
    original = q3.replace("CAST(l.extendedprice AS DECIMAL(18,2))", "l.extendedprice").replace("(DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))", "(1 - l.discount)")
    original_result = query(original.format(schema=f"tpch.sf{args.scale}"))["rows"]
    for a,b in zip(reference, original_result, strict=True):
        assert [a[0],a[2],a[3]] == [b[0],b[2],b[3]]
        assert abs(float(a[1])-float(b[1])) < 0.01
    save("teacher_reference.json", original_result)
    benchmark = []
    rng = random.Random(42)
    for repeat in range(args.write_repeats):
        configs = CONFIGS.copy()
        rng.shuffle(configs)
        for name, fmt, codec in configs:
            record = {"configuration": name, "format": fmt, "codec": codec, "repeat": repeat+1,
                      "write_seconds": 0, "bytes": 0, "write_queries": []}
            for table in PROJECTIONS:
                target = f"{name}_{table}"
                execute(f"DROP TABLE IF EXISTS iceberg.bench.{target}")
                result = execute(f"CREATE TABLE iceberg.bench.{target} WITH (format='{fmt}', compression_codec='{codec}') AS SELECT * FROM iceberg.bronze.{table}")
                record["write_seconds"] += result["wall_seconds"]
                record["write_queries"].append({"table": table, "id": result["id"], "stats": result["stats"]})
                assert count(f"iceberg.bench.{target}") == count(f"iceberg.bronze.{table}")
                record["bytes"] += table_size("bench", target)
            benchmark.append(record)
            save("writes.json", benchmark)
    reads = []
    def bench_sql(name):
        sql = q3.format(schema="iceberg.bench")
        for table in PROJECTIONS:
            sql = sql.replace(f"iceberg.bench.{table} ", f"iceberg.bench.{name}_{table} ")
        return sql
    for name, _, _ in CONFIGS:
        check_equal(query(bench_sql(name))["rows"], reference)
    for repeat in range(args.query_repeats):
        configs = CONFIGS.copy()
        rng.shuffle(configs)
        for name, _, _ in configs:
            result = execute(bench_sql(name))
            check_equal(result["rows"], reference)
            reads.append({"configuration": name, "repeat": repeat+1, "id": result["id"],
                          "query_seconds": result["wall_seconds"], "stats": result["stats"]})
            save("reads.json", reads)
    summary = []
    for name,fmt,codec in CONFIGS:
        w = [r["write_seconds"] for r in benchmark if r["configuration"]==name]
        r = [r["query_seconds"] for r in reads if r["configuration"]==name]
        sizes = [r["bytes"] for r in benchmark if r["configuration"]==name]
        summary.append({"configuration": name, "format": fmt, "codec": codec,
                        "bytes_median": statistics.median(sizes), "write_median_s": statistics.median(w),
                        "write_min_s": min(w), "write_max_s": max(w),
                        "query_median_s": statistics.median(r), "query_min_s": min(r), "query_max_s": max(r)})
    save("benchmark.json", summary)
    with (RESULTS / "benchmark.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(summary[0])); writer.writeheader(); writer.writerows(summary)
    save("evolution.json", demonstrate_evolution(execute))
    save("run.json", {"status": "complete", "parameters": vars(args), "host": platform.platform(),
                      "docker": hardware, "started_utc": started,
                      "finished_utc": datetime.now(timezone.utc).isoformat(),
                      "trino_version": query("SELECT version()")["rows"],
                      "containers": [{k: c.get(k) for k in ("Service", "Name", "State", "Health", "Image", "ID")}
                                     for c in (json.loads(line) for line in subprocess.check_output(
                                         ["docker","compose","ps","--format","json"], cwd=ROOT, text=True).splitlines())]})
    print("All checks passed. Results saved.", flush=True)

if __name__ == "__main__":
    main()
