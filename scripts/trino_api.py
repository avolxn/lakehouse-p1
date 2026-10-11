import json
import os
import time
import urllib.request

URL = os.environ.get("TRINO_URL", "http://127.0.0.1:18080")

def query(sql):
    start = time.perf_counter()
    request = urllib.request.Request(URL + "/v1/statement", data=sql.encode(), headers={
        "X-Trino-User": "student", "Content-Type": "text/plain; charset=utf-8"})
    rows, columns = [], []
    while True:
        with urllib.request.urlopen(request, timeout=600) as response:
            page = json.load(response)
        if "error" in page:
            raise RuntimeError(json.dumps(page["error"], ensure_ascii=False))
        if "columns" in page:
            columns = [c["name"] for c in page["columns"]]
        rows.extend(page.get("data", []))
        if not page.get("nextUri"):
            break
        request = urllib.request.Request(page["nextUri"])
    return {"id": page["id"], "columns": columns, "rows": rows,
            "wall_seconds": time.perf_counter() - start, "stats": page.get("stats", {})}

if __name__ == "__main__":
    import sys
    from pathlib import Path
    sql = Path(sys.argv[1]).read_text() if len(sys.argv) == 2 else sys.stdin.read()
    print(json.dumps(query(sql), ensure_ascii=False, indent=2))
