import json
import urllib.request

BASE = "http://127.0.0.1:18181/management/v1"

def call(path, payload=None):
    req = urllib.request.Request(BASE + path, headers={"Content-Type": "application/json"},
                                 data=None if payload is None else json.dumps(payload).encode())
    with urllib.request.urlopen(req, timeout=60) as response:
        data = response.read()
        return json.loads(data) if data else {}

info = call("/info")
if not info.get("bootstrapped", False):
    call("/bootstrap", {"accept-terms-of-use": True})
warehouses = call("/warehouse?project-id=00000000-0000-0000-0000-000000000000")
if not any(w["name"] == "lakehouse" for w in warehouses.get("warehouses", [])):
    call("/warehouse", {
        "warehouse-name": "lakehouse", "project-id": "00000000-0000-0000-0000-000000000000",
        "storage-profile": {"type": "s3", "bucket": "lakehouse", "key-prefix": "data",
                            "endpoint": "http://minio:9000", "region": "us-east-1",
                            "path-style-access": True, "flavor": "minio", "sts-enabled": False},
        "storage-credential": {"type": "s3", "credential-type": "access-key",
                               "aws-access-key-id": "lakehouse",
                               "aws-secret-access-key": "local-minio-password"}})
print("Warehouse lakehouse ready")
