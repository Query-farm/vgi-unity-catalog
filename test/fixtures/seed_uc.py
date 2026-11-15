"""Seed a disposable Unity Catalog OSS server with the objects the SQL suite reads.

Run against a throw-away ``unitycatalog/unitycatalog`` container (see README "Testing"):

    python test/fixtures/seed_uc.py http://localhost:18080 <admin token>

The admin token is the container's ``etc/conf/token.txt`` (authorization enabled). It is
a local, per-container token and is never written anywhere.
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from typing import Any

BASE = sys.argv[1].rstrip("/")
TOKEN = sys.argv[2]
UC = f"{BASE}/api/2.1/unity-catalog"


def call(method: str, url: str, body: Any = None) -> Any:
    data = json.dumps(body).encode() if body is not None else None
    ctype = "application/scim+json" if "/scim2/" in url else "application/json"
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": ctype},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as err:
        text = err.read().decode(errors="replace")
        if err.code == 409 or "ALREADY_EXISTS" in text:
            return None
        raise SystemExit(f"{method} {url} -> {err.code}: {text}") from err


def col(
    name: str,
    type_text: str,
    type_name: str,
    json_type: Any,
    pos: int,
    comment: str | None = None,
    nullable: bool = True,
    partition_index: int | None = None,
    precision: int = 0,
    scale: int = 0,
) -> dict[str, Any]:
    return {
        "name": name,
        "type_text": type_text,
        "type_name": type_name,
        "type_json": json.dumps({"name": name, "type": json_type, "nullable": nullable, "metadata": {}}),
        "type_precision": precision,
        "type_scale": scale,
        "position": pos,
        "comment": comment,
        "nullable": nullable,
        "partition_index": partition_index,
    }


def main() -> None:
    for email, name in (("alice@example.com", "Alice Analyst"), ("bob@example.com", "Bob Builder")):
        call(
            "POST",
            f"{BASE}/api/1.0/unity-control/scim2/Users",
            {
                "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
                "displayName": name,
                "emails": [{"value": email, "primary": True}],
            },
        )

    call(
        "POST",
        f"{UC}/catalogs",
        {"name": "sales", "comment": "Sales data products", "properties": {"team": "finance", "tier": "gold"}},
    )
    call("POST", f"{UC}/catalogs", {"name": "ml", "comment": "Machine learning assets"})
    call(
        "POST",
        f"{UC}/schemas",
        {"name": "raw", "catalog_name": "sales", "comment": "Landed source data", "properties": {"source": "erp"}},
    )
    call("POST", f"{UC}/schemas", {"name": "marts", "catalog_name": "sales", "comment": "Curated marts"})
    call("POST", f"{UC}/schemas", {"name": "models", "catalog_name": "ml", "comment": "Registered models"})

    order_cols = [
        col("order_id", "bigint", "LONG", "long", 0, "Order key", nullable=False),
        col("customer_id", "bigint", "LONG", "long", 1, "Customer key"),
        col("amount", "decimal(18,2)", "DECIMAL", "decimal(18,2)", 2, "Order total", precision=18, scale=2),
        col("ordered_at", "timestamp", "TIMESTAMP", "timestamp", 3, "When the order was placed"),
        col(
            "tags",
            "array<string>",
            "ARRAY",
            {"type": "array", "elementType": "string", "containsNull": True},
            4,
            "Free-form labels",
        ),
        col("order_date", "date", "DATE", "date", 5, "Partition date", nullable=False, partition_index=0),
    ]
    call(
        "POST",
        f"{UC}/tables",
        {
            "name": "orders",
            "catalog_name": "sales",
            "schema_name": "raw",
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "columns": order_cols,
            "storage_location": "s3://acme-lake/raw/orders",
            "comment": "Orders landed nightly from the ERP",
            "properties": {"delta.appendOnly": "true", "pii": "false"},
        },
    )
    call(
        "POST",
        f"{UC}/tables",
        {
            "name": "customers",
            "catalog_name": "sales",
            "schema_name": "raw",
            "table_type": "EXTERNAL",
            "data_source_format": "PARQUET",
            "columns": [
                col("customer_id", "bigint", "LONG", "long", 0, "Customer key", nullable=False),
                col("email", "string", "STRING", "string", 1, "Contact email (PII)"),
                col("region", "string", "STRING", "string", 2, "Sales region"),
            ],
            "storage_location": "s3://acme-lake/raw/customers",
            "comment": "Customer master",
        },
    )
    call(
        "POST",
        f"{UC}/tables",
        {
            "name": "daily_revenue",
            "catalog_name": "sales",
            "schema_name": "marts",
            "table_type": "VIEW",
            "columns": [
                col("order_date", "date", "DATE", "date", 0, "Day"),
                col(
                    "revenue", "decimal(28,2)", "DECIMAL", "decimal(28,2)", 1, "Revenue that day", precision=28, scale=2
                ),
            ],
            "view_definition": "SELECT order_date, sum(amount) AS revenue FROM sales.raw.orders GROUP BY order_date",
            "comment": "Revenue per day",
        },
    )
    call(
        "POST",
        f"{UC}/volumes",
        {
            "catalog_name": "sales",
            "schema_name": "raw",
            "name": "landing",
            "volume_type": "EXTERNAL",
            "storage_location": "s3://acme-lake/landing",
            "comment": "Raw file drops",
        },
    )
    call(
        "POST",
        f"{UC}/functions",
        {
            "function_info": {
                "name": "cents_to_dollars",
                "catalog_name": "sales",
                "schema_name": "marts",
                "input_params": {
                    "parameters": [
                        {
                            "name": "cents",
                            "type_text": "bigint",
                            "type_name": "LONG",
                            "type_json": json.dumps(
                                {"name": "cents", "type": "long", "nullable": True, "metadata": {}}
                            ),
                            "position": 0,
                            "comment": "Amount in cents",
                        }
                    ]
                },
                "data_type": "DECIMAL",
                "full_data_type": "DECIMAL(18,2)",
                "routine_body": "SQL",
                "routine_definition": "CAST(cents / 100.0 AS DECIMAL(18,2))",
                "parameter_style": "S",
                "is_deterministic": True,
                "sql_data_access": "CONTAINS_SQL",
                "is_null_call": True,
                "security_type": "DEFINER",
                "specific_name": "cents_to_dollars",
                "comment": "Convert cents to dollars",
            }
        },
    )
    call(
        "POST",
        f"{UC}/models",
        {"name": "churn", "catalog_name": "ml", "schema_name": "models", "comment": "Customer churn classifier"},
    )
    for run in ("run-001", "run-002"):
        v = call(
            "POST",
            f"{UC}/models/versions",
            {
                "model_name": "churn",
                "catalog_name": "ml",
                "schema_name": "models",
                "source": f"s3://acme-ml/{run}",
                "run_id": run,
                "comment": f"Trained by {run}",
            },
        )
        if v and run == "run-001":
            call(
                "PATCH",
                f"{UC}/models/ml.models.churn/versions/{v['version']}/finalize",
                {"full_name": "ml.models.churn", "version": v["version"]},
            )
    call(
        "POST",
        f"{UC}/credentials",
        {
            "name": "lake_role",
            "comment": "IAM role for the lake bucket",
            "purpose": "STORAGE",
            "aws_iam_role": {"role_arn": "arn:aws:iam::123456789012:role/uc-lake"},
        },
    )
    call(
        "POST",
        f"{UC}/external-locations",
        {"name": "lake_raw", "url": "s3://acme-lake/raw", "credential_name": "lake_role", "comment": "Raw zone"},
    )

    grants = [
        ("catalog", "sales", "alice@example.com", ["USE CATALOG"]),
        ("catalog", "sales", "bob@example.com", ["USE CATALOG", "CREATE SCHEMA"]),
        ("schema", "sales.raw", "bob@example.com", ["USE SCHEMA", "CREATE TABLE"]),
        ("table", "sales.raw.orders", "alice@example.com", ["SELECT"]),
        ("table", "sales.raw.orders", "bob@example.com", ["SELECT", "MODIFY"]),
        ("volume", "sales.raw.landing", "bob@example.com", ["READ VOLUME"]),
        ("function", "sales.marts.cents_to_dollars", "alice@example.com", ["EXECUTE"]),
        ("registered_model", "ml.models.churn", "alice@example.com", ["EXECUTE"]),
    ]
    for securable, name, principal, privs in grants:
        call(
            "PATCH",
            f"{UC}/permissions/{securable}/{name}",
            {"changes": [{"principal": principal, "add": privs, "remove": []}]},
        )
    print("seeded", file=sys.stderr)


if __name__ == "__main__":
    main()
