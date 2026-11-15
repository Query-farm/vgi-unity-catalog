"""Build ``databricks_extras.json``: Databricks-only responses for the replay server.

Open-source Unity Catalog has no connections, Delta Sharing, effective permissions, tags or
lineage, so these exchanges are written by hand from the documented response shapes
(Databricks SDK ``databricks/sdk/service/catalog.py``, ``tags.py``, ``sharing.py`` and the
Data Lineage API docs) over the objects ``seed_uc.py`` creates. Everything else the
Databricks-flavoured tests read comes from the real recording.

    python test/fixtures/build_databricks_extras.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "mock_uc"))
from server import canonical_key

UC = "/api/2.1/unity-catalog"
OUT = Path(__file__).resolve().parent / "databricks_extras.json"
T0 = 1794700000000  # 2026-11-14T21:46:40Z
PAGE = "2"  # the tests' PAGE_SIZE

exchanges: list[dict[str, Any]] = []


def add(path: str, response: Any, query: str = "", body: Any = None, status: int = 200) -> None:
    raw = json.dumps(body, sort_keys=True).encode() if body is not None else b""
    exchanges.append(
        {
            "key": canonical_key("GET", path, query, raw),
            "status": status,
            "content_type": "application/json",
            "response": json.dumps(response, sort_keys=True),
        }
    )


add(
    f"{UC}/metastore_summary",
    {
        "metastore_id": "11111111-2222-3333-4444-555555555555",
        "name": "acme-us-east-1",
        "cloud": "aws",
        "region": "us-east-1",
        "global_metastore_id": "aws:us-east-1:11111111-2222-3333-4444-555555555555",
        "owner": "metastore-admins",
        "storage_root": "s3://acme-uc-root/metastore",
        "delta_sharing_scope": "INTERNAL_AND_EXTERNAL",
        "delta_sharing_organization_name": "acme",
        "privilege_model_version": "1.0",
        "external_access_enabled": True,
        "created_at": T0,
        "created_by": "admin@acme.com",
        "updated_at": T0 + 1000,
        "updated_by": "admin@acme.com",
    },
)

add(
    f"{UC}/connections",
    {
        "connections": [
            {
                "name": "pg_orders",
                "connection_type": "POSTGRESQL",
                "credential_type": "USERNAME_PASSWORD",
                "options": {"host": "orders-db.internal", "port": "5432", "user": "svc_reader", "password": "hunter2"},
                "read_only": True,
                "provisioning_info": {"state": "ACTIVE"},
                "comment": "ERP replica",
                "owner": "data-platform",
                "connection_id": "c0ffee00-0000-4000-8000-000000000001",
                "full_name": "pg_orders",
                "created_at": T0,
                "created_by": "admin@acme.com",
            },
            {
                "name": "sf_finance",
                "connection_type": "SNOWFLAKE",
                "credential_type": "OAUTH",
                "options": {"host": "acme.snowflakecomputing.com", "sfWarehouse": "FIN_WH", "client_secret": "s3cr3t"},
                "read_only": True,
                "owner": "alice@example.com",
                "connection_id": "c0ffee00-0000-4000-8000-000000000002",
                "created_at": T0 + 5000,
            },
        ]
    },
    query=f"max_results={PAGE}",
)

add(
    f"{UC}/shares",
    {
        "shares": [
            {
                "name": "partner_sales",
                "comment": "Daily revenue for partners",
                "owner": "data-platform",
                "created_at": T0,
                "created_by": "admin@acme.com",
            },
        ]
    },
    query=f"max_results={PAGE}",
)

add(
    f"{UC}/recipients",
    {
        "recipients": [
            {
                "name": "globex",
                "authentication_type": "TOKEN",
                "activated": False,
                "activation_url": "https://acme.cloud.databricks.com/delta_sharing/retrieve_config.html?SECRET",
                "tokens": [{"id": "tok-1", "activation_url": "https://example.invalid/SECRET"}],
                "comment": "Open sharing recipient",
                "owner": "data-platform",
                "created_at": T0,
            },
            {
                "name": "initech",
                "authentication_type": "DATABRICKS",
                "activated": True,
                "cloud": "azure",
                "region": "westeurope",
                "data_recipient_global_metastore_id": "azure:westeurope:99999999-0000-0000-0000-000000000000",
                "sharing_code": "SECRET-CODE",
                "owner": "data-platform",
                "created_at": T0,
            },
        ]
    },
    query=f"max_results={PAGE}",
)

add(
    f"{UC}/storage-credentials",
    {
        "storage_credentials": [
            {
                "name": "lake_role",
                "aws_iam_role": {"role_arn": "arn:aws:iam::123456789012:role/uc-lake", "external_id": "SECRET-ID"},
                "comment": "IAM role for the lake bucket",
                "owner": "data-platform",
                "read_only": False,
                "used_for_managed_storage": True,
                "isolation_mode": "ISOLATION_MODE_OPEN",
                "id": "5ca1ab1e-0000-4000-8000-000000000001",
                "created_at": T0,
            },
            {
                "name": "adls_mi",
                "azure_managed_identity": {"access_connector_id": "/subscriptions/x/accessConnectors/uc"},
                "owner": "data-platform",
                "read_only": True,
                "id": "5ca1ab1e-0000-4000-8000-000000000002",
                "created_at": T0,
            },
        ]
    },
    query=f"max_results={PAGE}",
)

add(
    f"{UC}/effective-permissions/table/sales.raw.orders",
    {
        "privilege_assignments": [
            {
                "principal": "alice@example.com",
                "privileges": [
                    {"privilege": "SELECT"},
                    {"privilege": "USE_CATALOG", "inherited_from_type": "CATALOG", "inherited_from_name": "sales"},
                ],
            },
            {
                "principal": "analysts",
                "privileges": [
                    {"privilege": "SELECT", "inherited_from_type": "SCHEMA", "inherited_from_name": "sales.raw"}
                ],
            },
        ]
    },
)

add(
    "/api/2.1/tag-policies",
    {
        "tag_policies": [
            {
                "tag_key": "pii",
                "description": "Personally identifiable information",
                "id": "tp-1",
                "values": [{"name": "email"}, {"name": "phone"}],
                "create_time": "2026-11-01T10:00:00Z",
                "update_time": "2026-11-02T10:00:00Z",
            },
            {
                "tag_key": "tier",
                "description": "Data product tier",
                "id": "tp-2",
                "values": [{"name": "gold"}, {"name": "silver"}],
                "create_time": "2026-11-01T10:00:00Z",
            },
        ]
    },
    query=f"page_size={PAGE}",
)


def tags(entity_type: str, name: str, assignments: list[tuple[str, str | None]]) -> None:
    add(
        f"{UC}/entity-tag-assignments/{entity_type}/{name}/tags",
        {
            "tag_assignments": [
                {
                    "entity_type": entity_type,
                    "entity_name": name,
                    "tag_key": k,
                    **({"tag_value": v} if v is not None else {}),
                    "source_type": "TAG_ASSIGNMENT_SOURCE_TYPE_USER",
                    "update_time": "2026-11-03T08:00:00Z",
                    "updated_by": "alice@example.com",
                }
                for k, v in assignments
            ]
        },
        query=f"max_results={PAGE}",
    )


tags("tables", "sales.raw.customers", [("pii", None), ("tier", "silver")])
tags("tables", "sales.raw.orders", [("tier", "gold")])
tags("tables", "sales.marts.daily_revenue", [])
tags("columns", "sales.raw.customers.customer_id", [])
tags("columns", "sales.raw.customers.email", [("pii", "email")])
tags("columns", "sales.raw.customers.region", [])

LINEAGE = "/api/2.0/lineage-tracking"


def table_info(cat: str, sch: str, name: str, kind: str = "TABLE") -> dict[str, Any]:
    return {
        "name": name,
        "catalog_name": cat,
        "schema_name": sch,
        "table_type": kind,
        "lineage_timestamp": "2026-11-14 06:00:00.0",
    }


def lineage(table: str, response: dict[str, Any]) -> None:
    params = {"include_entity_lineage": True, "table_name": table}
    add(f"{LINEAGE}/table-lineage", response, query=f"include_entity_lineage=true&table_name={table}", body=params)


lineage(
    "sales.marts.daily_revenue",
    {
        "upstreams": [
            {
                "tableInfo": table_info("sales", "raw", "orders"),
                "jobInfos": [{"workspace_id": 1234567890, "job_id": 101, "job_run_id": 9001}],
            },
        ],
        "downstreams": [
            {"dashboardV3Infos": [{"workspace_id": 1234567890, "dashboard_id": "01ef-revenue"}]},
        ],
    },
)
lineage(
    "sales.raw.orders",
    {
        "upstreams": [{"notebookInfos": [{"workspace_id": 1234567890, "notebook_id": 555}]}],
        "downstreams": [
            {
                "tableInfo": table_info("sales", "marts", "daily_revenue", "VIEW"),
                "jobInfos": [{"workspace_id": 1234567890, "job_id": 101}],
            },
        ],
    },
)


def column_lineage(table: str, col: str, response: dict[str, Any]) -> None:
    params = {"column_name": col, "table_name": table}
    add(f"{LINEAGE}/column-lineage", response, query=f"column_name={col}&table_name={table}", body=params)


def col_info(cat: str, sch: str, table: str, name: str) -> dict[str, Any]:
    return {
        "name": name,
        "catalog_name": cat,
        "schema_name": sch,
        "table_name": table,
        "table_type": "TABLE",
        "lineage_timestamp": "2026-11-14 06:00:00.0",
    }


column_lineage(
    "sales.marts.daily_revenue",
    "order_date",
    {"upstream_cols": [col_info("sales", "raw", "orders", "order_date")], "downstream_cols": []},
)
column_lineage(
    "sales.marts.daily_revenue",
    "revenue",
    {"upstream_cols": [col_info("sales", "raw", "orders", "amount")], "downstream_cols": []},
)
for c in ("order_id", "customer_id", "ordered_at", "tags"):
    column_lineage("sales.raw.orders", c, {"upstream_cols": [], "downstream_cols": []})
column_lineage(
    "sales.raw.orders",
    "amount",
    {"upstream_cols": [], "downstream_cols": [col_info("sales", "marts", "daily_revenue", "revenue")]},
)
column_lineage(
    "sales.raw.orders",
    "order_date",
    {"upstream_cols": [], "downstream_cols": [col_info("sales", "marts", "daily_revenue", "order_date")]},
)

OUT.write_text(json.dumps(sorted(exchanges, key=lambda e: e["key"]), indent=1, sort_keys=True) + "\n", encoding="utf-8")
print(f"wrote {len(exchanges)} exchanges to {OUT}", file=sys.stderr)
