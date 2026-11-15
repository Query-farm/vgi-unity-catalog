"""Unit tests. The SQL-level tests live in test/sql and run through DuckDB."""

from __future__ import annotations

import json
import urllib.request
from typing import Any

import pyarrow as pa
import pytest

from unity_catalog_worker import client as uc
from unity_catalog_worker.assets import Datasets, ModelVersions, entity_tags, tag_labels
from unity_catalog_worker.governance import Grants, LineageEdges, mask_options, securables
from unity_catalog_worker.scan import Pushdown, catalogs_listing, drain, normalize_privilege, principal_type, ts
from unity_catalog_worker.worker import TABLES, UnityCatalogWorker


class FakePushdown(Pushdown):
    """A Pushdown whose ``=`` / ``IN`` values are given directly."""

    def __init__(self, **values: list[Any]) -> None:
        super().__init__(None)
        self._v = values

    def values(self, column: str) -> list[Any] | None:
        return self._v.get(column)


def make_client(url: str, **fields: Any) -> uc.UnityCatalogClient:
    return uc.UnityCatalogClient(uc.UnityCatalogConfig.from_secret({"host": url, "token": "test-token", **fields}))


def requests_seen(url: str) -> list[dict[str, Any]]:
    with urllib.request.urlopen(f"{url}/__requests") as resp:
        return list(json.load(resp))


def unrecorded(url: str) -> list[dict[str, Any]]:
    with urllib.request.urlopen(f"{url}/__unrecorded") as resp:
        return list(json.load(resp))


# ---------------------------------------------------------------- configuration


def test_config_auto_auth_and_flavor() -> None:
    cfg = uc.UnityCatalogConfig.from_secret({"host": "http://localhost:8080"})
    assert (cfg.auth_type, cfg.flavor, cfg.is_databricks) == ("none", "oss", False)
    cfg = uc.UnityCatalogConfig.from_secret({"host": "dbc-1.cloud.databricks.com", "token": "dapi1"})
    assert (cfg.host, cfg.auth_type, cfg.flavor) == ("https://dbc-1.cloud.databricks.com", "pat", "databricks")
    cfg = uc.UnityCatalogConfig.from_secret(
        {"host": "https://adb-1.7.azuredatabricks.net/", "client_id": "a", "client_secret": "b"}
    )
    assert (cfg.host, cfg.auth_type, cfg.flavor) == ("https://adb-1.7.azuredatabricks.net", "oauth-m2m", "databricks")
    cfg = uc.UnityCatalogConfig.from_secret({"host": "http://x/api/2.1/unity-catalog", "include_browse": "true"})
    assert cfg.host == "http://x" and cfg.include_browse


def test_config_accepts_arrow_scalars_and_never_reprs_secrets() -> None:
    cfg = uc.UnityCatalogConfig.from_secret({"host": pa.scalar("http://h"), "token": pa.scalar("s3cr3t")})
    assert cfg.token == "s3cr3t"
    assert "s3cr3t" not in repr(cfg)


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        (None, "no Unity Catalog connection"),
        ({"token": "x"}, "HOST must be set"),
        ({"host": "ftp://x"}, "http:// or https://"),
        ({"host": "http://x", "auth_type": "pat"}, "needs TOKEN"),
        ({"host": "http://x", "auth_type": "oauth-m2m", "client_id": "a"}, "CLIENT_SECRET"),
        ({"host": "http://x", "auth_type": "kerberos"}, "AUTH_TYPE must be one of"),
        ({"host": "http://x", "flavor": "snowflake"}, "FLAVOR must be one of"),
        ({"host": "http://x", "page_size": 0}, "PAGE_SIZE"),
        ({"host": "http://x", "timeout": -1}, "TIMEOUT"),
    ],
)
def test_config_errors(fields: dict[str, Any] | None, message: str) -> None:
    with pytest.raises(uc.UnityCatalogError, match=message):
        uc.UnityCatalogConfig.from_secret(fields)


def test_retry_delay_honours_retry_after() -> None:
    assert uc.retry_delay("3", 1) == 3.0
    assert uc.retry_delay("9999", 1) == uc.MAX_BACKOFF_SECONDS
    assert uc.retry_delay(None, 2) == 2.0


def test_endpoint_missing_detection() -> None:
    assert uc.endpoint_missing(404, "Status: 404\nDescription: Not Found")
    assert uc.endpoint_missing(500, {"error_code": "INTERNAL", "message": "Couldn't unwrap service."})
    assert uc.endpoint_missing(404, {"error_code": "ENDPOINT_NOT_FOUND", "message": "No API found"})
    assert not uc.endpoint_missing(404, {"error_code": "TABLE_NOT_FOUND", "message": "Table not found: a.b.c"})
    assert not uc.endpoint_missing(500, {"error_code": "INTERNAL", "message": "boom"})


# ---------------------------------------------------------------- conversions


def test_helpers() -> None:
    assert normalize_privilege("USE CATALOG") == "USE_CATALOG"
    assert normalize_privilege("select") == "SELECT"
    assert principal_type("alice@example.com") == "user"
    assert principal_type("8b5b1f8e-1c2d-4e5f-9a0b-1c2d3e4f5a6b") == "service_principal"
    assert principal_type("data-eng") == "group"
    assert ts(0) is None and ts(1794700000000) is not None
    assert ts("2026-11-14 06:00:00.0") is not None
    assert tag_labels([{"tag_key": "pii"}, {"tag_key": "tier", "tag_value": "gold"}]) == ["pii", "tier=gold"]
    assert tag_labels(None) is None


def test_mask_options_keeps_only_safe_settings() -> None:
    masked = dict(mask_options({"host": "db", "port": "5432", "user": "u", "password": "p", "PEM": "x"}) or [])
    assert masked == {"PEM": "****", "host": "db", "password": "****", "port": "5432", "user": "****"}


# ---------------------------------------------------------------- against the replay server


def test_listing_pages_and_survives_rate_limits(mock_uc: str) -> None:
    client = make_client(mock_uc, page_size=2)
    catalogs = drain(client, catalogs_listing(client.config))
    assert sorted(c["name"] for c in catalogs) == ["ml", "sales", "unity"]
    pages = [r for r in requests_seen(mock_uc) if r["path"].endswith("/catalogs")]
    assert len(pages) >= 2 and any("page_token=" in r["query"] for r in pages)
    assert not unrecorded(mock_uc)


def test_wrong_token_is_reported(mock_uc: str) -> None:
    client = uc.UnityCatalogClient(uc.UnityCatalogConfig.from_secret({"host": mock_uc, "token": "nope"}))
    with pytest.raises(uc.UnityCatalogError, match="HTTP 401"):
        client.get(f"{uc.UC_API}/catalogs", {"max_results": 2})


def test_table_scope_pushdown_lists_only_wanted_schemas(mock_uc: str) -> None:
    client = make_client(mock_uc, page_size=2)
    ids = Datasets.fanout(client, FakePushdown(catalog_name=["sales"], schema_name=["raw"]))
    assert ids == ["S:sales.raw"]
    listings = Datasets.requests(FakePushdown(), ids, client.config, {"id", "tags"})
    assert dict(listings[0].query) == {
        "catalog_name": "sales",
        "schema_name": "raw",
        "omit_columns": True,
        "omit_properties": True,
    }
    # selecting column_count keeps the columns in the response
    listings = Datasets.requests(FakePushdown(), ids, client.config, {"id", "column_count"})
    assert "omit_columns" not in dict(listings[0].query)
    # an id filter is a direct GET
    assert Datasets.fanout(client, FakePushdown(id=["sales.raw.orders"])) == ["T:sales.raw.orders"]


def test_securables_by_name_skip_enumeration() -> None:
    cfg = uc.UnityCatalogConfig.from_secret({"host": "http://unused"})
    pd = FakePushdown(securable_type=["table"], securable_name=["sales.raw.orders", "sales"])
    assert securables(uc.UnityCatalogClient(cfg), pd) == ["table|sales.raw.orders"]
    listing = Grants.requests(FakePushdown(principal=["bob@example.com"]), ["table|sales.raw.orders"], cfg, set())[0]
    assert listing.path.endswith("/permissions/table/sales.raw.orders")
    assert dict(listing.query) == {"principal": "bob@example.com"}


def test_storage_credential_securable_spelling() -> None:
    oss = uc.UnityCatalogConfig.from_secret({"host": "http://h"})
    dbx = uc.UnityCatalogConfig.from_secret({"host": "http://h", "flavor": "databricks"})
    assert Grants.requests(FakePushdown(), ["storage_credential|c"], oss, set())[0].path.endswith("/credential/c")
    assert Grants.requests(FakePushdown(), ["storage_credential|c"], dbx, set())[0].path.endswith(
        "/storage_credential/c"
    )


def test_model_versions_fanout_from_model_id() -> None:
    client = uc.UnityCatalogClient(uc.UnityCatalogConfig.from_secret({"host": "http://unused"}))
    assert ModelVersions.fanout(client, FakePushdown(model_id=["ml.models.churn"])) == ["ml.models.churn"]


def test_tags_are_none_on_open_source(mock_uc: str) -> None:
    client = make_client(mock_uc, page_size=2)
    assert entity_tags(client, "tables", "unity.default.numbers") is None


def test_tags_on_databricks(mock_dbx: str) -> None:
    client = make_client(mock_dbx, page_size=2, flavor="databricks")
    assert tag_labels(entity_tags(client, "tables", "sales.raw.customers")) == ["pii", "tier=silver"]


def test_lineage_modes() -> None:
    cfg = uc.UnityCatalogConfig.from_secret({"host": "http://unused"})
    client = uc.UnityCatalogClient(cfg)
    assert LineageEdges.fanout(client, FakePushdown(upstream_id=["a.b.c"])) == ["U:a.b.c"]
    assert LineageEdges.fanout(client, FakePushdown(downstream_id=["a.b.c"], upstream_id=["x.y.z"])) == ["D:a.b.c"]
    listing = LineageEdges.requests(FakePushdown(level=["column"]), ["D:a.b.c"], cfg, set())[0]
    assert dict(listing.body or ()) == {"include_entity_lineage": True, "table_name": "a.b.c"}
    assert listing.ctx("levels") == "column"


def test_catalog_shape() -> None:
    schema = UnityCatalogWorker.catalog.schemas[0]
    names = {t.name for t in schema.tables}
    assert {"datasets", "columns", "owners", "tags", "lineage_edges"} <= names
    assert len(TABLES) == 21
    # the shared data-catalog vocabulary
    for table in ("datasets", "columns", "owners", "lineage_edges", "tags"):
        fields = next(t for t in schema.tables if t.name == table).function.FIXED_SCHEMA  # type: ignore[union-attr]
        assert fields.field("catalog_tool").nullable is False
    datasets = Datasets.FIXED_SCHEMA.names
    for col in (
        "id",
        "platform",
        "qualified_name",
        "description",
        "created_at",
        "updated_at",
        "url",
        "database",
        "schema",
        "name",
        "subtype",
        "owners",
        "tags",
        "glossary_terms",
        "domain_id",
        "domain",
        "container_id",
        "deprecated",
    ):
        assert col in datasets, col


def test_qualified_name() -> None:
    from unity_catalog_worker.scalars import qualified_name

    assert qualified_name("Sales.RAW.Orders") == "sales.raw.orders"
    assert qualified_name("`a.b`.`c``d`") == "a.b.c`d"
    assert qualified_name("") is None
    assert qualified_name(None) is None
