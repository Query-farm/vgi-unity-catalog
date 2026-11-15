"""Unity Catalog securables as tables.

Metastore, catalogs, schemas, tables / views, columns, volumes, functions,
registered models and their versions.

Each class is the scan behind one catalog table. ``fanout`` / ``requests``
document, in code, exactly which SQL predicates reach the API; everything else
is filtered by DuckDB (``auto_apply_filters`` re-checks every pushed predicate).
Timestamps (epoch milliseconds in the API) become ``TIMESTAMPTZ``.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pyarrow as pa

from .client import UC_API, EndpointMissingError, NotFoundError, UnityCatalogClient, UnityCatalogConfig, quote
from .meta import catalog_tool_column, column
from .scan import (
    STR_LIST,
    STR_MAP,
    TSTZ,
    ApiScan,
    Listing,
    Pushdown,
    catalog_names,
    catalogs_listing,
    common,
    drain,
    explore_path,
    full_name,
    lower,
    names_from_full,
    schema_scope,
    schemas_listing,
    split_scope,
    str_map,
    to_int,
    ts,
)

TABLE_PAGE_MAX = 50  # Databricks caps tables/list max_results at 50
VIEW_TYPES = ("VIEW", "MATERIALIZED_VIEW", "METRIC_VIEW", "STREAMING_TABLE")


def _common_cols(id_doc: str) -> list[pa.Field[Any]]:
    return [
        catalog_tool_column(),
        column("id", pa.string(), id_doc, nullable=False),
        column("platform", pa.string(), "'databricks' for a Databricks workspace, 'unity_catalog' for open source."),
    ]


def _times() -> list[pa.Field[Any]]:
    return [
        column("created_at", TSTZ, "When the object was created."),
        column("created_by", pa.string(), "Principal that created the object."),
        column("updated_at", TSTZ, "When the object was last changed."),
        column("updated_by", pa.string(), "Principal that last changed the object."),
    ]


def _who(item: dict[str, Any]) -> dict[str, Any]:
    return {"created_by": item.get("created_by"), "updated_by": item.get("updated_by"), "owner": item.get("owner")}


# ---------------------------------------------------------------------------- tags

TAGS_API = f"{UC_API}/entity-tag-assignments"


def entity_tags(client: UnityCatalogClient, entity_type: str, name: str) -> list[dict[str, Any]] | None:
    """Tag assignments of one securable (``tables``, ``columns``, ...); None when the server has no tags API."""
    out: list[dict[str, Any]] = []
    token = ""
    try:
        while True:
            query: dict[str, Any] = {"max_results": min(client.config.page_size, 1000)}
            if token:
                query["page_token"] = token
            body = client.get(f"{TAGS_API}/{entity_type}/{quote(name)}/tags", query)
            out.extend(t for t in (body.get("tag_assignments") or []) if isinstance(t, dict))
            token = str(body.get("next_page_token") or "")
            if not token:
                return out
    except EndpointMissingError:
        return None
    except NotFoundError:
        return []


def tag_labels(tags: list[dict[str, Any]] | None) -> list[str] | None:
    """``key`` or ``key=value`` labels, sorted; None when tags are unavailable."""
    if tags is None:
        return None
    labels = {f"{t['tag_key']}={t['tag_value']}" if t.get("tag_value") else str(t["tag_key"]) for t in tags}
    return sorted(labels)


# ---------------------------------------------------------------------------- metastores


class Metastores(ApiScan):
    """Rows for ``unity_catalog.main.metastores``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            catalog_tool_column(),
            column("metastore_id", pa.string(), "Metastore id (UUID).", nullable=False),
            column("name", pa.string(), "Metastore name (Databricks; NULL on open source)."),
            column("cloud", pa.string(), "Cloud of a Databricks metastore: aws, azure or gcp."),
            column("region", pa.string(), "Cloud region of a Databricks metastore."),
            column("global_metastore_id", pa.string(), "Delta Sharing identifier: <cloud>:<region>:<uuid>."),
            column("owner", pa.string(), "Metastore admin (user or group)."),
            column("storage_root", pa.string(), "Default managed storage URL."),
            column("storage_root_credential_name", pa.string(), "Storage credential used for the storage root."),
            column("delta_sharing_scope", pa.string(), "INTERNAL or INTERNAL_AND_EXTERNAL."),
            column("delta_sharing_organization_name", pa.string(), "Organization name shown to share recipients."),
            column("privilege_model_version", pa.string(), "Privilege model version, e.g. '1.0'."),
            column("external_access_enabled", pa.bool_(), "Whether external engines may read via credential vending."),
            *_times(),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "metastores_scan"
        description = "The metastore this Unity Catalog server or workspace serves."
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str], cfg: UnityCatalogConfig, wanted: set[str]) -> list[Listing]:
        """One ``GET /metastore_summary`` (both open source and Databricks)."""
        return [Listing(f"{UC_API}/metastore_summary", None, paged=False)]

    @classmethod
    def rows(
        cls, client: UnityCatalogClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map the summary to one row."""
        out = []
        for m in items:
            if not m.get("metastore_id"):
                continue
            out.append(
                {
                    "catalog_tool": "unity_catalog",
                    **{k: m.get(k) for k in cls.FIXED_SCHEMA.names if k in m},
                    "external_access_enabled": m.get("external_access_enabled"),
                    "created_at": ts(m.get("created_at")),
                    "updated_at": ts(m.get("updated_at")),
                }
            )
        return out


# ---------------------------------------------------------------------------- catalogs


class Catalogs(ApiScan):
    """Rows for ``unity_catalog.main.catalogs``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            *_common_cols("Catalog name (Unity Catalog's full name of a catalog)."),
            column("name", pa.string(), "Catalog name.", nullable=False),
            column("qualified_name", pa.string(), "Lower-cased catalog name."),
            column("description", pa.string(), "Catalog comment."),
            column(
                "catalog_type",
                pa.string(),
                "Databricks: MANAGED_CATALOG, DELTASHARING_CATALOG, FOREIGN_CATALOG, SYSTEM_CATALOG, ...",
            ),
            column("owner", pa.string(), "Owner principal (user, group or service principal)."),
            column("properties", STR_MAP, "User-defined key/value properties."),
            column("storage_root", pa.string(), "Managed storage root URL set on the catalog."),
            column("storage_location", pa.string(), "Effective managed storage location."),
            column("provider_name", pa.string(), "Delta Sharing provider of a shared catalog."),
            column("share_name", pa.string(), "Delta Sharing share a shared catalog mounts."),
            column("connection_name", pa.string(), "Lakehouse Federation connection of a foreign catalog."),
            column("isolation_mode", pa.string(), "OPEN or ISOLATED (bound to specific workspaces)."),
            column("browse_only", pa.bool_(), "True when the principal can only browse it (include_browse)."),
            column("metastore_id", pa.string(), "Metastore the catalog belongs to."),
            column("uuid", pa.string(), "Catalog UUID."),
            *_times(),
            column("url", pa.string(), "Catalog Explorer link (Databricks only)."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "catalogs_scan"
        description = "Unity Catalog catalogs."
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str], cfg: UnityCatalogConfig, wanted: set[str]) -> list[Listing]:
        """``name`` / ``id`` = or IN -> ``GET /catalogs/{name}`` each; else ``GET /catalogs``."""
        names = pd.strs("name", "id")
        if names is not None:
            return [Listing(f"{UC_API}/catalogs/{quote(n)}", None, paged=False, missing_ok=True) for n in names]
        return [catalogs_listing(cfg)]

    @classmethod
    def rows(
        cls, client: UnityCatalogClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map catalogs to rows."""
        out = []
        for c in items:
            name = c.get("name")
            if not name:
                continue
            out.append(
                {
                    **common(
                        client, id_=name, qualified=name, description=c.get("comment"), item=c, ui=explore_path(name)
                    ),
                    **_who(c),
                    "name": name,
                    "catalog_type": c.get("catalog_type"),
                    "properties": str_map(c.get("properties")),
                    "storage_root": c.get("storage_root"),
                    "storage_location": c.get("storage_location"),
                    "provider_name": c.get("provider_name"),
                    "share_name": c.get("share_name"),
                    "connection_name": c.get("connection_name"),
                    "isolation_mode": c.get("isolation_mode"),
                    "browse_only": c.get("browse_only"),
                    "metastore_id": c.get("metastore_id"),
                    "uuid": c.get("id"),
                }
            )
        return out


# ---------------------------------------------------------------------------- schemas


class Schemas(ApiScan):
    """Rows for ``unity_catalog.main.schemas``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            *_common_cols("Schema full name, 'catalog.schema'."),
            column("catalog_name", pa.string(), "Parent catalog.", nullable=False),
            column("name", pa.string(), "Schema name.", nullable=False),
            column("qualified_name", pa.string(), "Lower-cased 'catalog.schema'."),
            column("description", pa.string(), "Schema comment."),
            column("owner", pa.string(), "Owner principal."),
            column("properties", STR_MAP, "User-defined key/value properties."),
            column("storage_root", pa.string(), "Managed storage root URL set on the schema."),
            column("storage_location", pa.string(), "Effective managed storage location."),
            column("catalog_type", pa.string(), "Databricks: type of the parent catalog."),
            column("browse_only", pa.bool_(), "True when the principal can only browse it."),
            column("metastore_id", pa.string(), "Metastore the schema belongs to."),
            column("uuid", pa.string(), "Schema UUID."),
            *_times(),
            column("url", pa.string(), "Catalog Explorer link (Databricks only)."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "schemas_scan"
        description = "Unity Catalog schemas (databases)."
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def fanout(cls, client: UnityCatalogClient, pd: Pushdown) -> list[str] | None:
        """Catalogs in scope: ``catalog_name`` = / IN, the catalog part of ``id`` values, or every catalog."""
        ids = names_from_full(pd.strs("id"), 2)
        if ids is not None and pd.strs("catalog_name") is None:
            return sorted({split_scope(i)[0] for i in ids})
        return catalog_names(client, pd)

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str], cfg: UnityCatalogConfig, wanted: set[str]) -> list[Listing]:
        """``GET /schemas?catalog_name=`` per catalog in scope."""
        return [schemas_listing(cfg, cat) for cat in ids]

    @classmethod
    def rows(
        cls, client: UnityCatalogClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map schemas to rows."""
        out = []
        for s in items:
            cat, name = s.get("catalog_name") or listing.ctx("catalog_name"), s.get("name")
            if not cat or not name:
                continue
            fn = s.get("full_name") or full_name(cat, name)
            out.append(
                {
                    **common(
                        client, id_=fn, qualified=fn, description=s.get("comment"), item=s, ui=explore_path(cat, name)
                    ),
                    **_who(s),
                    "catalog_name": cat,
                    "name": name,
                    "properties": str_map(s.get("properties")),
                    "storage_root": s.get("storage_root"),
                    "storage_location": s.get("storage_location"),
                    "catalog_type": s.get("catalog_type"),
                    "browse_only": s.get("browse_only"),
                    "metastore_id": s.get("metastore_id"),
                    "uuid": s.get("schema_id"),
                }
            )
        return out


# ---------------------------------------------------------------------------- tables / views / columns


def _table_scope(client: UnityCatalogClient, pd: Pushdown, id_col: str) -> list[str]:
    """Fan-out ids for table scans: ``T:<full name>`` for direct gets, ``S:<catalog.schema>`` for listings.

    ``=`` / ``IN`` on the id column (or on ``qualified_name``) fetches those tables
    directly. Otherwise the catalog and schema filters (``catalog_name`` / ``database``,
    ``schema_name`` / ``schema``) bound the schemas that are listed.
    """
    direct = names_from_full(pd.strs(id_col) or pd.strs("qualified_name"), 3)
    if direct is not None:
        return [f"T:{n}" for n in direct]
    pairs = schema_scope(client, pd, ("catalog_name", "database"), ("schema_name", "schema"))
    return [f"S:{p}" for p in pairs]


def _table_listings(
    ids: list[str], cfg: UnityCatalogConfig, *, omit_columns: bool, omit_properties: bool
) -> list[Listing]:
    out = []
    for i in ids:
        kind, _, name = i.partition(":")
        if kind == "T":
            out.append(Listing(f"{UC_API}/tables/{quote(name)}", None, paged=False, missing_ok=True))
            continue
        cat, sch = split_scope(name)
        query: list[tuple[str, Any]] = [("catalog_name", cat), ("schema_name", sch)]
        if omit_columns:
            query.append(("omit_columns", True))
        if omit_properties:
            query.append(("omit_properties", True))
        if cfg.include_browse:
            query.append(("include_browse", True))
        out.append(Listing(f"{UC_API}/tables", "tables", tuple(query), max_size=TABLE_PAGE_MAX, missing_ok=True))
    return out


def primary_key_columns(table: dict[str, Any]) -> set[str]:
    """Columns of a Databricks table's informational primary key constraint."""
    out: set[str] = set()
    for c in table.get("table_constraints") or []:
        pk = c.get("primary_key_constraint") if isinstance(c, dict) else None
        if isinstance(pk, dict):
            out.update(str(x) for x in pk.get("child_columns") or [])
    return out


def view_dependencies(table: dict[str, Any]) -> list[str] | None:
    """Full names of the tables and functions a view depends on."""
    deps = (table.get("view_dependencies") or {}).get("dependencies")
    if deps is None:
        return None
    out = []
    for d in deps:
        if not isinstance(d, dict):
            continue
        name = (d.get("table") or {}).get("table_full_name") or (d.get("function") or {}).get("function_full_name")
        if name:
            out.append(str(name))
    return out


class Datasets(ApiScan):
    """Rows for ``unity_catalog.main.datasets`` (tables and views)."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            *_common_cols("Table full name, 'catalog.schema.table'."),
            column("environment", pa.string(), "Not tracked by Unity Catalog; always NULL (shared vocabulary)."),
            column("database", pa.string(), "Catalog of the table (the vocabulary's database part).", nullable=False),
            column("schema", pa.string(), "Schema of the table.", nullable=False),
            column("name", pa.string(), "Table name.", nullable=False),
            column(
                "qualified_name",
                pa.string(),
                "Lower-cased 'catalog.schema.table'; the join key to dbt, OpenLineage, Looker and the other catalog "
                "workers.",
            ),
            column("catalog_name", pa.string(), "Catalog of the table (same as database).", nullable=False),
            column("schema_name", pa.string(), "Schema of the table (same as schema).", nullable=False),
            column("subtype", pa.string(), "Lower-cased table_type: managed, external, view, materialized_view, ..."),
            column(
                "table_type",
                pa.string(),
                "MANAGED, EXTERNAL, VIEW, MATERIALIZED_VIEW, STREAMING_TABLE, METRIC_VIEW, FOREIGN, ...",
            ),
            column("data_source_format", pa.string(), "DELTA, ICEBERG, PARQUET, CSV, JSON, ... (NULL for views)."),
            column("storage_location", pa.string(), "Storage URL of the table's files."),
            column("view_definition", pa.string(), "SQL text of a view (NULL for tables)."),
            column("description", pa.string(), "Table comment."),
            column("owner", pa.string(), "Owner principal."),
            column("owners", STR_LIST, "Owner as a one-element list (shared vocabulary); see the owners table."),
            column(
                "tags",
                STR_LIST,
                "Databricks governed / user tags as 'key' or 'key=value' (one extra request per table, only when "
                "selected); NULL when the server has no tags API.",
            ),
            column("glossary_terms", STR_LIST, "Not tracked by Unity Catalog; always empty (shared vocabulary)."),
            column("domain_id", pa.string(), "Not tracked by Unity Catalog; always NULL (shared vocabulary)."),
            column("domain", pa.string(), "Not tracked by Unity Catalog; always NULL (shared vocabulary)."),
            column("container_id", pa.string(), "Parent schema full name, 'catalog.schema' (schemas.id)."),
            column("deprecated", pa.bool_(), "Always false: Unity Catalog has no deprecation flag.", nullable=False),
            column("custom_properties", STR_MAP, "Table properties (TBLPROPERTIES)."),
            column("column_count", pa.int32(), "Number of columns."),
            column("partition_columns", STR_LIST, "Partition columns in partition order."),
            column("uuid", pa.string(), "Table UUID."),
            column("metastore_id", pa.string(), "Metastore the table belongs to."),
            column("browse_only", pa.bool_(), "True when the principal can only browse it."),
            *_times(),
            column("url", pa.string(), "Catalog Explorer link (Databricks only)."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "datasets_scan"
        description = "Unity Catalog tables and views in the shared data-catalog vocabulary."
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def fanout(cls, client: UnityCatalogClient, pd: Pushdown) -> list[str] | None:
        """Direct gets for ``id`` / ``qualified_name`` values, else the schemas in scope."""
        return _table_scope(client, pd, "id")

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str], cfg: UnityCatalogConfig, wanted: set[str]) -> list[Listing]:
        """``GET /tables`` per schema; ``omit_columns`` / ``omit_properties`` unless those columns are selected."""
        return _table_listings(
            ids,
            cfg,
            omit_columns=not ({"column_count", "partition_columns"} & wanted),
            omit_properties="custom_properties" not in wanted,
        )

    @classmethod
    def rows(
        cls, client: UnityCatalogClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map tables to rows (fetching tags only when the tags column is selected)."""
        out = []
        for t in items:
            cat, sch, name = t.get("catalog_name"), t.get("schema_name"), t.get("name")
            if not cat or not sch or not name:
                continue
            fn = t.get("full_name") or full_name(cat, sch, name)
            cols = t.get("columns") or []
            parts = sorted(
                (c for c in cols if isinstance(c, dict) and c.get("partition_index") is not None),
                key=lambda c: int(c["partition_index"]),
            )
            tags = tag_labels(entity_tags(client, "tables", fn)) if "tags" in wanted else None
            owner = t.get("owner")
            out.append(
                {
                    **common(
                        client,
                        id_=fn,
                        qualified=fn,
                        description=t.get("comment"),
                        item=t,
                        ui=explore_path(cat, sch, name),
                    ),
                    **_who(t),
                    "database": cat,
                    "schema": sch,
                    "name": name,
                    "catalog_name": cat,
                    "schema_name": sch,
                    "subtype": lower(t.get("table_type")),
                    "table_type": t.get("table_type"),
                    "data_source_format": t.get("data_source_format"),
                    "storage_location": t.get("storage_location"),
                    "view_definition": t.get("view_definition"),
                    "owners": [owner] if owner else [],
                    "tags": tags,
                    "glossary_terms": [],
                    "container_id": full_name(cat, sch),
                    "deprecated": False,
                    "custom_properties": str_map(t.get("properties")),
                    "column_count": len(cols) if "columns" in t and cols else None,
                    "partition_columns": [str(c["name"]) for c in parts] if cols else None,
                    "uuid": t.get("table_id"),
                    "metastore_id": t.get("metastore_id"),
                    "browse_only": t.get("browse_only"),
                }
            )
        return out


class Views(ApiScan):
    """Rows for ``unity_catalog.main.views``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            catalog_tool_column(),
            column("id", pa.string(), "View full name, 'catalog.schema.view' (datasets.id).", nullable=False),
            column("catalog_name", pa.string(), "Catalog of the view.", nullable=False),
            column("schema_name", pa.string(), "Schema of the view.", nullable=False),
            column("name", pa.string(), "View name.", nullable=False),
            column("qualified_name", pa.string(), "Lower-cased 'catalog.schema.view'."),
            column("table_type", pa.string(), "VIEW, MATERIALIZED_VIEW, METRIC_VIEW or STREAMING_TABLE."),
            column("view_definition", pa.string(), "The view's SQL (YAML for metric views)."),
            column("view_dependencies", STR_LIST, "Full names of the tables and functions the view reads."),
            column("description", pa.string(), "View comment."),
            column("owner", pa.string(), "Owner principal."),
            column("created_at", TSTZ, "When the view was created."),
            column("updated_at", TSTZ, "When the view was last changed."),
            column("url", pa.string(), "Catalog Explorer link (Databricks only)."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "views_scan"
        description = "Unity Catalog views with their SQL definition."
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def fanout(cls, client: UnityCatalogClient, pd: Pushdown) -> list[str] | None:
        """Direct gets for ``id`` values, else the schemas in scope."""
        return _table_scope(client, pd, "id")

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str], cfg: UnityCatalogConfig, wanted: set[str]) -> list[Listing]:
        """``GET /tables`` per schema with ``omit_columns`` and ``omit_properties``."""
        return _table_listings(ids, cfg, omit_columns=True, omit_properties=True)

    @classmethod
    def rows(
        cls, client: UnityCatalogClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Keep the view-like table types."""
        out = []
        for t in items:
            if str(t.get("table_type") or "").upper() not in VIEW_TYPES:
                continue
            cat, sch, name = t.get("catalog_name"), t.get("schema_name"), t.get("name")
            fn = t.get("full_name") or full_name(cat, sch, name)
            out.append(
                {
                    "catalog_tool": "unity_catalog",
                    "id": fn,
                    "catalog_name": cat,
                    "schema_name": sch,
                    "name": name,
                    "qualified_name": lower(fn),
                    "table_type": t.get("table_type"),
                    "view_definition": t.get("view_definition"),
                    "view_dependencies": view_dependencies(t),
                    "description": t.get("comment") or None,
                    "owner": t.get("owner"),
                    "created_at": ts(t.get("created_at")),
                    "updated_at": ts(t.get("updated_at")),
                    "url": client.ui_url(explore_path(str(cat), str(sch), str(name))),
                }
            )
        return out


class Columns(ApiScan):
    """Rows for ``unity_catalog.main.columns``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            catalog_tool_column(),
            column("dataset_id", pa.string(), "Full name of the table (datasets.id).", nullable=False),
            column("platform", pa.string(), "'databricks' or 'unity_catalog'."),
            column("qualified_name", pa.string(), "Lower-cased 'catalog.schema.table' of the table."),
            column("catalog_name", pa.string(), "Catalog of the table.", nullable=False),
            column("schema_name", pa.string(), "Schema of the table.", nullable=False),
            column("table_name", pa.string(), "Short name of the table or view the column belongs to.", nullable=False),
            column("column_name", pa.string(), "Name of the column as declared in the table schema.", nullable=False),
            column("ordinal_position", pa.int32(), "1-based position in the table.", nullable=False),
            column("data_type", pa.string(), "Unity Catalog type name: INT, LONG, STRING, DECIMAL, STRUCT, ARRAY, ..."),
            column("native_data_type", pa.string(), "Full Spark SQL type text, e.g. 'decimal(18,2)', 'array<string>'."),
            column("type_json", pa.string(), "The Spark StructField JSON of the column (JSON text)."),
            column("type_precision", pa.int32(), "Decimal precision (0 when not applicable)."),
            column("type_scale", pa.int32(), "Decimal scale (0 when not applicable)."),
            column("type_interval_type", pa.string(), "Interval qualifier for INTERVAL columns."),
            column("nullable", pa.bool_(), "True if the column accepts NULL.", nullable=False),
            column(
                "is_primary_key",
                pa.bool_(),
                "True if the column is in the table's primary key constraint.",
                nullable=False,
            ),
            column(
                "partition_index",
                pa.int32(),
                "Partition order as a 0-based count (0 = first partition column); NULL for a column that does not "
                "partition the table.",
            ),
            column("description", pa.string(), "Column comment."),
            column(
                "tags",
                STR_LIST,
                "Databricks column tags as 'key' or 'key=value' (one extra request per column, only when selected); "
                "NULL when the server has no tags API.",
            ),
            column("glossary_terms", STR_LIST, "Not tracked by Unity Catalog; always empty (shared vocabulary)."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "columns_scan"
        description = "Columns of every Unity Catalog table and view."
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def fanout(cls, client: UnityCatalogClient, pd: Pushdown) -> list[str] | None:
        """Direct gets for ``dataset_id`` values, else the schemas in scope."""
        return _table_scope(client, pd, "dataset_id")

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str], cfg: UnityCatalogConfig, wanted: set[str]) -> list[Listing]:
        """``GET /tables`` per schema with columns, without properties."""
        return _table_listings(ids, cfg, omit_columns=False, omit_properties=True)

    @classmethod
    def rows(
        cls, client: UnityCatalogClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """One row per column."""
        out = []
        for t in items:
            cat, sch, name = t.get("catalog_name"), t.get("schema_name"), t.get("name")
            if not cat or not sch or not name:
                continue
            fn = t.get("full_name") or full_name(cat, sch, name)
            pk = primary_key_columns(t)
            for i, c in enumerate(t.get("columns") or []):
                if not isinstance(c, dict) or not c.get("name"):
                    continue
                pos = to_int(c.get("position"))
                tags = tag_labels(entity_tags(client, "columns", f"{fn}.{c['name']}")) if "tags" in wanted else None
                out.append(
                    {
                        "catalog_tool": "unity_catalog",
                        "dataset_id": fn,
                        "platform": "databricks" if client.config.is_databricks else "unity_catalog",
                        "qualified_name": lower(fn),
                        "catalog_name": cat,
                        "schema_name": sch,
                        "table_name": name,
                        "column_name": c["name"],
                        "ordinal_position": (pos if pos is not None else i) + 1,
                        "data_type": c.get("type_name"),
                        "native_data_type": c.get("type_text"),
                        "type_json": c.get("type_json"),
                        "type_precision": to_int(c.get("type_precision")),
                        "type_scale": to_int(c.get("type_scale")),
                        "type_interval_type": c.get("type_interval_type"),
                        "nullable": c.get("nullable") is not False,
                        "is_primary_key": c["name"] in pk,
                        "partition_index": to_int(c.get("partition_index")),
                        "description": c.get("comment") or None,
                        "tags": tags,
                        "glossary_terms": [],
                    }
                )
        return out


# ---------------------------------------------------------------------------- volumes / functions


def _schema_children_scope(client: UnityCatalogClient, pd: Pushdown) -> list[str]:
    ids = names_from_full(pd.strs("id"), 3)
    if ids is not None and pd.strs("catalog_name") is None:
        return sorted({".".join(i.split(".")[:2]) for i in ids})
    return schema_scope(client, pd, ("catalog_name",), ("schema_name",))


class Volumes(ApiScan):
    """Rows for ``unity_catalog.main.volumes``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            *_common_cols("Volume full name, 'catalog.schema.volume'."),
            column("catalog_name", pa.string(), "Catalog of the volume.", nullable=False),
            column("schema_name", pa.string(), "Schema of the volume.", nullable=False),
            column("name", pa.string(), "Volume name.", nullable=False),
            column("qualified_name", pa.string(), "Lower-cased 'catalog.schema.volume'."),
            column("description", pa.string(), "Volume comment."),
            column("volume_type", pa.string(), "MANAGED or EXTERNAL."),
            column("storage_location", pa.string(), "Storage URL of the volume's files."),
            column("owner", pa.string(), "Owner principal."),
            column("uuid", pa.string(), "Volume UUID."),
            column("metastore_id", pa.string(), "Metastore the volume belongs to."),
            column("browse_only", pa.bool_(), "True when the principal can only browse it."),
            *_times(),
            column("url", pa.string(), "Catalog Explorer link (Databricks only)."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "volumes_scan"
        description = "Unity Catalog volumes (governed file storage)."
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def fanout(cls, client: UnityCatalogClient, pd: Pushdown) -> list[str] | None:
        """Schemas in scope (``catalog_name`` / ``schema_name`` / ``id`` filters)."""
        return _schema_children_scope(client, pd)

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str], cfg: UnityCatalogConfig, wanted: set[str]) -> list[Listing]:
        """``GET /volumes?catalog_name=&schema_name=`` per schema."""
        out = []
        for scope in ids:
            cat, sch = split_scope(scope)
            q: tuple[tuple[str, Any], ...] = (("catalog_name", cat), ("schema_name", sch))
            if cfg.include_browse:
                q += (("include_browse", True),)
            out.append(Listing(f"{UC_API}/volumes", "volumes", q, missing_ok=True))
        return out

    @classmethod
    def rows(
        cls, client: UnityCatalogClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map volumes to rows."""
        out = []
        for v in items:
            cat, sch, name = v.get("catalog_name"), v.get("schema_name"), v.get("name")
            if not cat or not sch or not name:
                continue
            fn = v.get("full_name") or full_name(cat, sch, name)
            out.append(
                {
                    **common(
                        client,
                        id_=fn,
                        qualified=fn,
                        description=v.get("comment"),
                        item=v,
                        ui=explore_path("volumes", cat, sch, name),
                    ),
                    **_who(v),
                    "catalog_name": cat,
                    "schema_name": sch,
                    "name": name,
                    "volume_type": v.get("volume_type"),
                    "storage_location": v.get("storage_location"),
                    "uuid": v.get("volume_id"),
                    "metastore_id": v.get("metastore_id"),
                    "browse_only": v.get("browse_only"),
                }
            )
        return out


PARAM_STRUCT = pa.struct(
    [
        pa.field("name", pa.string()),
        pa.field("type_text", pa.string()),
        pa.field("type_name", pa.string()),
        pa.field("position", pa.int32()),
        pa.field("parameter_mode", pa.string()),
        pa.field("parameter_default", pa.string()),
        pa.field("comment", pa.string()),
    ]
)


def _params(value: Any) -> list[dict[str, Any]] | None:
    if not isinstance(value, dict):
        return None
    out = []
    for p in value.get("parameters") or []:
        if isinstance(p, dict):
            out.append(
                {
                    "name": p.get("name"),
                    "type_text": p.get("type_text"),
                    "type_name": p.get("type_name"),
                    "position": to_int(p.get("position")),
                    "parameter_mode": p.get("parameter_mode"),
                    "parameter_default": p.get("parameter_default"),
                    "comment": p.get("comment"),
                }
            )
    return out


class Functions(ApiScan):
    """Rows for ``unity_catalog.main.functions``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            *_common_cols("Function full name, 'catalog.schema.function'."),
            column("catalog_name", pa.string(), "Catalog of the function.", nullable=False),
            column("schema_name", pa.string(), "Schema of the function.", nullable=False),
            column("name", pa.string(), "Function name.", nullable=False),
            column("qualified_name", pa.string(), "Lower-cased 'catalog.schema.function'."),
            column("description", pa.string(), "Function comment."),
            column("data_type", pa.string(), "Return type name; TABLE_TYPE for table functions."),
            column("full_data_type", pa.string(), "Full return type text, e.g. 'DECIMAL(18,2)'."),
            column(
                "parameters",
                pa.list_(PARAM_STRUCT),
                "Input parameters: STRUCT(name, type_text, type_name, position, parameter_mode, parameter_default, "
                "comment)[].",
            ),
            column("return_columns", pa.list_(PARAM_STRUCT), "Result columns of a table function (same shape)."),
            column("routine_body", pa.string(), "SQL or EXTERNAL."),
            column("routine_definition", pa.string(), "Function body (SQL expression or external code)."),
            column("external_language", pa.string(), "Language of an EXTERNAL function, e.g. 'python'."),
            column("is_deterministic", pa.bool_(), "Whether the function is deterministic."),
            column("is_null_call", pa.bool_(), "True when the function returns NULL for any NULL argument."),
            column("sql_data_access", pa.string(), "CONTAINS_SQL, READS_SQL_DATA or NO_SQL."),
            column("security_type", pa.string(), "DEFINER."),
            column("specific_name", pa.string(), "Specific name of the function."),
            column("routine_dependencies", STR_LIST, "Full names of the tables and functions the body uses."),
            column("owner", pa.string(), "Owner principal."),
            column("uuid", pa.string(), "Function UUID."),
            *_times(),
            column("url", pa.string(), "Catalog Explorer link (Databricks only)."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "functions_scan"
        description = "Unity Catalog user-defined functions (SQL and Python UDFs)."
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def fanout(cls, client: UnityCatalogClient, pd: Pushdown) -> list[str] | None:
        """Schemas in scope (``catalog_name`` / ``schema_name`` / ``id`` filters)."""
        return _schema_children_scope(client, pd)

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str], cfg: UnityCatalogConfig, wanted: set[str]) -> list[Listing]:
        """``GET /functions?catalog_name=&schema_name=`` per schema."""
        out = []
        for scope in ids:
            cat, sch = split_scope(scope)
            q: tuple[tuple[str, Any], ...] = (("catalog_name", cat), ("schema_name", sch))
            if cfg.include_browse:
                q += (("include_browse", True),)
            out.append(Listing(f"{UC_API}/functions", "functions", q, missing_ok=True))
        return out

    @classmethod
    def rows(
        cls, client: UnityCatalogClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map functions to rows."""
        out = []
        for f in items:
            cat, sch, name = f.get("catalog_name"), f.get("schema_name"), f.get("name")
            if not cat or not sch or not name:
                continue
            fn = f.get("full_name") or full_name(cat, sch, name)
            out.append(
                {
                    **common(
                        client,
                        id_=fn,
                        qualified=fn,
                        description=f.get("comment"),
                        item=f,
                        ui=explore_path("functions", cat, sch, name),
                    ),
                    **_who(f),
                    "catalog_name": cat,
                    "schema_name": sch,
                    "name": name,
                    "data_type": f.get("data_type"),
                    "full_data_type": f.get("full_data_type"),
                    "parameters": _params(f.get("input_params")),
                    "return_columns": _params(f.get("return_params")),
                    "routine_body": f.get("routine_body"),
                    "routine_definition": f.get("routine_definition"),
                    "external_language": f.get("external_language"),
                    "is_deterministic": f.get("is_deterministic"),
                    "is_null_call": f.get("is_null_call"),
                    "sql_data_access": f.get("sql_data_access"),
                    "security_type": f.get("security_type"),
                    "specific_name": f.get("specific_name"),
                    "routine_dependencies": view_dependencies({"view_dependencies": f.get("routine_dependencies")}),
                    "uuid": f.get("function_id"),
                }
            )
        return out


# ---------------------------------------------------------------------------- models


def _model_listing(pd: Pushdown, cfg: UnityCatalogConfig) -> Listing:
    """``GET /models`` with ``catalog_name`` / ``schema_name`` when exactly one of each is pushed down."""
    q: list[tuple[str, Any]] = []
    cat = pd.one("catalog_name")
    if cat is not None:
        q.append(("catalog_name", str(cat)))
        sch = pd.one("schema_name")
        if sch is not None:
            q.append(("schema_name", str(sch)))
    if cfg.include_browse:
        q.append(("include_browse", True))
    return Listing(f"{UC_API}/models", "registered_models", tuple(q), missing_ok=True)


class RegisteredModels(ApiScan):
    """Rows for ``unity_catalog.main.registered_models``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            *_common_cols("Model full name, 'catalog.schema.model'."),
            column("catalog_name", pa.string(), "Catalog of the model.", nullable=False),
            column("schema_name", pa.string(), "Schema of the model.", nullable=False),
            column("name", pa.string(), "Model name.", nullable=False),
            column("qualified_name", pa.string(), "Lower-cased 'catalog.schema.model'."),
            column("description", pa.string(), "Model comment."),
            column("storage_location", pa.string(), "Storage URL of the model's artifacts."),
            column("owner", pa.string(), "Owner principal."),
            column("uuid", pa.string(), "Model UUID (open source) when reported."),
            column("metastore_id", pa.string(), "Metastore the model belongs to."),
            column("browse_only", pa.bool_(), "True when the principal can only browse it."),
            *_times(),
            column("url", pa.string(), "Catalog Explorer link (Databricks only)."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "registered_models_scan"
        description = "MLflow models registered in Unity Catalog."
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str], cfg: UnityCatalogConfig, wanted: set[str]) -> list[Listing]:
        """``id`` = / IN -> ``GET /models/{full_name}`` each; else ``GET /models`` (catalog / schema pushed)."""
        direct = names_from_full(pd.strs("id"), 3)
        if direct is not None:
            return [Listing(f"{UC_API}/models/{quote(n)}", None, paged=False, missing_ok=True) for n in direct]
        return [_model_listing(pd, cfg)]

    @classmethod
    def rows(
        cls, client: UnityCatalogClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map models to rows."""
        out = []
        for m in items:
            cat, sch, name = m.get("catalog_name"), m.get("schema_name"), m.get("name")
            if not cat or not sch or not name:
                continue
            fn = m.get("full_name") or full_name(cat, sch, name)
            out.append(
                {
                    **common(
                        client,
                        id_=fn,
                        qualified=fn,
                        description=m.get("comment"),
                        item=m,
                        ui=explore_path("models", cat, sch, name),
                    ),
                    **_who(m),
                    "catalog_name": cat,
                    "schema_name": sch,
                    "name": name,
                    "storage_location": m.get("storage_location"),
                    "uuid": m.get("id"),
                    "metastore_id": m.get("metastore_id"),
                    "browse_only": m.get("browse_only"),
                }
            )
        return out


class ModelVersions(ApiScan):
    """Rows for ``unity_catalog.main.model_versions``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            catalog_tool_column(),
            column("model_id", pa.string(), "Full name of the model (registered_models.id).", nullable=False),
            column("catalog_name", pa.string(), "Catalog of the model.", nullable=False),
            column("schema_name", pa.string(), "Schema of the model.", nullable=False),
            column(
                "model_name", pa.string(), "Short name of the registered model the version belongs to.", nullable=False
            ),
            column("version", pa.int64(), "Version number (1, 2, ...).", nullable=False),
            column("status", pa.string(), "READY, PENDING_REGISTRATION or FAILED_REGISTRATION."),
            column("source", pa.string(), "URI of the source artifacts (e.g. an MLflow run's artifact path)."),
            column("run_id", pa.string(), "MLflow run that produced the version."),
            column("storage_location", pa.string(), "Storage URL of the version's artifacts."),
            column("description", pa.string(), "Version comment."),
            column("uuid", pa.string(), "Model version UUID (open source) when reported."),
            *_times(),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "model_versions_scan"
        description = "Versions of the registered models."
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def fanout(cls, client: UnityCatalogClient, pd: Pushdown) -> list[str] | None:
        """Models in scope: ``model_id`` = / IN values, else every model (catalog / schema pushed to the list)."""
        direct = names_from_full(pd.strs("model_id"), 3)
        if direct is not None:
            return direct
        listing = _model_listing(pd, client.config)
        return sorted(
            str(m.get("full_name") or full_name(m.get("catalog_name"), m.get("schema_name"), m.get("name")))
            for m in drain(client, listing)
            if m.get("name")
        )

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str], cfg: UnityCatalogConfig, wanted: set[str]) -> list[Listing]:
        """``GET /models/{full_name}/versions`` per model."""
        return [
            Listing(
                f"{UC_API}/models/{quote(m)}/versions", "model_versions", missing_ok=True, context=(("model_id", m),)
            )
            for m in ids
        ]

    @classmethod
    def rows(
        cls, client: UnityCatalogClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map versions to rows."""
        out = []
        model = str(listing.ctx("model_id"))
        cat, sch, name = [*model.split("."), None, None, None][:3]
        for v in items:
            version = to_int(v.get("version"))
            if version is None:
                continue
            out.append(
                {
                    "catalog_tool": "unity_catalog",
                    "model_id": model,
                    "catalog_name": v.get("catalog_name") or cat,
                    "schema_name": v.get("schema_name") or sch,
                    "model_name": v.get("model_name") or name,
                    "version": version,
                    "status": v.get("status"),
                    "source": v.get("source"),
                    "run_id": v.get("run_id"),
                    "storage_location": v.get("storage_location"),
                    "description": v.get("comment") or None,
                    "uuid": v.get("id"),
                    "created_at": ts(v.get("created_at")),
                    "created_by": v.get("created_by"),
                    "updated_at": ts(v.get("updated_at")),
                    "updated_by": v.get("updated_by"),
                }
            )
        return out


__all__ = [
    "Catalogs",
    "Columns",
    "Datasets",
    "Functions",
    "Metastores",
    "ModelVersions",
    "RegisteredModels",
    "Schemas",
    "Views",
    "Volumes",
    "entity_tags",
    "tag_labels",
]
