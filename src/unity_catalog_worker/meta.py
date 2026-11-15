"""Discovery metadata: catalog, schema, tables.

DuckDB, coding agents and the ``vgi-lint`` metadata checker all read these
tags. Every table carries ``object_tags(...)``: a title, a category from the
schema's category list, keywords, and LLM- and human-facing docs.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "unity_catalog"

SOURCE_URL = "https://github.com/Query-farm/vgi-unity-catalog"
UC_API_DOCS = "https://docs.unitycatalog.io/"

SCHEMA_CATEGORIES = [
    {
        "name": "assets",
        "title": "Data assets",
        "description": "Metastore, catalogs, schemas, tables, views, columns, volumes, functions and models.",
    },
    {
        "name": "governance",
        "title": "Governance",
        "description": "Grants, owners, tags, storage, connections and Delta Sharing.",
    },
    {"name": "lineage", "title": "Lineage", "description": "Table and column lineage captured by Databricks."},
]


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def object_tags(*, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    """Build the standard per-object discovery tags."""
    if category not in {c["name"] for c in SCHEMA_CATEGORIES}:
        raise ValueError(f"unknown category {category!r}; add it to SCHEMA_CATEGORIES")
    return {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


def catalog_tool_column() -> pa.Field[Any]:
    """Return the shared data-catalog vocabulary's ``catalog_tool`` column (always 'unity_catalog' here)."""
    return column(
        "catalog_tool",
        pa.string(),
        "Which catalog tool the row comes from: always 'unity_catalog' (vgi-datahub says 'datahub', "
        "vgi-openmetadata 'openmetadata'), for UNION ALL across catalogs.",
        nullable=False,
    )


CREDENTIALS_MD = (
    "Connect with a DuckDB secret of type `unity_catalog`, created **after** `ATTACH`:\n\n"
    "```sql\n"
    "-- open-source Unity Catalog server (no authorization, or a bearer TOKEN)\n"
    "CREATE SECRET uc (TYPE unity_catalog, HOST 'http://localhost:8080');\n"
    "-- Databricks: personal access token\n"
    "CREATE SECRET uc (TYPE unity_catalog, HOST 'https://dbc-a1b2c3d4-e5f6.cloud.databricks.com', TOKEN 'dapi...');\n"
    "-- Databricks: OAuth machine-to-machine (service principal)\n"
    "CREATE SECRET uc (TYPE unity_catalog, HOST 'https://adb-123.7.azuredatabricks.net',\n"
    "                  CLIENT_ID '...', CLIENT_SECRET '...');\n"
    "```\n\n"
    "The fields match the `databricks` secret of vgi-databricks."
)

CATALOG_COMMENT = (
    "Unity Catalog metadata (open-source Unity Catalog and Databricks) as SQL tables: catalogs, schemas, tables "
    "and columns, volumes, functions, models, grants, owners, tags and lineage. Read-only."
)

CATALOG_DOC_LLM = (
    "Read the metadata of a Unity Catalog metastore through its REST API (/api/2.1/unity-catalog): either an "
    "open-source Unity Catalog server or a Databricks workspace. It never reads table data. Requires a DuckDB "
    "secret created after ATTACH: CREATE SECRET (TYPE unity_catalog, HOST 'http://localhost:8080') for open "
    "source, or HOST 'https://<workspace>' with TOKEN 'dapi...' (or CLIENT_ID/CLIENT_SECRET, AZURE_TOKEN) for "
    "Databricks. Everything is in schema `main`. Objects are named by their full name 'catalog.schema.object' "
    "(the id column); qualified_name is the lower-cased full name and joins to dbt, OpenLineage, Looker and the "
    "DataHub / OpenMetadata workers, whose datasets / columns / owners / tags / lineage_edges tables share column "
    "names. Filters on catalog_name and schema_name (also database / schema in datasets), and = / IN on id, are "
    "sent to the API so only those schemas are listed; always add them on large metastores. Connections, Delta "
    "Sharing, effective grants, tags and lineage exist only on Databricks: on open source those tables raise "
    "'not implemented by this server'."
)

CATALOG_DOC_MD = (
    "# Unity Catalog\n\n"
    "Query the metadata of a Unity Catalog metastore with SQL, whether it is an open-source "
    "[Unity Catalog](https://www.unitycatalog.io) server or a Databricks workspace: catalogs, schemas, tables "
    "and their columns, views and their SQL, volumes, functions, registered models, external locations, "
    "storage credentials (by name), grants and owners, and, on Databricks, connections, Delta Sharing, tags "
    "and lineage. Read-only.\n\n"
    f"{CREDENTIALS_MD}\n\n"
    "Tables use the same columns as the DataHub and OpenMetadata workers (`datasets`, `columns`, `owners`, "
    "`tags`, `lineage_edges`), so catalogs can be compared with `UNION ALL`."
)

SCHEMA_DOC_LLM = (
    "Schema main: one table per Unity Catalog collection. Hierarchy: metastore > catalog > schema > "
    "table / view / volume / function / registered model; registered models have versions. Ids are full "
    "names ('catalog', 'catalog.schema', 'catalog.schema.table'); columns.dataset_id = datasets.id, "
    "datasets.container_id = schemas.id, model_versions.model_id = registered_models.id. grants lists explicit "
    "privileges per securable (privilege spelled USE_CATALOG, SELECT, ...); effective_grants adds inherited ones "
    "(Databricks). owners lists the one owner of every object. Push catalog_name / schema_name / id filters down "
    "to keep scans small; grants, owners, tag_assignments and lineage_edges call the API once per object in "
    "scope. Databricks-only tables (connections, shares, recipients, effective_grants, tags, tag_assignments, "
    "lineage_edges) error on open-source servers."
)

SCHEMA_DOC_MD = (
    "## main\n\n"
    "Unity Catalog securables, permissions and lineage as tables: a metastore holds catalogs, a catalog "
    "holds schemas, and a schema holds tables, views, volumes, functions and registered models.\n\n"
    "Filter by `catalog_name` and `schema_name` whenever you can: those predicates decide which schemas the "
    "worker lists. `grants`, `owners`, `tag_assignments` and `lineage_edges` call the API once per object in "
    "scope.\n\n"
    "Secrets stay in Unity Catalog: storage credentials are listed by name and kind, connection options are "
    "masked and Delta Sharing recipient tokens are never read."
)

# name -> comment, title, category, keywords, doc_llm, doc_md
TABLE_DOCS: dict[str, dict[str, str]] = {
    "metastores": {
        "comment": "The Unity Catalog metastore this server or workspace serves.",
        "title": "Unity Catalog Metastore",
        "category": "assets",
        "keywords": "metastore, metastore id, region, cloud, storage root, delta sharing scope",
        "doc_llm": (
            "One row: the metastore behind the server (GET /metastore_summary). metastore_id is set everywhere; "
            "name, cloud, region, owner, storage_root, delta_sharing_scope and privilege_model_version are filled "
            "on Databricks and NULL on open source."
        ),
        "doc_md": "The metastore the server or workspace is attached to.",
    },
    "catalogs": {
        "comment": "Unity Catalog catalogs with type, owner, properties and storage root.",
        "title": "Unity Catalog Catalogs",
        "category": "assets",
        "keywords": "catalogs, catalog type, foreign catalog, shared catalog, owner, properties",
        "doc_llm": (
            "One row per catalog. id = name. catalog_type (Databricks) is MANAGED_CATALOG, FOREIGN_CATALOG "
            "(Lakehouse Federation, see connection_name), DELTASHARING_CATALOG (see provider_name / share_name) or "
            "SYSTEM_CATALOG. properties is a `MAP(VARCHAR, VARCHAR)`. Join metastores ON metastore_id. Pushed down: "
            "name / id = or IN (GET /catalogs/{name})."
        ),
        "doc_md": "Every catalog with its type, owner and properties.",
    },
    "schemas": {
        "comment": "Unity Catalog schemas with owner, properties and storage root.",
        "title": "Unity Catalog Schemas",
        "category": "assets",
        "keywords": "schemas, databases, namespaces, owner, properties",
        "doc_llm": (
            "One row per schema; id = 'catalog.schema'. Join catalogs ON catalog_name = catalogs.id and metastores ON "
            "metastore_id. Pushed down: catalog_name = / IN (GET /schemas?catalog_name="
            "), and the catalog of id values."
        ),
        "doc_md": "Every schema with its catalog, owner and properties.",
    },
    "datasets": {
        "comment": "Unity Catalog tables and views in the shared data-catalog vocabulary.",
        "title": "Tables and Views",
        "category": "assets",
        "keywords": "tables, views, datasets, table type, format, storage location, delta, iceberg, owner",
        "doc_llm": (
            "One row per table or view; id = 'catalog.schema.table', qualified_name its lower-cased form (join key "
            "to dbt / OpenLineage / Looker and the DataHub / OpenMetadata workers). database = catalog_name, "
            "schema = schema_name. table_type MANAGED, EXTERNAL, VIEW, MATERIALIZED_VIEW, STREAMING_TABLE, FOREIGN; "
            "subtype is its lower case. data_source_format DELTA, ICEBERG, PARQUET, ... custom_properties = "
            "TBLPROPERTIES. tags (Databricks) costs one request per table and is only fetched when selected. "
            "Pushed down: id / qualified_name = or IN (GET /tables/{full_name}), catalog_name or database, "
            "schema_name or schema (= / IN; only those schemas are listed). Columns are omitted from the API "
            "response (omit_columns) unless column_count / partition_columns are selected."
        ),
        "doc_md": (
            "Every table and view with type, format, storage location and owner. Filter by `catalog_name` and "
            "`schema_name` so only those schemas are listed."
        ),
    },
    "views": {
        "comment": "Unity Catalog views with their SQL definition and dependencies.",
        "title": "Unity Catalog Views",
        "category": "assets",
        "keywords": "views, view definition, sql, materialized views, metric views, dependencies",
        "doc_llm": (
            "One row per view-like table (VIEW, MATERIALIZED_VIEW, METRIC_VIEW, STREAMING_TABLE) with "
            "view_definition (SQL; YAML for metric views) and view_dependencies (full names it reads, when "
            "reported). id = datasets.id. Pushed down like datasets: id, catalog_name, schema_name."
        ),
        "doc_md": "The SQL behind every view.",
    },
    "columns": {
        "comment": "Columns of every Unity Catalog table and view, with types and partitioning.",
        "title": "Unity Catalog Columns",
        "category": "assets",
        "keywords": "columns, data types, nullable, partition columns, primary key, column comments",
        "doc_llm": (
            "One row per column. dataset_id = datasets.id. ordinal_position is 1-based. data_type is the Unity "
            "Catalog type name ('INT', 'LONG', 'STRING', ...), native_data_type the full Spark SQL type "
            "('decimal(18,2)', 'array<string>'), type_json the StructField JSON. partition_index is 0-based among "
            "partition columns, NULL otherwise. is_primary_key comes from Databricks informational constraints. "
            "tags (Databricks) costs one request per column, only when selected. Pushed down: dataset_id / "
            "qualified_name (GET /tables/{full_name}), catalog_name, schema_name."
        ),
        "doc_md": "Every column with its type, nullability, comment and partition position.",
    },
    "volumes": {
        "comment": "Unity Catalog volumes: governed directories of files.",
        "title": "Unity Catalog Volumes",
        "category": "assets",
        "keywords": "volumes, files, managed volume, external volume, storage location",
        "doc_llm": (
            "One row per volume; id = 'catalog.schema.volume'. volume_type MANAGED or EXTERNAL; storage_location "
            "is the backing URL. Join metastores ON metastore_id. Files are addressed as "
            "/Volumes/<catalog>/<schema>/<volume>/... Pushed down: "
            "catalog_name, schema_name (= / IN), the schema of id values."
        ),
        "doc_md": "Every volume with its type and storage location.",
    },
    "functions": {
        "comment": "Unity Catalog user-defined functions with parameters and bodies.",
        "title": "Unity Catalog Functions",
        "category": "assets",
        "keywords": "functions, udf, sql functions, python udf, parameters, routine definition",
        "doc_llm": (
            "One row per function; id = 'catalog.schema.function'. parameters is a `STRUCT(name, type_text, "
            "type_name, position, parameter_mode, parameter_default, comment)[]`; return_columns the result columns "
            "of a table function (data_type TABLE_TYPE). routine_body SQL or EXTERNAL (see external_language), "
            "routine_definition the body. Pushed down: catalog_name, schema_name, the schema of id values."
        ),
        "doc_md": "Every SQL and Python UDF with its signature and body.",
    },
    "registered_models": {
        "comment": "MLflow models registered in Unity Catalog.",
        "title": "Unity Catalog Registered Models",
        "category": "assets",
        "keywords": "models, mlflow, registered models, machine learning",
        "doc_llm": (
            "One row per registered model; id = 'catalog.schema.model'. Join model_versions ON model_versions.model_id "
            "= id and metastores ON metastore_id. Pushed down: id = / IN (GET "
            "/models/{full_name}), a single catalog_name and schema_name (GET /models?catalog_name=&schema_name=)."
        ),
        "doc_md": "Every registered MLflow model.",
    },
    "model_versions": {
        "comment": "Versions of the models registered in Unity Catalog.",
        "title": "Unity Catalog Model Versions",
        "category": "assets",
        "keywords": "model versions, mlflow, run id, model status",
        "doc_llm": (
            "One row per model version; model_id = registered_models.id, version 1, 2, ... status READY, "
            "PENDING_REGISTRATION or FAILED_REGISTRATION; source and run_id point to the MLflow run. Pushed down: "
            "model_id = / IN (one GET /models/{full_name}/versions each), catalog_name / schema_name (model list)."
        ),
        "doc_md": "Every version of every registered model.",
    },
    "external_locations": {
        "comment": "External locations: governed cloud storage paths.",
        "title": "Unity Catalog External Locations",
        "category": "governance",
        "keywords": "external locations, storage, s3, adls, gcs, storage credential",
        "doc_llm": (
            "One row per external location: name, url (s3:// / abfss:// / gs://), credential_name "
            "(storage_credentials.name), read_only, owner."
        ),
        "doc_md": "Cloud storage paths governed by Unity Catalog and the credential that reaches each.",
    },
    "storage_credentials": {
        "comment": "Storage credentials by name and kind; secret material is never read.",
        "title": "Cloud Storage Credentials",
        "category": "governance",
        "keywords": "storage credentials, iam role, managed identity, service principal, cloud credentials",
        "doc_llm": (
            "One row per storage credential: name, credential_type (aws_iam_role, azure_managed_identity, "
            "azure_service_principal, databricks_gcp_service_account, cloudflare_api_token), read_only, owner. "
            "Role ARNs, client secrets and keys are never returned. Reads /storage-credentials (Databricks) or "
            "/credentials (open source)."
        ),
        "doc_md": "Storage credentials by name and kind only.",
    },
    "connections": {
        "comment": "Lakehouse Federation connections (Databricks), options masked.",
        "title": "Unity Catalog Connections",
        "category": "governance",
        "keywords": "connections, lakehouse federation, foreign catalogs, mysql, postgres, snowflake",
        "doc_llm": (
            "Databricks only. One row per Lakehouse Federation connection: connection_type (MYSQL, POSTGRESQL, "
            "SNOWFLAKE, ...), credential_type, options as a `MAP(VARCHAR, VARCHAR)` in which every value except "
            "host, port, database, "
            "warehouse, httpPath and similar settings is '****'. Join catalogs.connection_name = connections.name."
        ),
        "doc_md": "Federated database connections, with secrets masked.",
    },
    "shares": {
        "comment": "Delta Sharing shares (Databricks), names only.",
        "title": "Delta Sharing Shares",
        "category": "governance",
        "keywords": "delta sharing, shares, data sharing",
        "doc_llm": "Databricks only. One row per Delta Sharing share: name, comment, owner, created/updated times.",
        "doc_md": "Delta Sharing shares this metastore offers.",
    },
    "recipients": {
        "comment": "Delta Sharing recipients (Databricks); tokens are never read.",
        "title": "Delta Sharing Recipients",
        "category": "governance",
        "keywords": "delta sharing, recipients, data sharing, consumers",
        "doc_llm": (
            "Databricks only. One row per Delta Sharing recipient: name, authentication_type (DATABRICKS or "
            "TOKEN), activated, cloud / region of a Databricks recipient. Tokens, activation links and sharing "
            "codes are never returned."
        ),
        "doc_md": "Who the metastore shares data with.",
    },
    "grants": {
        "comment": "Explicit privilege grants on every Unity Catalog securable.",
        "title": "Unity Catalog Grants",
        "category": "governance",
        "keywords": "grants, permissions, privileges, access control, who can read, principals",
        "doc_llm": (
            "One row per (securable, principal, privilege) granted directly (GET /permissions/{type}/{name}). "
            "securable_type metastore, catalog, schema, table, volume, function, registered_model, "
            "external_location, storage_credential; securable_name its full name. privilege uses the Databricks "
            "spelling with underscores (USE_CATALOG, USE_SCHEMA, SELECT, MODIFY, EXECUTE, READ_VOLUME, ...) on "
            "both servers. Inherited privileges are not included: see effective_grants (Databricks). One API call "
            "per securable in scope: push securable_type, securable_name, catalog_name and principal down."
        ),
        "doc_md": "Who was granted what, on every securable. Filter by `securable_type` and `catalog_name`.",
    },
    "effective_grants": {
        "comment": "Effective (direct and inherited) privileges per securable (Databricks).",
        "title": "Unity Catalog Effective Grants",
        "category": "governance",
        "keywords": "effective permissions, inherited privileges, access review, grants",
        "doc_llm": (
            "Databricks only. Like grants, plus privileges inherited from parent securables: inherited_from_type "
            "and inherited_from_name say where a privilege comes from (NULL when granted directly). Same "
            "pushdown as grants."
        ),
        "doc_md": "What each principal can effectively do, including inherited privileges.",
    },
    "owners": {
        "comment": "The owner of every Unity Catalog object, in the shared data-catalog vocabulary.",
        "title": "Unity Catalog Owners",
        "category": "governance",
        "keywords": "owners, ownership, stewards, accountability",
        "doc_llm": (
            "One row per owned object: entity_type metastore, catalog, schema, dataset (table or view), volume, "
            "function, registered_model, external_location, storage_credential, connection, share, recipient; "
            "entity_id its full name (join to that table's id). owner_type is user (email), service_principal "
            "(an application id) or group, inferred from the name. Pushed down: entity_type = / IN decides which "
            "collections are listed. Databricks-only collections are skipped on open source."
        ),
        "doc_md": "Who owns what, across every kind of object.",
    },
    "tags": {
        "comment": "Governed tag definitions (Databricks tag policies).",
        "title": "Unity Catalog Governed Tags",
        "category": "governance",
        "keywords": "tags, governed tags, tag policies, classification",
        "doc_llm": (
            "Databricks only. One row per governed tag (tag policy): id = name = tag key, description, "
            "allowed_values. Assignments are in tag_assignments."
        ),
        "doc_md": "The governed tags defined in the account.",
    },
    "tag_assignments": {
        "comment": "Tags on catalogs, schemas, tables, columns and volumes (Databricks).",
        "title": "Unity Catalog Tag Assignments",
        "category": "governance",
        "keywords": "tag assignments, tagged columns, pii, classification, tags",
        "doc_llm": (
            "Databricks only. One row per tag on an object: entity_type catalog, schema, table, column, volume; "
            "entity_id the full name (a column's table for columns, with column_name); tag_key, tag_value. One "
            "API call per object in scope: push entity_type, catalog_name and entity_id down."
        ),
        "doc_md": "Which objects and columns carry which tags. Filter by `catalog_name` and `entity_type`.",
    },
    "lineage_edges": {
        "comment": "Table and column lineage captured by Databricks, in the shared vocabulary.",
        "title": "Unity Catalog Lineage",
        "category": "lineage",
        "keywords": "lineage, data lineage, upstream, downstream, impact analysis, column lineage",
        "doc_llm": (
            "Databricks only (Data Lineage API). Direct edges: level 'dataset' (table to table, or between a "
            "table and the notebook / job / query / dashboard / pipeline that reads or writes it, as "
            "'<workspace_id>/<id>') and 'column' (column to column). via lists the notebooks / jobs on a "
            "table-to-table edge. Pushed down: downstream_id / downstream_qualified_name (that table's "
            "upstreams), upstream_id / upstream_qualified_name (that table's downstreams), level. Without them "
            "every table in the metastore is asked, one call per table plus one per column of tables with "
            "lineage. System-table lineage (system.access.table_lineage) is out of scope."
        ),
        "doc_md": (
            "What feeds a table and what it feeds. Filter by `downstream_id` or `upstream_id`; an unfiltered scan "
            "asks the API about every table."
        ),
    },
}

AGENT_TEST_TASKS = [
    {
        "name": "external_tables_in_sales",
        "prompt": "Using the unity_catalog worker, list the full names of the external tables in the sales catalog.",
    },
    {
        "name": "partition_columns_of_orders",
        "prompt": "Which column(s) partition the table sales.raw.orders, and what is their data type?",
    },
    {"name": "who_can_select_orders", "prompt": "Which principals have been granted SELECT on sales.raw.orders?"},
    {"name": "model_versions_ready", "prompt": "How many versions of the ml.models.churn model are in READY status?"},
    {
        "name": "catalog_team_property",
        "prompt": "Which Unity Catalog catalog has a 'team' property, and what is its value?",
    },
    {"name": "schemas_per_catalog", "prompt": "How many schemas does each catalog have?"},
    {
        "name": "view_reading_orders",
        "prompt": "Which views read from the table sales.raw.orders? Give their full names.",
    },
    {
        "name": "external_volume_locations",
        "prompt": "Where do the files of each external volume live? Give the volume's full name and storage location.",
    },
    {"name": "python_udfs", "prompt": "List the full names of the Python user-defined functions."},
    {
        "name": "models_in_ml_catalog",
        "prompt": "Which registered models are in the ml catalog, and what is each one's comment?",
    },
    {
        "name": "raw_zone_credential_kind",
        "prompt": (
            "What kind of cloud identity does the storage credential behind the external location for "
            "s3://acme-lake/raw use?"
        ),
    },
    {"name": "objects_owned_per_type", "prompt": "How many objects of each type does the principal 'admin' own?"},
    {"name": "metastore_id", "prompt": "What is the id of the Unity Catalog metastore?"},
    {"name": "owner_of_quoted_name", "prompt": "Who owns the table written in SQL as `Sales`.`RAW`.`Orders`?"},
    {
        "name": "privilege_spelling",
        "prompt": "Who holds the privilege written in a GRANT statement as 'use schema', and on which schema?",
    },
    {
        "name": "federated_postgres_host",
        "prompt": "Databricks: what host does the PostgreSQL Lakehouse Federation connection point at?",
    },
    {"name": "delta_shares", "prompt": "Databricks: list the Delta Sharing shares and their owners."},
    {"name": "inactive_recipients", "prompt": "Databricks: which Delta Sharing recipients have not activated yet?"},
    {
        "name": "inherited_privileges_on_orders",
        "prompt": "Databricks: which privileges on sales.raw.orders are inherited, by whom, and from where?",
    },
    {"name": "pii_allowed_values", "prompt": "Databricks: which values does the governed tag pii allow?"},
    {"name": "pii_columns_customers", "prompt": "Databricks: which columns of sales.raw.customers are tagged pii?"},
    {
        "name": "daily_revenue_upstream_columns",
        "prompt": "Databricks: which upstream columns feed the revenue column of sales.marts.daily_revenue?",
    },
]

CATALOG_TAGS = {
    "vgi.title": "Unity Catalog Metadata",
    "vgi.subtitle": "Catalogs, tables, columns, grants, tags and lineage of open-source Unity Catalog or Databricks.",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query.Farm LLC",
    "vgi.keywords": keywords_array(
        "unity catalog, databricks, data catalog, metadata, tables, columns, grants, lineage, governance"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
