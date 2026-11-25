<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query Unity Catalog Tables, Grants, Tags and Lineage in DuckDB

> **vgi-unity-catalog** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-unity-catalog/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-unity-catalog/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

[Unity Catalog](https://www.unitycatalog.io) is the governance layer of the lakehouse, both as the
open-source server (Linux Foundation) and as Databricks Unity Catalog. Both speak the same REST API,
`/api/2.1/unity-catalog/...`. This worker reads that metadata and gives it to you as typed DuckDB
tables. It is read-only and never reads table data:

- **Assets**: the metastore, catalogs, schemas, tables and views (type, format, storage location,
  owner, properties), columns (Spark type text and JSON, nullability, partition position, comments),
  view SQL, volumes, SQL and Python UDFs, and registered MLflow models with their versions.
- **Storage**: external locations and storage credentials, by name and kind only. Role ARNs, keys
  and secrets are never read.
- **Governance**: explicit grants on every securable, effective (inherited) grants on Databricks,
  and the owner of every object.
- **Databricks only**: Lakehouse Federation connections (with their options masked), Delta Sharing
  shares and recipients (names only), governed tags and tag assignments, and table and column
  lineage from the Data Lineage API.

vgi-unity-catalog 0.2.0 speaks VGI protocol 3.4, so it needs Haybarn 2.0.1 or newer (Haybarn 2.0.0 loads a 3.3 `vgi` extension that cannot attach it).

## Quick start

```sql
INSTALL vgi FROM community;
LOAD vgi;
ATTACH 'unity_catalog' (TYPE vgi,
  LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-unity-catalog@v0.2.0 vgi-unity-catalog-worker');

-- open-source Unity Catalog (bin/start-uc-server or the unitycatalog/unitycatalog image)
CREATE SECRET uc (TYPE unity_catalog, HOST 'http://localhost:8080');

-- Tables in one catalog by type and format. Only that catalog's schemas are listed.
SELECT table_type, data_source_format, count(*) AS tables
FROM unity_catalog.main.datasets WHERE catalog_name = 'unity' GROUP BY ALL ORDER BY tables DESC;

-- Partition columns of a table, read with one GET /tables/{full_name}.
SELECT column_name, native_data_type, partition_index
FROM unity_catalog.main.columns WHERE dataset_id = 'unity.default.user_countries' AND partition_index IS NOT NULL;

-- Who can SELECT from which table.
SELECT securable_name, principal FROM unity_catalog.main.grants
WHERE securable_type = 'table' AND privilege = 'SELECT' ORDER BY ALL;
```

On Databricks you can also ask what feeds a table:

```sql
CREATE OR REPLACE SECRET uc (TYPE unity_catalog, HOST 'https://dbc-a1b2c3d4-e5f6.cloud.databricks.com', TOKEN 'dapi...');

SELECT level, upstream_id, upstream_column, downstream_column
FROM unity_catalog.main.lineage_edges WHERE downstream_id = 'sales.marts.daily_revenue' ORDER BY ALL;
```

To use DuckDB 2.0 instead of Haybarn, load the extension from the Query.Farm repository:

```sql
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;
```

To run from a source checkout, run `make build` and then use
`LOCATION '/abs/path/to/vgi-unity-catalog/.venv/bin/vgi-unity-catalog-worker'`.

## Credentials

Every table reads the one secret of type `unity_catalog` in the session. Create it after `ATTACH`,
which registers the type. The field names are the same as those of the `databricks` secret in
[vgi-databricks](https://github.com/Query-farm/vgi-databricks), so the same values work for both
workers.

| Field | Meaning |
|---|---|
| `HOST` (required) | The server URL. For open source this is e.g. `http://localhost:8080`. For Databricks it is the workspace URL (`https://dbc-….cloud.databricks.com`, `https://adb-….azuredatabricks.net`, `https://….gcp.databricks.com`). A bare host name gets `https://`. |
| `TOKEN` | A bearer token: a Databricks personal access token, or an open-source Unity Catalog access token. |
| `CLIENT_ID`, `CLIENT_SECRET` | Databricks OAuth machine-to-machine. The service principal's secret is exchanged at `<HOST>/oidc/v1/token`. The token is cached per process and refreshed on expiry or a 401. |
| `AZURE_TOKEN` | An Azure AD (Entra ID) access token for Azure Databricks. |
| `AUTH_TYPE` | `auto` (the default) uses `TOKEN`, then `CLIENT_ID`/`CLIENT_SECRET`, then `AZURE_TOKEN`, and falls back to no authentication. The explicit values are `pat`, `oauth-m2m`, `azure` and `none`. |
| `FLAVOR` | `auto` (the default) means Databricks for `*.databricks.com` and `*.azuredatabricks.net` hosts, and open source otherwise. You can also set `databricks` or `oss`. It sets the `platform` column and the Catalog Explorer links in `url`. |
| `INCLUDE_BROWSE` | Databricks only. Also list objects the principal can only browse (`include_browse=true`). |
| `PAGE_SIZE` | Items per API page (`max_results`), capped at each endpoint's maximum. |
| `TIMEOUT` | Per-request timeout in seconds. The default is 30. |

```sql
CREATE SECRET uc (TYPE unity_catalog, HOST 'https://adb-1234567890123456.7.azuredatabricks.net',
                  CLIENT_ID '...', CLIENT_SECRET '...');
```

Credentials are read from the secret on every call. They are never written to logs, error messages
or the scan cursor, and nothing is read from environment variables. Responses with `429` and
`502`/`503`/`504` are retried with backoff that honours `Retry-After`. The worker only makes GET
requests (plus the OAuth token exchange).

## Tables and functions

All objects are in `unity_catalog.main`. Ids are Unity Catalog full names (`catalog`,
`catalog.schema`, `catalog.schema.object`).

| Object | One row per | Pushed down to the API |
|---|---|---|
| `metastores` | the metastore behind the server (`/metastore_summary`) | — |
| `catalogs` | catalog: type, owner, properties, storage root, federation and sharing source | `name` / `id` (= / IN → `GET /catalogs/{name}`) |
| `schemas` | schema | `catalog_name`, catalog of `id` |
| `datasets` | table or view: type, format, storage location, owner, properties, column count, partition columns, tags (Databricks) | `id` / `qualified_name` (→ `GET /tables/{full_name}`), `catalog_name` / `database`, `schema_name` / `schema`; `omit_columns` / `omit_properties` unless those columns are selected |
| `views` | view, materialized view, metric view or streaming table, with its SQL and dependencies | like `datasets` |
| `columns` | column: type name, Spark type text and JSON, precision/scale, nullable, primary key, partition index, comment, tags (Databricks) | `dataset_id` / `qualified_name`, `catalog_name`, `schema_name` |
| `volumes` | volume: managed / external, storage location | `catalog_name`, `schema_name`, schema of `id` |
| `functions` | UDF: parameters (`STRUCT[]`), return type, SQL / Python body | `catalog_name`, `schema_name`, schema of `id` |
| `registered_models` | MLflow model | `id` (→ `GET /models/{full_name}`), one `catalog_name` / `schema_name` |
| `model_versions` | model version: status, source, run id | `model_id` (one `…/versions` call each), `catalog_name` / `schema_name` |
| `external_locations` | external location: URL and storage credential | — |
| `storage_credentials` | storage credential: name and kind only | — (`/storage-credentials` on Databricks, `/credentials` on open source) |
| `connections` | Lakehouse Federation connection, options masked (Databricks) | — |
| `shares`, `recipients` | Delta Sharing share / recipient, names and types only (Databricks) | — |
| `grants` | (securable, principal, privilege) granted directly | `securable_type`, `securable_name`, `catalog_name`, `principal` (`?principal=`) |
| `effective_grants` | as `grants`, plus inherited privileges and their source (Databricks) | as `grants` |
| `owners` | owner of every object, in the shared vocabulary | `entity_type` decides which collections are listed |
| `tags` | governed tag (tag policy) with allowed values (Databricks) | — |
| `tag_assignments` | tag on a catalog, schema, table, column or volume (Databricks) | `entity_type`, `entity_id`, `catalog_name`, `schema_name` |
| `lineage_edges` | direct lineage edge, table and column level (Databricks Data Lineage API) | `downstream_id` / `downstream_qualified_name`, `upstream_id` / `upstream_qualified_name`, `level` |
| `uc_qualified_name(name)` | scalar, offline: the lower-cased `catalog.schema.table` of a (backtick-quoted) name | — |
| `uc_privilege(privilege)` | scalar, offline: `'USE CATALOG'` → `'USE_CATALOG'` | — |

**How pushdown works.** Unity Catalog lists tables, volumes and functions one schema at a time. Each
scan first works out the `catalog.schema` pairs in scope. It takes them from `=` / `IN` filters on
`catalog_name` and `schema_name` when you give them, and lists catalogs and schemas otherwise. Then
it reads only those schemas. An `=` / `IN` on the id column skips listing and fetches the object
directly. DuckDB re-applies every predicate, so pushdown only narrows what is fetched.

`grants`, `effective_grants`, `owners`, `tag_assignments`, `lineage_edges` and the `tags` columns
of `datasets` and `columns` make one request per object in scope. Filter them.

**Pagination.** `max_results` and `page_token` are used throughout. The page token, the resolved
scope and the listing position live in the scan's state, so a long scan resumes on any worker
process over the HTTP transport.

**Open source vs. Databricks.** Endpoints the server does not implement are detected from the
response. Open-source Unity Catalog answers them with a plain 404, or with `500 Couldn't unwrap
service` when authorization is enabled. Databricks answers with `ENDPOINT_NOT_FOUND`. On such a
server the Databricks-only tables raise "… which this Unity Catalog server does not implement",
`owners` skips those collections, and the `tags` columns are NULL. Privileges always use the
Databricks spelling (`USE_CATALOG`), including on open source, which reports `USE CATALOG`.
Lineage from system tables (`system.access.table_lineage`) is out of scope, because it needs a SQL
warehouse. Query it with a Databricks SQL connector.

## Shared data-catalog vocabulary

`vgi-unity-catalog` uses the table names and columns of
[vgi-datahub](https://github.com/Query-farm/vgi-datahub) and vgi-openmetadata: `datasets`,
`columns`, `owners`, `tags` and `lineage_edges`. Every entity table carries `catalog_tool`
(`'unity_catalog'`), `id`, `platform` (`'databricks'` or `'unity_catalog'`), `qualified_name`
(lower-cased `catalog.schema.table`), `description`, `created_at`, `updated_at` and `url`. You can
therefore `UNION ALL` catalogs, or join them to dbt, OpenLineage and Looker on `qualified_name`.
Columns Unity Catalog has no notion of are present and empty: `environment`, `glossary_terms`,
`domain_id`, `domain` and `deprecated` (always false).

```sql
-- Tables DataHub knows about that Unity Catalog does not (or the other way round).
SELECT coalesce(d.qualified_name, u.qualified_name) AS table_name, d.id IS NOT NULL AS in_datahub,
       u.id IS NOT NULL AS in_unity_catalog
FROM (SELECT * FROM datahub.main.datasets WHERE platform = 'databricks') d
FULL JOIN unity_catalog.main.datasets u USING (qualified_name)
WHERE d.id IS NULL OR u.id IS NULL;
```

## Testing

`make check` runs ruff, mypy and pydoclint, the unit tests, the SQL suite over the stdio, launch and
HTTP transports, and the metadata linter (score 100, zero findings at `info`).

The SQL suite runs against a **strict replay server**, `test/mock_uc/server.py`. It replays
`test/fixtures/uc_recording.json`, an exact capture of a real **open-source Unity Catalog v0.7.0**
server with authorization enabled. That server is the `unitycatalog/unitycatalog:v0.7.0` image,
seeded by `test/fixtures/seed_uc.py` on top of its sample `unity` catalog with:

- the `sales` and `ml` catalogs
- external Delta and Parquet tables with partition, decimal and array columns
- a view, a volume and a SQL UDF
- a registered model with two versions
- a storage credential and an external location
- grants for two users

The replay server works as follows:

- It matches the method, path, full query string and body of every request. Anything unrecorded
  fails with HTTP 418, and `ci/run-integration.sh` fails the run if any request was unrecorded.
  This proves that the pushed-down requests (`catalog_name=`, `omit_columns=`, `?principal=`, …)
  are exactly the ones the real server answered.
- It requires `Authorization: Bearer test-token` and rate-limits the first sighting of every
  third request (`429` + `Retry-After`).
- With `MOCK_FLAVOR=databricks` it adds `test/fixtures/databricks_extras.json`. These are the
  Databricks-only endpoints (lineage, tags, effective permissions, connections, Delta Sharing),
  written from the documented response shapes by `build_databricks_extras.py`. This instance also
  serves the OAuth M2M token endpoint.

To re-record (about 1 GB of RAM, a few minutes):

```sh
test/fixtures/uc_server.sh start
rm test/fixtures/uc_recording.json
UC_RECORD_UPSTREAM=http://localhost:18080 UC_RECORD_TOKEN="$(test/fixtures/uc_server.sh token)" make test-sql
test/fixtures/uc_server.sh stop
```

The container's admin token is only passed to the recording proxy. It is never recorded.

## Authorship & License

Copyright 2026 Query Farm LLC - https://query.farm

Licensed under the BSD Zero Clause License (0BSD); see [LICENSE](LICENSE).
