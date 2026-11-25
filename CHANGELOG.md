# Changelog

## 0.2.0 (2026-11-25)

- Speaks VGI protocol 3.4: require vgi-python `>=0.55.2,<0.56`; uv.lock re-locked. This release
  needs Haybarn 2.0.1 (or stock DuckDB 2.0 with `INSTALL vgi FROM query_farm`): Haybarn 2.0.0
  loads a 3.3 `vgi` extension that cannot attach it.
- SQL tests run on haybarn-unittest 2.0.1rc3.
- `serve` extra is `vgi-python[http,oauth]`; the Docker image installs it, so the image runs in
  Orchard hosted mode (`VGI_ORCHARD_HOSTED=1` with VGI_JWT_ISSUER, VGI_JWT_AUDIENCE,
  VGI_JWT_JWKS_URI and VGI_SIGNING_KEY). The HEALTHCHECK uses `HEAD /health`, which stays
  anonymous in hosted mode.

## 0.1.1 (2026-11-16)

- Bump vgi-python to 0.52.2.
- Requires Python 3.14 (CI, Docker image and tooling moved to 3.14).

## 0.1.0 (unreleased)

First release: Unity Catalog metadata (open-source Unity Catalog and Databricks) as DuckDB tables.

- 21 tables in `unity_catalog.main`: metastores, catalogs, schemas, datasets, views, columns, volumes,
  functions, registered_models, model_versions, external_locations, storage_credentials, connections,
  shares, recipients, grants, effective_grants, owners, tags, tag_assignments, lineage_edges.
- Offline scalars `uc_qualified_name` and `uc_privilege`.
- `unity_catalog` secret (PAT, OAuth M2M, Azure AD token or no auth), same field names as vgi-databricks.
- Tests against a strict replay of a real open-source Unity Catalog v0.7.0 server.

Targets: vgi-python 0.51 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
