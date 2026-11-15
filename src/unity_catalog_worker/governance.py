"""Governance tables: storage, connections, Delta Sharing, grants, owners, tags and lineage.

Storage credentials, connections and recipients are listed by name and kind only:
no role ARNs, keys, connection passwords, recipient tokens or activation links ever
reach a row. Connection options are masked unless the option name is on a short
allow-list of non-secret settings (host, port, ...).

Databricks-only APIs (connections, Delta Sharing, effective permissions, tags, the
Data Lineage API) are feature-detected: against open-source Unity Catalog those
tables raise a clear "not implemented by this server" error, and the ``tags``
columns of ``datasets`` / ``columns`` are NULL.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pyarrow as pa

from .assets import _table_listings, _table_scope
from .client import UC_API, NotFoundError, UnityCatalogClient, UnityCatalogConfig, quote
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
    drain,
    full_name,
    lower,
    normalize_privilege,
    principal_type,
    schema_scope,
    schemas_listing,
    split_scope,
    ts,
)

LINEAGE_API = "/api/2.0/lineage-tracking"
TAG_POLICIES_API = "/api/2.1/tag-policies"


def _times() -> list[pa.Field[Any]]:
    return [
        column("created_at", TSTZ, "When the object was created."),
        column("created_by", pa.string(), "Principal that created the object."),
        column("updated_at", TSTZ, "When the object was last changed."),
        column("updated_by", pa.string(), "Principal that last changed the object."),
    ]


def _time_vals(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "created_at": ts(item.get("created_at")),
        "created_by": item.get("created_by"),
        "updated_at": ts(item.get("updated_at")),
        "updated_by": item.get("updated_by"),
    }


# ---------------------------------------------------------------------------- storage


def credentials_listing(cfg: UnityCatalogConfig) -> Listing:
    """Databricks ``/storage-credentials``, falling back to open source's ``/credentials``."""
    return Listing(
        f"{UC_API}/storage-credentials",
        "storage_credentials",
        fallback=Listing(f"{UC_API}/credentials", "credentials"),
    )


class ExternalLocations(ApiScan):
    """Rows for ``unity_catalog.main.external_locations``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            catalog_tool_column(),
            column("id", pa.string(), "External location name (unique in the metastore).", nullable=False),
            column("name", pa.string(), "External location name.", nullable=False),
            column("url", pa.string(), "Storage URL the location governs, e.g. 's3://bucket/path'."),
            column("credential_name", pa.string(), "Storage credential used to reach it (storage_credentials.name)."),
            column("read_only", pa.bool_(), "True for a read-only location (Databricks)."),
            column("isolation_mode", pa.string(), "OPEN or ISOLATED (Databricks)."),
            column("description", pa.string(), "Comment."),
            column("owner", pa.string(), "Owner principal."),
            column("uuid", pa.string(), "External location UUID (open source)."),
            *_times(),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "external_locations_scan"
        description = "External locations: governed cloud storage paths and the credential that reaches them."
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str], cfg: UnityCatalogConfig, wanted: set[str]) -> list[Listing]:
        """``GET /external-locations``."""
        return [Listing(f"{UC_API}/external-locations", "external_locations")]

    @classmethod
    def rows(
        cls, client: UnityCatalogClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map locations to rows."""
        return [
            {
                "catalog_tool": "unity_catalog",
                "id": e["name"],
                "name": e["name"],
                "url": e.get("url"),
                "credential_name": e.get("credential_name"),
                "read_only": e.get("read_only"),
                "isolation_mode": e.get("isolation_mode"),
                "description": e.get("comment") or None,
                "owner": e.get("owner"),
                "uuid": e.get("id"),
                **_time_vals(e),
            }
            for e in items
            if e.get("name")
        ]


CREDENTIAL_KINDS = (
    "aws_iam_role",
    "azure_managed_identity",
    "azure_service_principal",
    "databricks_gcp_service_account",
    "gcp_service_account_key",
    "cloudflare_api_token",
)


class StorageCredentials(ApiScan):
    """Rows for ``unity_catalog.main.storage_credentials``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            catalog_tool_column(),
            column("id", pa.string(), "Storage credential name (unique in the metastore).", nullable=False),
            column("name", pa.string(), "Storage credential name.", nullable=False),
            column(
                "credential_type",
                pa.string(),
                "Kind of cloud identity: aws_iam_role, azure_managed_identity, azure_service_principal, "
                "databricks_gcp_service_account, cloudflare_api_token. The identity itself is never returned.",
            ),
            column("purpose", pa.string(), "STORAGE (open source); NULL on Databricks."),
            column("read_only", pa.bool_(), "True when the credential only allows reads (Databricks)."),
            column("used_for_managed_storage", pa.bool_(), "True when it backs managed storage (Databricks)."),
            column("isolation_mode", pa.string(), "OPEN or ISOLATED (Databricks)."),
            column("description", pa.string(), "Comment."),
            column("owner", pa.string(), "Owner principal."),
            column("uuid", pa.string(), "Credential UUID."),
            *_times(),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "storage_credentials_scan"
        description = "Storage credentials by name and kind (never the secret material)."
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str], cfg: UnityCatalogConfig, wanted: set[str]) -> list[Listing]:
        """``GET /storage-credentials`` (Databricks) or ``GET /credentials`` (open source)."""
        return [credentials_listing(cfg)]

    @classmethod
    def rows(
        cls, client: UnityCatalogClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map credentials to rows, keeping only the kind of identity."""
        return [
            {
                "catalog_tool": "unity_catalog",
                "id": c["name"],
                "name": c["name"],
                "credential_type": next((k for k in CREDENTIAL_KINDS if c.get(k)), None),
                "purpose": c.get("purpose"),
                "read_only": c.get("read_only"),
                "used_for_managed_storage": c.get("used_for_managed_storage"),
                "isolation_mode": c.get("isolation_mode"),
                "description": c.get("comment") or None,
                "owner": c.get("owner"),
                "uuid": c.get("id"),
                **_time_vals(c),
            }
            for c in items
            if c.get("name")
        ]


# ---------------------------------------------------------------------------- connections / sharing

# Connection option names that are configuration, not secrets. Everything else is masked.
SAFE_OPTIONS = frozenset(
    {
        "host",
        "port",
        "database",
        "warehouse",
        "httppath",
        "http_path",
        "catalog",
        "account",
        "region",
        "sfwarehouse",
        "sfrole",
        "trustservercertificate",
        "authorization_endpoint",
        "token_endpoint",
        "workspace",
        "project",
        "url",
        "base_path",
    }
)
MASK = "****"


def mask_options(options: Any) -> list[tuple[str, str]] | None:
    """Return connection options as MAP entries; values of non-allow-listed keys become ``****``."""
    if not isinstance(options, dict):
        return None
    return [
        (str(k), str(v) if str(k).lower() in SAFE_OPTIONS and v is not None else MASK)
        for k, v in sorted(options.items())
    ]


class Connections(ApiScan):
    """Rows for ``unity_catalog.main.connections``."""

    DATABRICKS_ONLY = "the Lakehouse Federation connections API"
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            catalog_tool_column(),
            column("id", pa.string(), "Connection name.", nullable=False),
            column("name", pa.string(), "Connection name.", nullable=False),
            column("connection_type", pa.string(), "MYSQL, POSTGRESQL, SNOWFLAKE, SQLSERVER, BIGQUERY, ..."),
            column("credential_type", pa.string(), "USERNAME_PASSWORD, OAUTH, ..."),
            column(
                "options",
                STR_MAP,
                "Connection options; values other than host, port, database, warehouse, httpPath and similar "
                "settings are masked as '****'.",
            ),
            column("read_only", pa.bool_(), "True for read-only connections."),
            column("provisioning_state", pa.string(), "Provisioning state, e.g. ACTIVE."),
            column("description", pa.string(), "Comment."),
            column("owner", pa.string(), "Owner principal."),
            column("uuid", pa.string(), "Connection UUID."),
            *_times(),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "connections_scan"
        description = "Lakehouse Federation connections to external databases (secrets masked)."
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str], cfg: UnityCatalogConfig, wanted: set[str]) -> list[Listing]:
        """``GET /connections``."""
        return [Listing(f"{UC_API}/connections", "connections")]

    @classmethod
    def rows(
        cls, client: UnityCatalogClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map connections to rows with masked options."""
        return [
            {
                "catalog_tool": "unity_catalog",
                "id": c["name"],
                "name": c["name"],
                "connection_type": c.get("connection_type"),
                "credential_type": c.get("credential_type"),
                "options": mask_options(c.get("options")),
                "read_only": c.get("read_only"),
                "provisioning_state": (c.get("provisioning_info") or {}).get("state"),
                "description": c.get("comment") or None,
                "owner": c.get("owner"),
                "uuid": c.get("connection_id"),
                **_time_vals(c),
            }
            for c in items
            if c.get("name")
        ]


class Shares(ApiScan):
    """Rows for ``unity_catalog.main.shares``."""

    DATABRICKS_ONLY = "the Delta Sharing shares API"
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            catalog_tool_column(),
            column("id", pa.string(), "Share name.", nullable=False),
            column("name", pa.string(), "Share name.", nullable=False),
            column("description", pa.string(), "Comment."),
            column("owner", pa.string(), "Owner principal."),
            *_times(),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "shares_scan"
        description = "Delta Sharing shares offered by this metastore (names only)."
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str], cfg: UnityCatalogConfig, wanted: set[str]) -> list[Listing]:
        """``GET /shares``."""
        return [Listing(f"{UC_API}/shares", "shares")]

    @classmethod
    def rows(
        cls, client: UnityCatalogClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map shares to rows."""
        return [
            {
                "catalog_tool": "unity_catalog",
                "id": s["name"],
                "name": s["name"],
                "description": s.get("comment") or None,
                "owner": s.get("owner"),
                **_time_vals(s),
            }
            for s in items
            if s.get("name")
        ]


class Recipients(ApiScan):
    """Rows for ``unity_catalog.main.recipients``."""

    DATABRICKS_ONLY = "the Delta Sharing recipients API"
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            catalog_tool_column(),
            column("id", pa.string(), "Recipient name.", nullable=False),
            column("name", pa.string(), "Recipient name.", nullable=False),
            column("authentication_type", pa.string(), "DATABRICKS (Databricks-to-Databricks) or TOKEN (open)."),
            column("activated", pa.bool_(), "Whether the recipient has activated its credential."),
            column("cloud", pa.string(), "Cloud of a DATABRICKS recipient's metastore."),
            column("region", pa.string(), "Region of a DATABRICKS recipient's metastore."),
            column("data_recipient_global_metastore_id", pa.string(), "Global metastore id of a DATABRICKS recipient."),
            column("description", pa.string(), "Comment."),
            column("owner", pa.string(), "Owner principal."),
            *_times(),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "recipients_scan"
        description = "Delta Sharing recipients (names and type only; tokens and activation links are never read)."
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str], cfg: UnityCatalogConfig, wanted: set[str]) -> list[Listing]:
        """``GET /recipients``."""
        return [Listing(f"{UC_API}/recipients", "recipients")]

    @classmethod
    def rows(
        cls, client: UnityCatalogClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map recipients to rows (no tokens, no activation URL, no sharing code)."""
        return [
            {
                "catalog_tool": "unity_catalog",
                "id": r["name"],
                "name": r["name"],
                "authentication_type": r.get("authentication_type"),
                "activated": r.get("activated"),
                "cloud": r.get("cloud"),
                "region": r.get("region"),
                "data_recipient_global_metastore_id": r.get("data_recipient_global_metastore_id"),
                "description": r.get("comment") or None,
                "owner": r.get("owner"),
                **_time_vals(r),
            }
            for r in items
            if r.get("name")
        ]


# ---------------------------------------------------------------------------- grants

SECURABLE_TYPES = (
    "metastore",
    "catalog",
    "schema",
    "table",
    "volume",
    "function",
    "registered_model",
    "external_location",
    "storage_credential",
)
_PARTS = {"catalog": 1, "external_location": 1, "storage_credential": 1, "metastore": 1, "schema": 2}


def _api_securable(securable_type: str, cfg: UnityCatalogConfig) -> str:
    """Open source calls a storage credential's securable type ``credential``."""
    if securable_type == "storage_credential" and not cfg.is_databricks:
        return "credential"
    return securable_type


def securables(client: UnityCatalogClient, pd: Pushdown) -> list[str]:
    """``type|full_name`` of every securable in scope.

    ``securable_type`` = / IN limits the kinds; ``securable_name`` = / IN names them directly;
    ``catalog_name`` = / IN limits the catalog-scoped kinds to those catalogs.
    """
    types = [t for t in (pd.strs("securable_type") or SECURABLE_TYPES) if t in SECURABLE_TYPES]
    names = pd.strs("securable_name")
    if names is not None:
        return [f"{t}|{n}" for t in types for n in names if len(n.split(".")) == _PARTS.get(t, 3)]
    out: list[str] = []
    cfg = client.config
    if "metastore" in types:
        summary = client.get(f"{UC_API}/metastore_summary")
        if isinstance(summary, dict) and summary.get("metastore_id"):
            out.append(f"metastore|{summary['metastore_id']}")
    if "catalog" in types:
        out.extend(f"catalog|{c}" for c in catalog_names(client, pd))
    child_kinds = {"schema", "table", "volume", "function"} & set(types)
    if child_kinds:
        scopes = schema_scope(client, pd, ("catalog_name",), ())
        if "schema" in types:
            out.extend(f"schema|{s}" for s in scopes)
        for scope in scopes:
            cat, sch = split_scope(scope)
            q = (("catalog_name", cat), ("schema_name", sch))
            if "table" in types:
                for listing in _table_listings([f"S:{scope}"], cfg, omit_columns=True, omit_properties=True):
                    out.extend(
                        f"table|{full_name(cat, sch, t['name'])}" for t in drain(client, listing) if t.get("name")
                    )
            if "volume" in types:
                vl = Listing(f"{UC_API}/volumes", "volumes", q, missing_ok=True)
                out.extend(f"volume|{full_name(cat, sch, v['name'])}" for v in drain(client, vl) if v.get("name"))
            if "function" in types:
                fl = Listing(f"{UC_API}/functions", "functions", q, missing_ok=True)
                out.extend(f"function|{full_name(cat, sch, f['name'])}" for f in drain(client, fl) if f.get("name"))
    if "registered_model" in types:
        q2: tuple[tuple[str, Any], ...] = ()
        cat_one = pd.one("catalog_name")
        if cat_one is not None:
            q2 = (("catalog_name", str(cat_one)),)
        ml = Listing(f"{UC_API}/models", "registered_models", q2, missing_ok=True)
        out.extend(
            "registered_model|"
            + str(m.get("full_name") or full_name(m.get("catalog_name"), m.get("schema_name"), m["name"]))
            for m in drain(client, ml)
            if m.get("name")
        )
    if "external_location" in types:
        el = Listing(f"{UC_API}/external-locations", "external_locations", optional=True)
        out.extend(f"external_location|{e['name']}" for e in drain(client, el) if e.get("name"))
    if "storage_credential" in types:
        out.extend(f"storage_credential|{c['name']}" for c in drain(client, credentials_listing(cfg)) if c.get("name"))
    return out


def _securable_catalog(securable_type: str, name: str) -> str | None:
    if securable_type in ("catalog", "schema", "table", "volume", "function", "registered_model"):
        return name.split(".")[0]
    return None


class Grants(ApiScan):
    """Rows for ``unity_catalog.main.grants``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            catalog_tool_column(),
            column(
                "securable_type",
                pa.string(),
                "metastore, catalog, schema, table, volume, function, registered_model, external_location or "
                "storage_credential.",
                nullable=False,
            ),
            column(
                "securable_name",
                pa.string(),
                "Full name of the securable (the metastore id for metastore).",
                nullable=False,
            ),
            column("catalog_name", pa.string(), "Catalog of a catalog-scoped securable."),
            column(
                "principal", pa.string(), "User email, group name or service principal application id.", nullable=False
            ),
            column(
                "principal_type",
                pa.string(),
                "Best guess from the name: user (has @), service_principal (UUID), else group.",
            ),
            column(
                "privilege",
                pa.string(),
                "Privilege, Databricks spelling: USE_CATALOG, SELECT, MODIFY, ...",
                nullable=False,
            ),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "grants_scan"
        description = "Explicit privilege grants on every securable (no inherited privileges)."
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    PERMISSIONS_PATH: ClassVar[str] = "permissions"

    @classmethod
    def fanout(cls, client: UnityCatalogClient, pd: Pushdown) -> list[str] | None:
        """Securables in scope (``securable_type``, ``securable_name``, ``catalog_name`` filters)."""
        return securables(client, pd)

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str], cfg: UnityCatalogConfig, wanted: set[str]) -> list[Listing]:
        """``GET /permissions/{type}/{full_name}`` per securable; ``principal`` = is sent as ``?principal=``."""
        principal = pd.one("principal")
        q = (("principal", str(principal)),) if principal is not None else ()
        out = []
        for i in ids:
            stype, _, name = i.partition("|")
            out.append(
                Listing(
                    f"{UC_API}/{cls.PERMISSIONS_PATH}/{_api_securable(stype, cfg)}/{quote(name)}",
                    "privilege_assignments",
                    q,
                    paged=False,
                    missing_ok=True,
                    context=(("securable_type", stype), ("securable_name", name)),
                )
            )
        return out

    @classmethod
    def rows(
        cls, client: UnityCatalogClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """One row per (principal, privilege)."""
        stype, name = str(listing.ctx("securable_type")), str(listing.ctx("securable_name"))
        out = []
        for a in items:
            principal = a.get("principal")
            if not principal:
                continue
            for p in a.get("privileges") or []:
                priv = p.get("privilege") if isinstance(p, dict) else p
                if not priv:
                    continue
                row = {
                    "catalog_tool": "unity_catalog",
                    "securable_type": stype,
                    "securable_name": name,
                    "catalog_name": _securable_catalog(stype, name),
                    "principal": principal,
                    "principal_type": principal_type(principal),
                    "privilege": normalize_privilege(priv),
                }
                if isinstance(p, dict):
                    row["inherited_from_type"] = lower(p.get("inherited_from_type"))
                    row["inherited_from_name"] = p.get("inherited_from_name")
                out.append(row)
        return out


class EffectiveGrants(Grants):
    """Rows for ``unity_catalog.main.effective_grants``."""

    DATABRICKS_ONLY = "the effective permissions API"
    PERMISSIONS_PATH: ClassVar[str] = "effective-permissions"
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            *Grants.FIXED_SCHEMA,
            column(
                "inherited_from_type",
                pa.string(),
                "Securable type the privilege is inherited from; NULL when granted directly.",
            ),
            column("inherited_from_name", pa.string(), "Full name of the securable it is inherited from."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "effective_grants_scan"
        description = "Effective privileges on every securable, including those inherited from parents (Databricks)."
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True


# ---------------------------------------------------------------------------- owners

OWNER_KINDS = (
    "metastore",
    "catalog",
    "schema",
    "dataset",
    "volume",
    "function",
    "registered_model",
    "external_location",
    "storage_credential",
    "connection",
    "share",
    "recipient",
)


class Owners(ApiScan):
    """Rows for ``unity_catalog.main.owners``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            catalog_tool_column(),
            column("entity_id", pa.string(), "Full name of the owned object (its table's id column).", nullable=False),
            column(
                "entity_type",
                pa.string(),
                "metastore, catalog, schema, dataset (table or view), volume, function, registered_model, "
                "external_location, storage_credential, connection, share or recipient.",
                nullable=False,
            ),
            column("entity_name", pa.string(), "Short name of the owned object."),
            column("qualified_name", pa.string(), "Lower-cased full name of the owned object."),
            column("owner_id", pa.string(), "Owner principal as Unity Catalog reports it.", nullable=False),
            column(
                "owner_type",
                pa.string(),
                "'user', 'group' or 'service_principal' (inferred from the name).",
                nullable=False,
            ),
            column("owner_name", pa.string(), "Owner principal."),
            column("owner_email", pa.string(), "The owner when it is an email address."),
            column("ownership_type", pa.string(), "Always 'OWNER': Unity Catalog has one owner per object."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "owners_scan"
        description = "The owner of every Unity Catalog object, in the shared data-catalog vocabulary."
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def fanout(cls, client: UnityCatalogClient, pd: Pushdown) -> list[str] | None:
        """Kinds in scope (``entity_type`` = / IN) and, for schema-scoped kinds, every schema."""
        kinds = [k for k in (pd.strs("entity_type") or OWNER_KINDS) if k in OWNER_KINDS]
        out = [f"K:{k}" for k in kinds]
        if {"schema", "dataset", "volume", "function"} & set(kinds):
            for cat in catalog_names(client, pd, "_none_"):
                schemas = drain(client, schemas_listing(client.config, cat))
                out.extend(f"S:{cat}.{s['name']}" for s in schemas if s.get("name"))
        return out

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str], cfg: UnityCatalogConfig, wanted: set[str]) -> list[Listing]:
        """Return the listing of every kind in scope."""
        kinds = {i[2:] for i in ids if i.startswith("K:")}
        scopes = [i[2:] for i in ids if i.startswith("S:")]
        out: list[Listing] = []

        def add(listing: Listing, kind: str) -> None:
            out.append(
                Listing(
                    listing.path,
                    listing.key,
                    listing.query,
                    paged=listing.paged,
                    max_size=listing.max_size,
                    missing_ok=True,
                    optional=True,
                    fallback=listing.fallback,
                    context=(("kind", kind),),
                )
            )

        if "metastore" in kinds:
            add(Listing(f"{UC_API}/metastore_summary", None, paged=False), "metastore")
        if "catalog" in kinds:
            add(catalogs_listing(cfg), "catalog")
        if "schema" in kinds:
            for cat in sorted({split_scope(s)[0] for s in scopes}):
                add(schemas_listing(cfg, cat), "schema")
        for scope in scopes:
            cat, sch = split_scope(scope)
            q = (("catalog_name", cat), ("schema_name", sch))
            if "dataset" in kinds:
                for listing in _table_listings([f"S:{scope}"], cfg, omit_columns=True, omit_properties=True):
                    add(listing, "dataset")
            if "volume" in kinds:
                add(Listing(f"{UC_API}/volumes", "volumes", q), "volume")
            if "function" in kinds:
                add(Listing(f"{UC_API}/functions", "functions", q), "function")
        if "registered_model" in kinds:
            add(Listing(f"{UC_API}/models", "registered_models"), "registered_model")
        if "external_location" in kinds:
            add(Listing(f"{UC_API}/external-locations", "external_locations"), "external_location")
        if "storage_credential" in kinds:
            add(credentials_listing(cfg), "storage_credential")
        if "connection" in kinds:
            add(Listing(f"{UC_API}/connections", "connections"), "connection")
        if "share" in kinds:
            add(Listing(f"{UC_API}/shares", "shares"), "share")
        if "recipient" in kinds:
            add(Listing(f"{UC_API}/recipients", "recipients"), "recipient")
        return out

    @classmethod
    def rows(
        cls, client: UnityCatalogClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """One row per owned object that has an owner."""
        kind = str(listing.ctx("kind"))
        out = []
        for it in items:
            owner = it.get("owner")
            if not owner:
                continue
            if kind == "metastore":
                eid, short = it.get("metastore_id"), it.get("name") or it.get("metastore_id")
            elif kind in ("catalog", "external_location", "storage_credential", "connection", "share", "recipient"):
                eid = short = it.get("name")
            elif kind == "schema":
                short = it.get("name")
                eid = it.get("full_name") or full_name(it.get("catalog_name"), short)
            else:
                short = it.get("name")
                eid = it.get("full_name") or full_name(it.get("catalog_name"), it.get("schema_name"), short)
            if not eid:
                continue
            otype = principal_type(owner) or "group"
            out.append(
                {
                    "catalog_tool": "unity_catalog",
                    "entity_id": eid,
                    "entity_type": kind,
                    "entity_name": short,
                    "qualified_name": lower(eid),
                    "owner_id": owner,
                    "owner_type": otype,
                    "owner_name": owner,
                    "owner_email": owner if otype == "user" else None,
                    "ownership_type": "OWNER",
                }
            )
        return out


# ---------------------------------------------------------------------------- tags


class Tags(ApiScan):
    """Rows for ``unity_catalog.main.tags``."""

    DATABRICKS_ONLY = "the governed tags (tag policies) API"
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            catalog_tool_column(),
            column("id", pa.string(), "Tag key (the governed tag's identifier in SQL and the UI).", nullable=False),
            column("name", pa.string(), "Tag key.", nullable=False),
            column("description", pa.string(), "What the tag means and when to apply it."),
            column("allowed_values", STR_LIST, "Values the tag may take; empty when any value is allowed."),
            column("policy_id", pa.string(), "Tag policy id."),
            column("created_at", TSTZ, "When the tag policy was created."),
            column("updated_at", TSTZ, "When the tag policy was last changed."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "tags_scan"
        description = "Governed tag definitions (Databricks tag policies)."
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str], cfg: UnityCatalogConfig, wanted: set[str]) -> list[Listing]:
        """``GET /api/2.1/tag-policies`` (paged with ``page_size``)."""
        return [Listing(TAG_POLICIES_API, "tag_policies", size_param="page_size")]

    @classmethod
    def rows(
        cls, client: UnityCatalogClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map tag policies to rows."""
        return [
            {
                "catalog_tool": "unity_catalog",
                "id": t["tag_key"],
                "name": t["tag_key"],
                "description": t.get("description") or None,
                "allowed_values": [
                    str(v["name"]) for v in t.get("values") or [] if isinstance(v, dict) and v.get("name")
                ],
                "policy_id": t.get("id"),
                "created_at": ts(t.get("create_time")),
                "updated_at": ts(t.get("update_time")),
            }
            for t in items
            if t.get("tag_key")
        ]


TAG_ENTITY_KINDS = ("catalog", "schema", "table", "column", "volume")


class TagAssignments(ApiScan):
    """Rows for ``unity_catalog.main.tag_assignments``."""

    DATABRICKS_ONLY = "the entity tag assignments API"
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            catalog_tool_column(),
            column(
                "entity_type",
                pa.string(),
                "catalog, schema, table (tables and views), column or volume.",
                nullable=False,
            ),
            column(
                "entity_id",
                pa.string(),
                "Full name of the tagged object; for a column, its table's full name (datasets.id).",
                nullable=False,
            ),
            column("column_name", pa.string(), "Tagged column (entity_type 'column' only)."),
            column("catalog_name", pa.string(), "Catalog of the tagged object.", nullable=False),
            column("tag_key", pa.string(), "Tag key (tags.id for governed tags).", nullable=False),
            column("tag_value", pa.string(), "Tag value; NULL for a key-only tag."),
            column("source_type", pa.string(), "How the tag was assigned, e.g. user-assigned or system-assigned."),
            column("updated_at", TSTZ, "When the assignment last changed."),
            column("updated_by", pa.string(), "Who last changed the assignment."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "tag_assignments_scan"
        description = "Tags on catalogs, schemas, tables, columns and volumes (Databricks)."
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def fanout(cls, client: UnityCatalogClient, pd: Pushdown) -> list[str] | None:
        """``type|name`` of every taggable object in scope (``entity_type``, ``catalog_name``, ``entity_id``)."""
        kinds = [k for k in (pd.strs("entity_type") or TAG_ENTITY_KINDS) if k in TAG_ENTITY_KINDS]
        out: list[str] = []
        ids = pd.strs("entity_id")
        cats = [i for i in ids if "." not in i] if ids is not None else catalog_names(client, pd)
        if "catalog" in kinds:
            out.extend(f"catalogs|{c}" for c in cats)
        if not ({"schema", "table", "column", "volume"} & set(kinds)):
            return out
        if ids is not None:
            schemas = [i for i in ids if i.count(".") == 1]
            tables = [i for i in ids if i.count(".") == 2]
            out.extend(f"schemas|{s}" for s in schemas if "schema" in kinds)
            out.extend(f"tables|{t}" for t in tables if "table" in kinds)
            out.extend(f"volumes|{t}" for t in tables if "volume" in kinds)
            if "column" in kinds:
                for listing in _table_listings(
                    [f"T:{t}" for t in tables], client.config, omit_columns=False, omit_properties=True
                ):
                    for t in drain(client, listing):
                        fn = full_name(t.get("catalog_name"), t.get("schema_name"), t.get("name"))
                        out.extend(f"columns|{fn}.{c['name']}" for c in t.get("columns") or [] if c.get("name"))
            return out
        scopes = schema_scope(client, pd, ("catalog_name",), ("schema_name",))
        for scope in scopes:
            cat, sch = split_scope(scope)
            if "schema" in kinds:
                out.append(f"schemas|{scope}")
            if {"table", "column"} & set(kinds):
                for listing in _table_listings(
                    [f"S:{scope}"], client.config, omit_columns="column" not in kinds, omit_properties=True
                ):
                    for t in drain(client, listing):
                        fn = full_name(cat, sch, t.get("name"))
                        if "table" in kinds:
                            out.append(f"tables|{fn}")
                        if "column" in kinds:
                            out.extend(f"columns|{fn}.{c['name']}" for c in t.get("columns") or [] if c.get("name"))
            if "volume" in kinds:
                vl = Listing(
                    f"{UC_API}/volumes", "volumes", (("catalog_name", cat), ("schema_name", sch)), missing_ok=True
                )
                out.extend(f"volumes|{full_name(cat, sch, v['name'])}" for v in drain(client, vl) if v.get("name"))
        return out

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str], cfg: UnityCatalogConfig, wanted: set[str]) -> list[Listing]:
        """``GET /entity-tag-assignments/{type}/{full_name}/tags`` per object."""
        out = []
        for i in ids:
            etype, _, name = i.partition("|")
            out.append(
                Listing(
                    f"{UC_API}/entity-tag-assignments/{etype}/{quote(name)}/tags",
                    "tag_assignments",
                    missing_ok=True,
                    context=(("entity_type", etype), ("entity_name", name)),
                )
            )
        return out

    @classmethod
    def rows(
        cls, client: UnityCatalogClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """One row per tag assignment."""
        etype = str(listing.ctx("entity_type")).removesuffix("s")
        name = str(listing.ctx("entity_name"))
        entity_id, col = (name.rsplit(".", 1)[0], name.rsplit(".", 1)[1]) if etype == "column" else (name, None)
        return [
            {
                "catalog_tool": "unity_catalog",
                "entity_type": etype,
                "entity_id": entity_id,
                "column_name": col,
                "catalog_name": name.split(".")[0],
                "tag_key": t["tag_key"],
                "tag_value": t.get("tag_value"),
                "source_type": t.get("source_type"),
                "updated_at": ts(t.get("update_time")),
                "updated_by": t.get("updated_by"),
            }
            for t in items
            if t.get("tag_key")
        ]


# ---------------------------------------------------------------------------- lineage

ENTITY_INFOS = (
    ("notebookInfos", "notebook", ("workspace_id", "notebook_id")),
    ("jobInfos", "job", ("workspace_id", "job_id")),
    ("queryInfos", "query", ("workspace_id", "query_id")),
    ("dashboardInfos", "dashboard", ("workspace_id", "dashboard_id")),
    ("dashboardV3Infos", "dashboard", ("workspace_id", "dashboard_id")),
    ("pipelineInfos", "pipeline", ("workspace_id", "pipeline_id")),
)


def _table_ref(info: dict[str, Any]) -> str | None:
    name = full_name(info.get("catalog_name"), info.get("schema_name"), info.get("table_name") or info.get("name"))
    return name if name.count(".") == 2 else None


def _entities(entry: dict[str, Any]) -> list[tuple[str, str]]:
    """``(type, id)`` of the notebooks, jobs, queries, ... in one lineage entry."""
    out = []
    for key, kind, (ws, ident) in ENTITY_INFOS:
        for info in entry.get(key) or []:
            if isinstance(info, dict) and info.get(ident) is not None:
                out.append((kind, f"{info.get(ws)}/{info.get(ident)}"))
    return out


class LineageEdges(ApiScan):
    """Rows for ``unity_catalog.main.lineage_edges``."""

    DATABRICKS_ONLY = "the Data Lineage API"
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            catalog_tool_column(),
            column("level", pa.string(), "'dataset' (table/entity to table/entity) or 'column'.", nullable=False),
            column(
                "upstream_id",
                pa.string(),
                "Upstream table full name, or '<workspace_id>/<id>' of a notebook, job, query, dashboard or pipeline.",
                nullable=False,
            ),
            column("upstream_type", pa.string(), "dataset, notebook, job, query, dashboard or pipeline."),
            column("upstream_qualified_name", pa.string(), "Lower-cased full name when upstream is a table."),
            column("upstream_column", pa.string(), "Upstream column (column level only)."),
            column(
                "downstream_id",
                pa.string(),
                "Downstream table full name, or '<workspace_id>/<id>' of the entity that reads the table.",
                nullable=False,
            ),
            column("downstream_type", pa.string(), "dataset, notebook, job, query, dashboard or pipeline."),
            column("downstream_qualified_name", pa.string(), "Lower-cased full name when downstream is a table."),
            column("downstream_column", pa.string(), "Downstream column (column level only)."),
            column("transformation", pa.string(), "Not reported by the Data Lineage API; always NULL."),
            column(
                "via",
                STR_LIST,
                "Notebooks, jobs, ... ('<type>:<workspace_id>/<id>') that moved data along a table-to-table edge.",
            ),
            column("lineage_timestamp", TSTZ, "When Databricks last observed the edge, when reported."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "lineage_edges_scan"
        description = "Table and column lineage captured by Databricks (Data Lineage API)."
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def fanout(cls, client: UnityCatalogClient, pd: Pushdown) -> list[str] | None:
        """``U:<table>`` (walk downstream from pushed upstream ids) or ``D:<table>`` (every table in scope)."""
        up = pd.strs("upstream_id") or pd.strs("upstream_qualified_name")
        down = pd.strs("downstream_id") or pd.strs("downstream_qualified_name")
        if up is not None and down is None:
            return [f"U:{t}" for t in up if t.count(".") == 2]
        if down is not None:
            return [f"D:{t}" for t in down if t.count(".") == 2]
        out: list[str] = []
        for listing in _table_listings(
            _table_scope(client, pd, "_none_"), client.config, omit_columns=True, omit_properties=True
        ):
            out.extend(
                f"D:{full_name(t.get('catalog_name'), t.get('schema_name'), t.get('name'))}"
                for t in drain(client, listing)
                if t.get("name")
            )
        return out

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str], cfg: UnityCatalogConfig, wanted: set[str]) -> list[Listing]:
        """``GET /api/2.0/lineage-tracking/table-lineage`` per table (``level`` = pushed down)."""
        levels = pd.strs("level")
        out = []
        for i in ids:
            mode, _, table = i.partition(":")
            params = (("include_entity_lineage", True), ("table_name", table))
            out.append(
                Listing(
                    f"{LINEAGE_API}/table-lineage",
                    None,
                    params,
                    paged=False,
                    missing_ok=True,
                    body=params,
                    context=(("mode", mode), ("table", table), ("levels", ",".join(levels or ["dataset", "column"]))),
                )
            )
        return out

    @classmethod
    def rows(
        cls, client: UnityCatalogClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Edges of one table: its upstreams (or downstreams in upstream mode), then column edges."""
        mode, table = str(listing.ctx("mode")), str(listing.ctx("table"))
        levels = set(str(listing.ctx("levels")).split(","))
        out: list[dict[str, Any]] = []
        has_table_edges = False
        for body in items:
            sides = [("upstreams", True), ("downstreams", False)] if mode == "D" else [("downstreams", True)]
            for key, tables_too in sides:
                for entry in body.get(key) or []:
                    if not isinstance(entry, dict):
                        continue
                    other = _table_ref(entry.get("tableInfo") or {})
                    ents = _entities(entry)
                    upstream_side = key == "upstreams"
                    if other:
                        if not tables_too:
                            continue
                        has_table_edges = True
                        if "dataset" in levels:
                            up, down = (other, table) if upstream_side else (table, other)
                            out.append(
                                cls._edge(
                                    "dataset",
                                    up,
                                    "dataset",
                                    None,
                                    down,
                                    "dataset",
                                    None,
                                    [f"{k}:{v}" for k, v in ents],
                                    (entry.get("tableInfo") or {}).get("lineage_timestamp"),
                                )
                            )
                    elif "dataset" in levels:
                        for kind, ident in ents:
                            if upstream_side:
                                out.append(cls._edge("dataset", ident, kind, None, table, "dataset", None, None, None))
                            else:
                                out.append(cls._edge("dataset", table, "dataset", None, ident, kind, None, None, None))
        if has_table_edges and "column" in levels:
            out.extend(cls._column_edges(client, table, mode))
        return out

    @classmethod
    def _column_edges(cls, client: UnityCatalogClient, table: str, mode: str) -> list[dict[str, Any]]:
        try:
            info = client.get(f"{UC_API}/tables/{quote(table)}")
        except NotFoundError:  # the table vanished between calls: no column lineage
            return []
        out = []
        for c in info.get("columns") or []:
            col = c.get("name") if isinstance(c, dict) else None
            if not col:
                continue
            params = {"column_name": col, "table_name": table}
            body = client.get(f"{LINEAGE_API}/column-lineage", params, params)
            key = "upstream_cols" if mode == "D" else "downstream_cols"
            for other in body.get(key) or []:
                ref = _table_ref(other)
                if not ref or not other.get("name"):
                    continue
                if mode == "D":
                    out.append(
                        cls._edge(
                            "column",
                            ref,
                            "dataset",
                            other["name"],
                            table,
                            "dataset",
                            col,
                            None,
                            other.get("lineage_timestamp"),
                        )
                    )
                else:
                    out.append(
                        cls._edge(
                            "column",
                            table,
                            "dataset",
                            col,
                            ref,
                            "dataset",
                            other["name"],
                            None,
                            other.get("lineage_timestamp"),
                        )
                    )
        return out

    @staticmethod
    def _edge(
        level: str,
        up: str,
        up_type: str,
        up_col: str | None,
        down: str,
        down_type: str,
        down_col: str | None,
        via: list[str] | None,
        when: Any,
    ) -> dict[str, Any]:
        return {
            "catalog_tool": "unity_catalog",
            "level": level,
            "upstream_id": up,
            "upstream_type": up_type,
            "upstream_qualified_name": lower(up) if up_type == "dataset" else None,
            "upstream_column": up_col,
            "downstream_id": down,
            "downstream_type": down_type,
            "downstream_qualified_name": lower(down) if down_type == "dataset" else None,
            "downstream_column": down_col,
            "transformation": None,
            "via": via,
            "lineage_timestamp": ts(when),
        }
