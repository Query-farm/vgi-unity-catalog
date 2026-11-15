"""Shared machinery for the API-backed tables: pushdown, scoping, paging, row conversion.

Every table is a ``TableFunctionGenerator`` subclass of ``ApiScan``. A subclass
declares its ``FIXED_SCHEMA`` and implements:

* ``fanout(client, pd)`` (optional) resolves, once per scan, what a listing has
  to be repeated for: Unity Catalog lists tables, volumes and functions one
  schema at a time, so ``datasets`` first resolves the ``catalog.schema`` pairs
  in scope (from pushed-down ``catalog_name`` / ``schema_name`` values, or by
  listing catalogs and schemas). The ids are stored in the cursor.
* ``requests(pd, ids, cfg, wanted)`` turns the pushed-down predicates (and the
  projected columns) into API listings: path, query parameters, page parameters.
* ``rows(client, items, wanted, listing)`` maps one page of API items to rows.

The cursor (``ScanState``) is ``(listing index, page token, fan-out ids)``. It
travels with each batch over the HTTP transport, holds no credentials, and lets
a scan resume on any worker process: Unity Catalog page tokens are opaque and
stateless on the server side. ``auto_apply_filters`` re-applies every predicate
to each batch, so translating a predicate only ever has to *narrow* what the
API returns, never be exact.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from dataclasses import dataclass, field
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Secret
from vgi.invocation import BindResponse
from vgi.table_filter_pushdown import PushdownFilters
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from .client import (
    SECRET_TYPE,
    UC_API,
    EndpointMissingError,
    NotFoundError,
    UnityCatalogClient,
    UnityCatalogConfig,
    UnityCatalogError,
    quote,
)

log = logging.getLogger(__name__)

CATALOG_TOOL = "unity_catalog"
TSTZ = pa.timestamp("us", tz="UTC")
STR_LIST = pa.list_(pa.string())
STR_MAP = pa.map_(pa.string(), pa.string())


@dataclass(slots=True, frozen=True, kw_only=True)
class NoArgs:
    """Catalog tables take no arguments."""


@dataclass(kw_only=True)
class ScanState(ArrowSerializableDataclass):
    """Resumable cursor: which listing, the next page token, and the fan-out ids."""

    listing: int = 0
    token: str = ""
    resolved: bool = False
    ids: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class Listing:
    """One (possibly paginated) API collection to read.

    Attributes:
        path: Absolute API path.
        key: Response key holding the items; ``None`` means the response *is* one item.
        query: Query parameters.
        paged: Whether the endpoint takes a page size and ``page_token``.
        size_param: Name of the page-size parameter (``max_results``; ``page_size`` for tag policies).
        max_size: The endpoint's maximum page size.
        missing_ok: Ignore "does not exist" (an equality filter named an object that is gone).
        optional: Skip the listing when the server does not implement the endpoint.
        fallback: Read this listing instead when the server does not implement ``path``.
        context: Values implied by the request (e.g. the securable a permissions call is for).
        body: JSON body sent with the GET (the Data Lineage API documents its parameters that way).

    """

    path: str
    key: str | None
    query: tuple[tuple[str, Any], ...] = ()
    paged: bool = True
    size_param: str = "max_results"
    max_size: int = 1000
    missing_ok: bool = False
    optional: bool = False
    fallback: Listing | None = None
    context: tuple[tuple[str, Any], ...] = ()
    body: tuple[tuple[str, Any], ...] | None = None

    def ctx(self, name: str) -> Any:
        """Return a context value by name (None when absent)."""
        return dict(self.context).get(name)


def secret_from(params: ProcessParams[Any]) -> dict[str, Any] | None:
    """Return the first resolved ``unity_catalog`` secret, if any."""
    for secret in params.secrets.of_type(SECRET_TYPE):
        return dict(secret)
    return None


def client_from(params: ProcessParams[Any]) -> UnityCatalogClient:
    """Build a client from the secret DuckDB resolved for this call."""
    return UnityCatalogClient(UnityCatalogConfig.from_secret(secret_from(params)))


# --------------------------------------------------------------------- conversions


def ts(value: Any) -> dt.datetime | None:
    """Parse epoch milliseconds or an ISO-8601 string into an aware UTC datetime; 0 / empty is NULL."""
    if value is None or value == "" or value == 0:
        return None
    if isinstance(value, int | float):
        return dt.datetime.fromtimestamp(value / 1000.0, tz=dt.UTC)
    text = str(value)
    if text.isdigit():
        return dt.datetime.fromtimestamp(int(text) / 1000.0, tz=dt.UTC)
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.UTC)


def to_int(value: Any) -> int | None:
    """Coerce to int, keeping NULL."""
    try:
        return None if value is None or value == "" else int(value)
    except (TypeError, ValueError):
        return None


def str_map(value: Any) -> list[tuple[str, str]] | None:
    """Turn a JSON object (``properties``) into MAP entries sorted by key; NULL when absent."""
    if not isinstance(value, dict):
        return None
    return [(str(k), "" if v is None else str(v)) for k, v in sorted(value.items())]


def json_text(value: Any) -> str | None:
    """Serialize a JSON-able value for a JSON column (``None`` stays NULL)."""
    if value is None:
        return None
    return json.dumps(value, sort_keys=True)


def lower(value: Any) -> str | None:
    """Lower-case text, keeping NULL."""
    return str(value).lower() if value is not None else None


def full_name(*parts: Any) -> str:
    """Dotted full name of a securable (``catalog.schema.table``)."""
    return ".".join(str(p) for p in parts if p is not None)


def principal_type(principal: str | None) -> str | None:
    """Best-effort kind of a Unity Catalog principal: user (email), service_principal (UUID) or group."""
    if not principal:
        return None
    if "@" in principal:
        return "user"
    p = principal.lower()
    if len(p) == 36 and p.count("-") == 4 and all(c in "0123456789abcdef-" for c in p):
        return "service_principal"
    return "group"


def normalize_privilege(value: Any) -> str | None:
    """``'USE CATALOG'`` (open source) and ``'USE_CATALOG'`` (Databricks) -> ``'USE_CATALOG'``."""
    if value is None:
        return None
    return str(value).strip().upper().replace(" ", "_")


# ------------------------------------------------------------------------ pushdown


class Pushdown:
    """Read-only helpers over the predicates DuckDB pushed into a scan."""

    def __init__(self, filters: PushdownFilters | None) -> None:
        """Wrap the (possibly absent) pushdown filters."""
        self.filters = filters

    def values(self, column: str) -> list[Any] | None:
        """Exact values a column is constrained to (``=`` or ``IN``), else None."""
        if not self.filters:
            return None
        arr = self.filters.get_column_values(column)
        if arr is None:
            return None
        return [v for v in arr.to_pylist() if v is not None]

    def strs(self, *columns: str) -> list[str] | None:
        """Text values of the first column (in order) that has an ``=`` / ``IN`` constraint."""
        for column in columns:
            vals = self.values(column)
            if vals is not None:
                return sorted({str(v) for v in vals})
        return None

    def one(self, column: str) -> Any | None:
        """Return the single value a column must equal, else None."""
        vals = self.values(column)
        return vals[0] if vals and len(vals) == 1 else None


# --------------------------------------------------------------------------- listings


def catalogs_listing(cfg: UnityCatalogConfig) -> Listing:
    """``GET /catalogs`` (``include_browse`` on Databricks when the secret asks for it)."""
    query = (("include_browse", True),) if cfg.include_browse else ()
    return Listing(f"{UC_API}/catalogs", "catalogs", query)


def schemas_listing(cfg: UnityCatalogConfig, catalog: str) -> Listing:
    """``GET /schemas?catalog_name=``."""
    query: tuple[tuple[str, Any], ...] = (("catalog_name", catalog),)
    if cfg.include_browse:
        query += (("include_browse", True),)
    return Listing(f"{UC_API}/schemas", "schemas", query, missing_ok=True, context=(("catalog_name", catalog),))


def split_scope(scope: str) -> tuple[str, str]:
    """``'cat.schema'`` -> ``('cat', 'schema')`` (catalog names never contain dots)."""
    cat, _, sch = scope.partition(".")
    return cat, sch


def drain(client: UnityCatalogClient, listing: Listing) -> list[dict[str, Any]]:
    """Read every page of a listing (used to resolve fan-out ids, which are small)."""
    out: list[dict[str, Any]] = []
    token = ""
    while True:
        items, token = ApiScan.fetch(client, listing, token)
        out.extend(items)
        if not token:
            return out


def catalog_names(client: UnityCatalogClient, pd: Pushdown, column: str = "catalog_name") -> list[str]:
    """Catalogs in scope: the pushed-down ``=`` / ``IN`` values, else every catalog."""
    wanted = pd.strs(column)
    if wanted is not None:
        return wanted
    return sorted(str(c["name"]) for c in drain(client, catalogs_listing(client.config)) if c.get("name"))


def schema_scope(
    client: UnityCatalogClient, pd: Pushdown, catalog_cols: tuple[str, ...], schema_cols: tuple[str, ...]
) -> list[str]:
    """``catalog.schema`` pairs in scope, from pushed-down catalog / schema names or by listing them."""
    catalogs = pd.strs(*catalog_cols)
    if catalogs is None:
        catalogs = sorted(str(c["name"]) for c in drain(client, catalogs_listing(client.config)) if c.get("name"))
    schemas = pd.strs(*schema_cols)
    out: list[str] = []
    for cat in catalogs:
        if schemas is not None:
            out.extend(f"{cat}.{s}" for s in schemas)
            continue
        out.extend(f"{cat}.{s['name']}" for s in drain(client, schemas_listing(client.config, cat)) if s.get("name"))
    return out


def names_from_full(values: list[str] | None, parts: int) -> list[str] | None:
    """Keep the full names with exactly ``parts`` dotted parts (lower-cased qualified names work too)."""
    if values is None:
        return None
    return [v for v in values if len(v.split(".")) == parts]


# --------------------------------------------------------------------------- base


@init_single_worker
class ApiScan(TableFunctionGenerator[NoArgs, ScanState]):
    """Base class for tables that page through one or more Unity Catalog listings."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([])
    #: Set for tables that only Databricks implements; used in the "not supported" error.
    DATABRICKS_ONLY: ClassVar[str | None] = None

    class Meta:
        """Overridden by every concrete table."""

        name = "api_scan"
        description = "Base class for Unity Catalog API scans"

    @classmethod
    def on_bind(
        cls,
        params: BindParams[NoArgs],
        *,
        unity_catalog: Annotated[dict[str, pa.Scalar[Any]] | None, Secret(SECRET_TYPE)] = None,
    ) -> BindResponse:
        """Declare the fixed schema and ask DuckDB for the ``unity_catalog`` secret."""
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[NoArgs]) -> ScanState:
        """Start at the first listing's first page."""
        return ScanState()

    @classmethod
    def fanout(cls, client: UnityCatalogClient, pd: Pushdown) -> list[str] | None:
        """Ids to repeat the listing for (resolved once per scan); ``None`` when not needed."""
        return None

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str], cfg: UnityCatalogConfig, wanted: set[str]) -> list[Listing]:
        """Return the listings to read for these pushed-down predicates and projected columns."""
        raise NotImplementedError

    @classmethod
    def rows(
        cls, client: UnityCatalogClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map one page of API items to rows (dicts keyed by column name)."""
        raise NotImplementedError

    @classmethod
    def emit(cls, params: ProcessParams[Any], out: OutputCollector, rows: list[dict[str, Any]]) -> None:
        """Emit rows as one batch in the bound (projected) schema."""
        schema = params.output_schema
        columns = {f.name: [row.get(f.name) for row in rows] for f in schema}
        out.emit(pa.RecordBatch.from_pydict(columns, schema=schema))

    @classmethod
    def fetch(cls, client: UnityCatalogClient, listing: Listing, token: str) -> tuple[list[dict[str, Any]], str]:
        """Read one page of a listing; return its items and the next page token ('' when done)."""
        params: dict[str, Any] = dict(listing.query)
        if listing.paged:
            params[listing.size_param] = min(client.config.page_size, listing.max_size)
            if token:
                params["page_token"] = token
        try:
            body = client.get(listing.path, params, dict(listing.body) if listing.body is not None else None)
        except EndpointMissingError:
            if listing.fallback is not None:
                return cls.fetch(client, listing.fallback, token)
            if listing.optional:
                return [], ""
            if cls.DATABRICKS_ONLY:
                raise UnityCatalogError(
                    f"{cls.Meta.name.removesuffix('_scan')} needs {cls.DATABRICKS_ONLY}, which this Unity Catalog "
                    "server does not implement (open-source Unity Catalog has no such API); it works against "
                    "Databricks."
                ) from None
            raise
        except NotFoundError:
            if not listing.missing_ok:
                raise
            return [], ""
        if listing.key is None:
            return ([body] if isinstance(body, dict) and body else []), ""
        items = list(body.get(listing.key) or []) if isinstance(body, dict) else []
        nxt = str(body.get("next_page_token") or "") if isinstance(body, dict) and listing.paged else ""
        if nxt == token:
            nxt = ""
        return items, nxt

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: ScanState, out: OutputCollector) -> None:
        """Read one page of the current listing, emit it, and advance the cursor."""
        client = client_from(params)
        pd = Pushdown(params.current_pushdown_filters)
        if not state.resolved:
            state.resolved = True
            ids = cls.fanout(client, pd)
            state.ids = list(ids) if ids is not None else []
        wanted = set(params.output_schema.names)
        listings = cls.requests(pd, state.ids, client.config, wanted)
        while state.listing < len(listings):
            listing = listings[state.listing]
            items, nxt = cls.fetch(client, listing, state.token)
            if nxt:
                state.token = nxt
            else:
                state.listing, state.token = state.listing + 1, ""
            if items:
                rows = cls.rows(client, items, wanted, listing)
                if rows:
                    cls.emit(params, out, rows)
                    return
        out.finish()


def common(
    client: UnityCatalogClient,
    *,
    id_: str,
    qualified: str | None,
    description: Any,
    item: dict[str, Any],
    ui: str | None,
) -> dict[str, Any]:
    """Return the shared data-catalog columns (catalog_tool, id, platform, qualified_name, description, times, url)."""
    return {
        "catalog_tool": CATALOG_TOOL,
        "id": id_,
        "platform": "databricks" if client.config.is_databricks else "unity_catalog",
        "qualified_name": lower(qualified),
        "description": description or None,
        "created_at": ts(item.get("created_at")),
        "updated_at": ts(item.get("updated_at")),
        "url": client.ui_url(ui) if ui else None,
    }


def explore_path(*parts: str) -> str:
    """Catalog Explorer path in the Databricks UI for a catalog / schema / table."""
    return "/explore/data/" + "/".join(quote(p) for p in parts)
