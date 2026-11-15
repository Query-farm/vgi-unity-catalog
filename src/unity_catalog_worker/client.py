"""HTTP client for the Unity Catalog REST API (open-source Unity Catalog and Databricks).

Everything that touches the network lives here:

* **Configuration** comes from a DuckDB secret of type ``unity_catalog`` (see
  ``UNITY_CATALOG_SECRET``). The field names match the ``databricks`` secret of
  vgi-databricks, so the same ``HOST`` / ``TOKEN`` / ``CLIENT_ID`` /
  ``CLIENT_SECRET`` / ``AZURE_TOKEN`` values work for both workers. Nothing is
  read from the environment.
* **Authentication**, one of:

  - a bearer token (``TOKEN``): a Databricks personal access token, or an
    open-source Unity Catalog access token;
  - OAuth machine-to-machine (``CLIENT_ID`` / ``CLIENT_SECRET`` of a Databricks
    service principal), exchanged at ``<HOST>/oidc/v1/token`` and cached per
    process until shortly before it expires, refreshed on a 401;
  - an Azure AD (Entra ID) access token (``AZURE_TOKEN``) for Azure Databricks;
  - nothing at all, for an open-source server running without authorization.

* **Politeness**: every request has a timeout; 429 and 502/503/504 are retried
  with backoff that honours ``Retry-After``.
* **Feature detection**: an endpoint the server does not implement (open-source
  Unity Catalog has no lineage, tags, connections or Delta Sharing) raises
  ``EndpointMissingError``, which tables turn into a clear message or skip.

Credentials are never written to table-function state or logs: each ``process``
call rebuilds the client from the secret the SDK hands it.
"""

from __future__ import annotations

import base64
import datetime as dt
import email.utils
import hashlib
import json
import logging
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
from vgi.catalog import SecretTypeSpec

log = logging.getLogger(__name__)

SECRET_TYPE = "unity_catalog"
UC_API = "/api/2.1/unity-catalog"
USER_AGENT = "vgi-unity-catalog-worker/0.1"
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 1000
MAX_ATTEMPTS = 5
MAX_BACKOFF_SECONDS = 60.0
RETRY_STATUSES = frozenset({429, 502, 503, 504})
DOC_URL = "https://github.com/Query-farm/vgi-unity-catalog#credentials"
AUTH_TYPES = ("auto", "pat", "oauth-m2m", "azure", "none")
FLAVORS = ("auto", "databricks", "oss")
DATABRICKS_HOST_SUFFIXES = (".cloud.databricks.com", ".azuredatabricks.net", ".gcp.databricks.com", ".databricks.com")


def _field(
    name: str, doc: str, type_: pa.DataType | None = None, *, redact: bool = False, required: bool = False
) -> Any:
    meta = {"vgi_doc": doc}
    if redact:
        meta["redact"] = "true"
    if required:
        meta["vgi_required"] = "true"
    return pa.field(name, type_ or pa.string(), metadata=meta)


UNITY_CATALOG_SECRET = SecretTypeSpec(
    name=SECRET_TYPE,
    description=(
        "Connection to a Unity Catalog server: an open-source Unity Catalog server URL, or a Databricks workspace "
        "URL plus a personal access token, an OAuth service principal (client id + secret) or an Azure AD token."
    ),
    schema=pa.schema(
        [
            _field(
                "host",
                "Server URL: open-source Unity Catalog, e.g. 'http://localhost:8080', or a Databricks workspace, "
                "e.g. 'https://dbc-a1b2c3d4-e5f6.cloud.databricks.com'. A bare host name gets https://.",
                required=True,
            ),
            _field(
                "token",
                "Bearer token: a Databricks personal access token (dapi...) or an open-source Unity Catalog token.",
                redact=True,
            ),
            _field("client_id", "Databricks OAuth M2M: the service principal's application (client) id."),
            _field(
                "client_secret",
                "Databricks OAuth M2M: the service principal's OAuth secret, exchanged at <host>/oidc/v1/token.",
                redact=True,
            ),
            _field(
                "azure_token", "An Azure AD (Entra ID) access token for the Azure Databricks resource.", redact=True
            ),
            _field(
                "auth_type",
                "'auto' (default: token, then client_id/client_secret, then azure_token, else no authentication), "
                "'pat', 'oauth-m2m', 'azure' or 'none'.",
            ),
            _field(
                "flavor",
                "'auto' (default: Databricks for *.databricks.com / *.azuredatabricks.net hosts, else open source), "
                "'databricks' or 'oss'. Sets the platform column and the UI links in the url column.",
            ),
            _field(
                "include_browse",
                "Databricks: also list catalogs, schemas and tables the principal can only browse (include_browse).",
                pa.bool_(),
            ),
            _field(
                "page_size",
                "Items requested per API page (max_results), capped at each endpoint's maximum.",
                pa.int64(),
            ),
            _field("timeout", "Per-request timeout in seconds (default 30).", pa.float64()),
        ],
        metadata={"vgi_doc_url": DOC_URL},
    ),
)


class UnityCatalogError(RuntimeError):
    """The Unity Catalog API returned an error, or the secret is unusable."""


class NotFoundError(UnityCatalogError):
    """The API reported that the requested object does not exist."""


class EndpointMissingError(UnityCatalogError):
    """The server does not implement this endpoint (e.g. open-source Unity Catalog and lineage)."""


@dataclass(frozen=True, slots=True)
class UnityCatalogConfig:
    """Connection settings parsed from a ``unity_catalog`` secret."""

    host: str
    token: str | None = field(default=None, repr=False)
    client_id: str | None = None
    client_secret: str | None = field(default=None, repr=False)
    azure_token: str | None = field(default=None, repr=False)
    auth_type: str = "none"
    flavor: str = "oss"
    include_browse: bool = False
    page_size: int = DEFAULT_PAGE_SIZE
    timeout: float = 30.0

    @property
    def is_databricks(self) -> bool:
        """True when the server is a Databricks workspace."""
        return self.flavor == "databricks"

    @classmethod
    def from_secret(cls, secret: Mapping[str, Any] | None) -> UnityCatalogConfig:
        """Build and validate a config from a resolved secret's key/value pairs.

        Args:
            secret: Secret fields; values may be ``pa.Scalar`` or plain Python.

        Returns:
            The validated configuration, with ``auth_type`` and ``flavor`` resolved from ``auto``.

        Raises:
            UnityCatalogError: No secret, or an invalid field value.

        """
        if not secret:
            raise UnityCatalogError(
                "no Unity Catalog connection: run CREATE SECRET (TYPE unity_catalog, HOST 'http://localhost:8080') "
                "for an open-source server, or (TYPE unity_catalog, HOST 'https://<workspace>', TOKEN 'dapi...') "
                f"for Databricks, after ATTACH. See {DOC_URL}"
            )

        def get(name: str) -> Any:
            value = secret.get(name)
            if isinstance(value, pa.Scalar):
                value = value.as_py()
            return None if value == "" else value

        host = get("host")
        if not host:
            raise UnityCatalogError("unity_catalog secret: HOST must be set to the server or workspace URL")
        host = str(host).strip().rstrip("/").removesuffix(UC_API)
        if "://" not in host:
            host = f"https://{host}"
        if not host.startswith(("http://", "https://")):
            raise UnityCatalogError("unity_catalog secret: HOST must be an http:// or https:// URL")
        token, client_id, client_secret, azure_token = (
            get("token"),
            get("client_id"),
            get("client_secret"),
            get("azure_token"),
        )
        auth = str(get("auth_type") or "auto").lower()
        if auth not in AUTH_TYPES:
            raise UnityCatalogError(
                f"unity_catalog secret: AUTH_TYPE must be one of {', '.join(AUTH_TYPES)}, not {auth!r}"
            )
        if auth == "auto":
            if token:
                auth = "pat"
            elif client_id and client_secret:
                auth = "oauth-m2m"
            elif azure_token:
                auth = "azure"
            else:
                auth = "none"
        if auth == "pat" and not token:
            raise UnityCatalogError("unity_catalog secret: AUTH_TYPE 'pat' needs TOKEN")
        if auth == "oauth-m2m" and not (client_id and client_secret):
            raise UnityCatalogError("unity_catalog secret: AUTH_TYPE 'oauth-m2m' needs CLIENT_ID and CLIENT_SECRET")
        if auth == "azure" and not azure_token:
            raise UnityCatalogError("unity_catalog secret: AUTH_TYPE 'azure' needs AZURE_TOKEN")
        flavor = str(get("flavor") or "auto").lower()
        if flavor not in FLAVORS:
            raise UnityCatalogError(f"unity_catalog secret: FLAVOR must be one of {', '.join(FLAVORS)}, not {flavor!r}")
        if flavor == "auto":
            hostname = (urllib.parse.urlsplit(host).hostname or "").lower()
            flavor = "databricks" if hostname.endswith(DATABRICKS_HOST_SUFFIXES) else "oss"
        try:
            raw_size, raw_timeout = get("page_size"), get("timeout")
            page_size = int(raw_size) if raw_size is not None else DEFAULT_PAGE_SIZE
            timeout = float(raw_timeout) if raw_timeout is not None else 30.0
        except (TypeError, ValueError) as err:
            raise UnityCatalogError("unity_catalog secret: PAGE_SIZE and TIMEOUT must be numbers") from err
        if not 1 <= page_size <= MAX_PAGE_SIZE:
            raise UnityCatalogError(f"unity_catalog secret: PAGE_SIZE must be between 1 and {MAX_PAGE_SIZE}")
        if timeout <= 0:
            raise UnityCatalogError("unity_catalog secret: TIMEOUT must be positive")
        browse = get("include_browse")
        if isinstance(browse, str):
            browse = browse.strip().lower() in ("true", "1", "yes")
        return cls(
            host=host,
            token=str(token) if token else None,
            client_id=str(client_id) if client_id else None,
            client_secret=str(client_secret) if client_secret else None,
            azure_token=str(azure_token) if azure_token else None,
            auth_type=auth,
            flavor=flavor,
            include_browse=bool(browse),
            page_size=page_size,
            timeout=timeout,
        )


# Process-wide OAuth token cache. Keys hold a hash of the client secret, never the secret.
_LOCK = threading.Lock()
_TOKEN_CACHE: dict[tuple[str, str, str], tuple[str, float]] = {}


def clear_caches() -> None:
    """Forget exchanged OAuth tokens (used by tests)."""
    with _LOCK:
        _TOKEN_CACHE.clear()


def retry_delay(header: str | None, attempt: int) -> float:
    """Seconds to wait before retry ``attempt`` given a ``Retry-After`` header value."""
    if header:
        header = header.strip()
        if header.replace(".", "", 1).isdigit():
            return min(float(header), MAX_BACKOFF_SECONDS)
        try:
            when = email.utils.parsedate_to_datetime(header)
            return max(0.0, min((when - dt.datetime.now(dt.UTC)).total_seconds(), MAX_BACKOFF_SECONDS))
        except (TypeError, ValueError):
            pass
    return min(0.5 * float(2**attempt), MAX_BACKOFF_SECONDS)


def _error_text(body: Any) -> str:
    """Unity Catalog errors are ``{"error_code": ..., "message": ...}``."""
    if isinstance(body, dict):
        code, msg = body.get("error_code"), body.get("message")
        if code or msg:
            return f"{code or 'ERROR'}: {msg or ''}".strip()
    return str(body)[:800]


def endpoint_missing(status: int, body: Any) -> bool:
    """Tell "this server has no such endpoint" apart from "no such object".

    Open-source Unity Catalog (Armeria) answers an unknown route with a plain-text 404
    (or, with authorization enabled, a 500 "Couldn't unwrap service."); Databricks answers
    ``ENDPOINT_NOT_FOUND`` / ``FEATURE_DISABLED`` / ``NOT_IMPLEMENTED``.
    """
    if status in (404, 405, 501) and not isinstance(body, dict):
        return True
    if isinstance(body, dict):
        code = str(body.get("error_code") or "")
        # With authorization enabled, open-source Unity Catalog's auth decorator answers an unknown
        # route with HTTP 500 "Couldn't unwrap service." before routing.
        if status == 500 and "unwrap service" in str(body.get("message") or "").lower():
            return True
        return code in {"ENDPOINT_NOT_FOUND", "FEATURE_DISABLED", "NOT_IMPLEMENTED"} or status == 501
    return False


class UnityCatalogClient:
    """Authenticated JSON requests against one Unity Catalog server."""

    def __init__(self, config: UnityCatalogConfig) -> None:
        """Bind to a configuration; OAuth tokens are fetched lazily."""
        self.config = config

    # ------------------------------------------------------------------ plumbing

    def _open(self, request: urllib.request.Request) -> tuple[int, Any]:
        """Send one request with retries; return (status, decoded JSON body or text)."""
        url = request.full_url.split("?", 1)[0]
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                with urllib.request.urlopen(request, timeout=self.config.timeout) as resp:
                    raw = resp.read()
                    try:
                        return resp.status, json.loads(raw) if raw else {}
                    except ValueError:
                        return resp.status, raw.decode(errors="replace")
            except urllib.error.HTTPError as err:
                if err.code in RETRY_STATUSES and attempt < MAX_ATTEMPTS:
                    delay = retry_delay(err.headers.get("Retry-After"), attempt)
                    log.info("unity_catalog: HTTP %s from %s; retrying in %.1fs", err.code, url, delay)
                    time.sleep(delay)
                    continue
                text = err.read(4000).decode(errors="replace")
                try:
                    return err.code, json.loads(text)
                except ValueError:
                    return err.code, text
            except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
                if attempt == MAX_ATTEMPTS:
                    reason = getattr(err, "reason", err)
                    raise UnityCatalogError(f"{request.get_method()} {url} failed: {reason}") from err
                time.sleep(min(0.5 * 2**attempt, MAX_BACKOFF_SECONDS))
        raise UnityCatalogError(f"{request.get_method()} {url}: gave up after {MAX_ATTEMPTS} attempts")

    def _oauth_token(self, *, refresh: bool) -> str:
        cfg = self.config
        assert cfg.client_id is not None and cfg.client_secret is not None
        key = (cfg.host, cfg.client_id, hashlib.sha256(cfg.client_secret.encode()).hexdigest())
        with _LOCK:
            hit = _TOKEN_CACHE.get(key)
        if hit and not refresh and hit[1] - 60 > time.time():
            return hit[0]
        basic = base64.b64encode(f"{cfg.client_id}:{cfg.client_secret}".encode()).decode()
        req = urllib.request.Request(
            f"{cfg.host}/oidc/v1/token",
            data=urllib.parse.urlencode({"grant_type": "client_credentials", "scope": "all-apis"}).encode(),
            headers={
                "Authorization": f"Basic {basic}",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
                "User-Agent": USER_AGENT,
            },
            method="POST",
        )
        status, body = self._open(req)
        if status != 200 or not isinstance(body, dict) or "access_token" not in body:
            detail = (body.get("error_description") or body.get("error")) if isinstance(body, dict) else body
            raise UnityCatalogError(
                f"OAuth token request to {cfg.host}/oidc/v1/token failed with HTTP {status}: {detail}"
            )
        token = str(body["access_token"])
        expires = time.time() + float(body.get("expires_in") or 3600)
        with _LOCK:
            _TOKEN_CACHE[key] = (token, expires)
        return token

    def _auth_header(self, *, refresh: bool = False) -> str | None:
        cfg = self.config
        if cfg.auth_type == "pat":
            return f"Bearer {cfg.token}"
        if cfg.auth_type == "azure":
            return f"Bearer {cfg.azure_token}"
        if cfg.auth_type == "oauth-m2m":
            return f"Bearer {self._oauth_token(refresh=refresh)}"
        return None

    def _call(self, method: str, url: str, body: bytes | None) -> tuple[int, Any]:
        headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
        if body is not None:
            headers["Content-Type"] = "application/json"

        def send(refresh: bool) -> tuple[int, Any]:
            auth = self._auth_header(refresh=refresh)
            req = urllib.request.Request(
                url, data=body, headers={**headers, **({"Authorization": auth} if auth else {})}, method=method
            )
            return self._open(req)

        status, payload = send(False)
        if status == 401 and self.config.auth_type == "oauth-m2m":
            status, payload = send(True)
        return status, payload

    def _check(self, method: str, url: str, status: int, payload: Any) -> Any:
        if 200 <= status < 300:
            return payload
        text = _error_text(payload)
        shown = url.split("?", 1)[0]
        if endpoint_missing(status, payload):
            raise EndpointMissingError(f"{method} {shown}: this Unity Catalog server does not implement the endpoint")
        if status == 404 or "NOT_FOUND" in text or "DOES_NOT_EXIST" in text:
            raise NotFoundError(f"{method} {shown} failed with HTTP {status}: {text}")
        if status in (401, 403):
            raise UnityCatalogError(
                f"{method} {shown} failed with HTTP {status} (check the unity_catalog secret's credentials and the "
                f"principal's privileges): {text}"
            )
        raise UnityCatalogError(f"{method} {shown} failed with HTTP {status}: {text}")

    # ------------------------------------------------------------------ public API

    def get(self, path: str, query: Mapping[str, Any] | None = None, body: Mapping[str, Any] | None = None) -> Any:
        """GET ``<host><path>`` and return the decoded JSON body.

        Args:
            path: Absolute API path, e.g. ``/api/2.1/unity-catalog/catalogs``.
            query: Query parameters; list values repeat the parameter, ``None`` values are dropped,
                booleans are sent as ``true`` / ``false``.
            body: Optional JSON body (the Data Lineage API documents its parameters as a GET body).

        Returns:
            The decoded JSON response.

        """
        clean: dict[str, Any] = {}
        for k, v in (query or {}).items():
            if v is None:
                continue
            clean[k] = ("true" if v else "false") if isinstance(v, bool) else v
        qs = urllib.parse.urlencode(clean, doseq=True)
        url = f"{self.config.host}{path}" + (f"?{qs}" if qs else "")
        data = json.dumps(dict(body), sort_keys=True).encode() if body is not None else None
        status, payload = self._call("GET", url, data)
        return self._check("GET", url, status, payload)

    def ui_url(self, path: str) -> str | None:
        """Absolute URL of a page in the Databricks workspace UI (None for open-source servers)."""
        if not self.config.is_databricks:
            return None
        return f"{self.config.host}{path}"


def quote(value: str) -> str:
    """Quote one path segment (full names keep their dots)."""
    return urllib.parse.quote(value, safe=".")
