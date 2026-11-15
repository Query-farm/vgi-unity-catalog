"""Assemble the ``unity_catalog`` catalog and run the worker.

Run it:

    vgi-unity-catalog-worker                      # stdio: what DuckDB launches by default
    vgi-unity-catalog-worker --http --port 8000   # HTTP: for Docker / hosted deploys

Attach it from DuckDB, then point it at a server:

    ATTACH 'unity_catalog' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-unity-catalog-worker');
    CREATE SECRET (TYPE unity_catalog, HOST 'http://localhost:8080');
    SELECT id, table_type FROM unity_catalog.main.datasets WHERE catalog_name = 'unity';
"""

from __future__ import annotations

import json
from typing import Any

from vgi import Worker
from vgi.catalog import Catalog, Schema, Table
from vgi.catalog.constraints import ReferencedKey, constraint_record

from .assets import (
    Catalogs,
    Columns,
    Datasets,
    Functions,
    Metastores,
    ModelVersions,
    RegisteredModels,
    Schemas,
    Views,
    Volumes,
)
from .client import UNITY_CATALOG_SECRET
from .governance import (
    Connections,
    EffectiveGrants,
    ExternalLocations,
    Grants,
    LineageEdges,
    Owners,
    Recipients,
    Shares,
    StorageCredentials,
    TagAssignments,
    Tags,
)
from .meta import (
    CATALOG_COMMENT,
    CATALOG_NAME,
    CATALOG_TAGS,
    SCHEMA_CATEGORIES,
    SCHEMA_DOC_LLM,
    SCHEMA_DOC_MD,
    SOURCE_URL,
    TABLE_DOCS,
    keywords_array,
    object_tags,
)
from .scalars import SCALARS

Q = f"{CATALOG_NAME}.main"

EXAMPLES: dict[str, list[tuple[str, str]]] = {
    "metastores": [("The metastore id and region", f"SELECT metastore_id, cloud, region FROM {Q}.metastores")],
    "catalogs": [
        ("Catalogs by type and owner", f"SELECT name, catalog_type, owner FROM {Q}.catalogs ORDER BY name"),
    ],
    "schemas": [
        (
            "Schemas of one catalog (catalog_name is sent to the API)",
            f"SELECT name, owner, description FROM {Q}.schemas WHERE catalog_name = 'sales' ORDER BY name",
        ),
    ],
    "datasets": [
        (
            "Tables per type and format in one catalog (only that catalog's schemas are listed)",
            f"SELECT table_type, data_source_format, count(*) AS tables FROM {Q}.datasets "
            "WHERE catalog_name = 'sales' GROUP BY ALL ORDER BY tables DESC",
        ),
        (
            "External tables without a comment",
            f"SELECT id, storage_location FROM {Q}.datasets WHERE table_type = 'EXTERNAL' AND description IS NULL "
            "ORDER BY id",
        ),
    ],
    "views": [
        (
            "Views that read a given table",
            f"SELECT id FROM {Q}.views WHERE view_definition ILIKE '%sales.raw.orders%' ORDER BY id",
        ),
    ],
    "columns": [
        (
            "Partition columns of one table (fetched with one GET /tables call)",
            f"SELECT column_name, native_data_type, partition_index FROM {Q}.columns "
            "WHERE dataset_id = 'sales.raw.orders' AND partition_index IS NOT NULL ORDER BY partition_index",
        ),
        (
            "Columns whose comment mentions PII",
            f"SELECT dataset_id, column_name FROM {Q}.columns WHERE catalog_name = 'sales' "
            "AND description ILIKE '%pii%' ORDER BY ALL",
        ),
    ],
    "volumes": [
        (
            "External volumes and where their files live",
            f"SELECT id, storage_location FROM {Q}.volumes WHERE volume_type = 'EXTERNAL' ORDER BY id",
        ),
    ],
    "functions": [
        (
            "SQL functions and their parameter counts",
            f"SELECT id, len(parameters) AS params, full_data_type FROM {Q}.functions "
            "WHERE routine_body = 'SQL' ORDER BY id",
        ),
    ],
    "registered_models": [
        (
            "Registered models per catalog",
            f"SELECT catalog_name, count(*) AS models FROM {Q}.registered_models GROUP BY ALL ORDER BY ALL",
        ),
    ],
    "model_versions": [
        (
            "Latest ready version of each model",
            f"SELECT model_id, max(version) AS latest FROM {Q}.model_versions WHERE status = 'READY' "
            "GROUP BY ALL ORDER BY model_id",
        ),
    ],
    "external_locations": [
        (
            "External locations and their credentials",
            f"SELECT name, url, credential_name FROM {Q}.external_locations ORDER BY name",
        ),
    ],
    "storage_credentials": [
        (
            "Storage credentials by kind",
            f"SELECT credential_type, count(*) AS credentials FROM {Q}.storage_credentials GROUP BY ALL ORDER BY ALL",
        ),
    ],
    "connections": [
        (
            "Federated connections and their hosts",
            f"SELECT name, connection_type, options['host'] AS host FROM {Q}.connections ORDER BY name",
        ),
    ],
    "shares": [("Delta Sharing shares and owners", f"SELECT name, owner, created_at FROM {Q}.shares ORDER BY name")],
    "recipients": [
        (
            "Recipients that have not activated",
            f"SELECT name, authentication_type FROM {Q}.recipients WHERE NOT activated ORDER BY name",
        ),
    ],
    "grants": [
        (
            "Who can SELECT from tables in one catalog",
            f"SELECT securable_name, principal FROM {Q}.grants WHERE securable_type = 'table' "
            "AND catalog_name = 'sales' AND privilege = 'SELECT' ORDER BY ALL",
        ),
        (
            "Privileges held by one user across the metastore",
            f"SELECT securable_type, securable_name, privilege FROM {Q}.grants "
            "WHERE principal = 'alice@example.com' ORDER BY ALL",
        ),
    ],
    "effective_grants": [
        (
            "Inherited privileges on one table",
            f"SELECT principal, privilege, inherited_from_type, inherited_from_name FROM {Q}.effective_grants "
            "WHERE securable_type = 'table' AND securable_name = 'sales.raw.orders' ORDER BY ALL",
        ),
    ],
    "owners": [
        (
            "Objects owned per principal",
            f"SELECT owner_id, entity_type, count(*) AS objects FROM {Q}.owners GROUP BY ALL ORDER BY objects DESC",
        ),
    ],
    "tags": [("Governed tags and their allowed values", f"SELECT name, allowed_values FROM {Q}.tags ORDER BY name")],
    "tag_assignments": [
        (
            "Columns tagged as PII in one catalog",
            f"SELECT entity_id, column_name, tag_value FROM {Q}.tag_assignments WHERE entity_type = 'column' "
            "AND catalog_name = 'sales' AND tag_key = 'pii' ORDER BY ALL",
        ),
    ],
    "lineage_edges": [
        (
            "What feeds one table (only that table's lineage is requested)",
            f"SELECT upstream_id, upstream_type FROM {Q}.lineage_edges WHERE level = 'dataset' "
            "AND downstream_id = 'sales.marts.daily_revenue' ORDER BY ALL",
        ),
        (
            "Column-level impact of changing one table",
            f"SELECT upstream_column, downstream_id, downstream_column FROM {Q}.lineage_edges "
            "WHERE level = 'column' AND upstream_id = 'sales.raw.orders' ORDER BY ALL",
        ),
    ],
}


def _examples_json(name: str) -> str:
    return json.dumps([{"description": d, "sql": s} for d, s in EXAMPLES[name]])


FK = tuple[tuple[list[str], str, list[str]], ...]


def _table(name: str, scan: type[Any], key: list[str], fks: FK = ()) -> Table:
    """Build a catalog table over ``scan`` with NOT NULL, primary key and foreign key records.

    Each foreign key is ``(columns, referenced table, referenced primary key)``. Only references every row
    satisfies by construction are declared (child rows are read from their parent's listing).
    """
    doc = TABLE_DOCS[name]
    schema = ["main"]
    constraints = [
        constraint_record(
            schema,
            name,
            f"{name}_{f.name}_not_null",
            "NOT_NULL",
            [[f.name]],
            enforcement="ENFORCED",
            validation="ALL_ROWS",
        )
        for f in scan.FIXED_SCHEMA
        if not f.nullable
    ]
    if key:
        constraints.append(
            constraint_record(
                schema,
                name,
                f"{name}_pk",
                "PRIMARY_KEY",
                [[c] for c in key],
                enforcement="ENFORCED",
                validation="ALL_ROWS",
            )
        )
    for cols, ref_table, ref_cols in fks:
        constraints.append(
            constraint_record(
                schema,
                name,
                f"{name}_{ref_table}_fk",
                "FOREIGN_KEY",
                [[c] for c in cols],
                enforcement="ENFORCED",
                validation="ALL_ROWS",
                references=ReferencedKey(
                    schema_path=schema,
                    table=ref_table,
                    columns=[[c] for c in ref_cols],
                    constraint_id=f"{ref_table}_pk",
                ),
            )
        )
    return Table(
        name=name,
        function=scan,
        comment=doc["comment"],
        constraints=tuple(constraints),
        column_comments={f.name: (f.metadata or {})[b"comment"].decode() for f in scan.FIXED_SCHEMA},
        tags={
            **object_tags(
                title=doc["title"],
                category=doc["category"],
                keywords=doc["keywords"],
                doc_llm=doc["doc_llm"],
                doc_md=doc["doc_md"],
            ),
            "domain": "data catalog",
            "vgi.example_queries": _examples_json(name),
        },
    )


MS = ((["metastore_id"], "metastores", ["metastore_id"]),)

TABLES = [
    _table("metastores", Metastores, ["metastore_id"]),
    _table("catalogs", Catalogs, ["id"], MS),
    _table("schemas", Schemas, ["id"], MS),
    _table("datasets", Datasets, ["id"], MS),
    _table("views", Views, ["id"]),
    _table("columns", Columns, ["dataset_id", "column_name"], ((["dataset_id"], "datasets", ["id"]),)),
    _table("volumes", Volumes, ["id"], MS),
    _table("functions", Functions, ["id"]),
    _table("registered_models", RegisteredModels, ["id"], MS),
    _table("model_versions", ModelVersions, ["model_id", "version"], ((["model_id"], "registered_models", ["id"]),)),
    _table("external_locations", ExternalLocations, ["id"]),
    _table("storage_credentials", StorageCredentials, ["id"]),
    _table("connections", Connections, ["id"]),
    _table("shares", Shares, ["id"]),
    _table("recipients", Recipients, ["id"]),
    _table("grants", Grants, ["securable_type", "securable_name", "principal", "privilege"]),
    _table("effective_grants", EffectiveGrants, []),
    _table("owners", Owners, ["entity_type", "entity_id"]),
    _table("tags", Tags, ["id"]),
    _table("tag_assignments", TagAssignments, []),
    _table("lineage_edges", LineageEdges, []),
]

for _fn in SCALARS:
    _fn.Meta.tags = {
        **_fn.Meta.tags,
        "domain": "data catalog",
        "vgi.example_queries": json.dumps([{"description": e.description, "sql": e.sql} for e in _fn.Meta.examples]),
    }


class UnityCatalogWorker(Worker):
    """The worker process: the ``unity_catalog`` catalog with one ``main`` schema."""

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_COMMENT,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment="Unity Catalog securables, grants, owners, tags and lineage as tables.",
                tables=TABLES,
                functions=list(SCALARS),
                tags={
                    "vgi.title": "Unity Catalog Metadata",
                    "domain": "data catalog",
                    "vgi.keywords": keywords_array(
                        "catalogs, schemas, tables, columns, volumes, functions, models, grants, owners, tags, lineage"
                    ),
                    "vgi.doc_llm": SCHEMA_DOC_LLM,
                    "vgi.doc_md": SCHEMA_DOC_MD,
                    "vgi.categories": json.dumps(SCHEMA_CATEGORIES),
                    "vgi.example_queries": json.dumps(
                        [{"description": d, "sql": s} for t in TABLES for d, s in EXAMPLES[t.name][:1]]
                        + [{"description": e.description, "sql": e.sql} for f in SCALARS for e in f.Meta.examples]
                    ),
                },
            )
        ],
    )

    secret_types = [UNITY_CATALOG_SECRET]


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    UnityCatalogWorker.main()


if __name__ == "__main__":
    main()
