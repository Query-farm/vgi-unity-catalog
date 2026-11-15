"""Offline name scalars: normalize Unity Catalog names and privileges without calling the server.

They apply the same rules as the tables, so ``uc_qualified_name(name)`` equals
``datasets.qualified_name`` and ``uc_privilege(p)`` equals ``grants.privilege``.
"""

from __future__ import annotations

import json
from typing import Annotated, Any

import pyarrow as pa
from vgi.arguments import Param, Returns
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction

from .meta import object_tags
from .scan import normalize_privilege


def qualified_name(name: str | None) -> str | None:
    """Lower-cased dotted name with backtick quoting removed: '`Sales`.Raw.`Orders`' -> 'sales.raw.orders'."""
    if name is None:
        return None
    parts: list[str] = []
    buf: list[str] = []
    quoted = False
    i = 0
    while i < len(name):
        ch = name[i]
        if ch == "`":
            if quoted and i + 1 < len(name) and name[i + 1] == "`":
                buf.append("`")
                i += 2
                continue
            quoted = not quoted
        elif ch == "." and not quoted:
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
        i += 1
    parts.append("".join(buf).strip())
    text = ".".join(parts).lower()
    return text or None


class UcQualifiedName(ScalarFunction):
    """``uc_qualified_name(name)``."""

    class Meta:
        """Function metadata."""

        name = "uc_qualified_name"
        description = "The lower-cased catalog.schema.table join key of a Unity Catalog full name"
        tags = {
            **object_tags(
                title="UC Qualified Name",
                category="assets",
                keywords="qualified name, full name, join key, normalize, backticks, three-part name",
                doc_llm=(
                    "Normalize a Unity Catalog / Databricks SQL object name ('Sales.RAW.orders', "
                    "'`sales`.`raw`.`orders`') to the lower-cased 'catalog.schema.table' join key used by "
                    "datasets.qualified_name and the DataHub / OpenMetadata / dbt / OpenLineage workers. Backtick "
                    "quoting is removed (a doubled backtick is a literal one). Runs locally: no server call, no "
                    "secret. NULL gives NULL."
                ),
                doc_md=(
                    "## uc_qualified_name(name)\n\n"
                    "The lower-cased `catalog.schema.table` of a (possibly quoted) Unity Catalog name.\n\n"
                    "Offline; NULL in, NULL out."
                ),
            ),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "Join keys of a mixed-case and a backtick-quoted three-part name",
                        "sql": (
                            "SELECT unity_catalog.main.uc_qualified_name('Sales.RAW.Orders') AS plain, "
                            "unity_catalog.main.uc_qualified_name('`sales`.`raw`.`daily revenue`') AS quoted"
                        ),
                        "expected_result": [{"plain": "sales.raw.orders", "quoted": "sales.raw.daily revenue"}],
                    }
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT d.id, d.owner FROM unity_catalog.main.datasets d "
                    "WHERE d.qualified_name = unity_catalog.main.uc_qualified_name('Sales.Raw.Orders')"
                ),
                description="Look a table up by a name typed in any case",
            ),
        ]

    @classmethod
    def compute(
        cls,
        name: Annotated[
            pa.StringArray, Param(doc="Object name to normalize, e.g. 'Sales.Raw.Orders'; NULL gives NULL.")
        ],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Qualified name of every input."""
        return pa.array([qualified_name(n) for n in name.to_pylist()], type=pa.string())


class UcPrivilege(ScalarFunction):
    """``uc_privilege(privilege)``."""

    class Meta:
        """Function metadata."""

        name = "uc_privilege"
        description = "A Unity Catalog privilege in the canonical upper-case, underscore spelling"
        tags = {
            **object_tags(
                title="UC Privilege Name",
                category="governance",
                keywords="privilege, grant, normalize, use catalog, select, permissions",
                doc_llm=(
                    "Normalize a privilege as written in SQL GRANT statements or by open-source Unity Catalog "
                    "('USE CATALOG', 'select') to the Databricks spelling used in grants.privilege ('USE_CATALOG', "
                    "'SELECT'): trimmed, upper-cased, spaces replaced by underscores. Runs locally. NULL gives NULL."
                ),
                doc_md=(
                    "## uc_privilege(privilege)\n\n"
                    "A privilege in the spelling `grants.privilege` uses.\n\n"
                    "Offline; NULL in, NULL out."
                ),
            ),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "Open-source and SQL spellings of two privileges",
                        "sql": (
                            "SELECT unity_catalog.main.uc_privilege('USE CATALOG') AS use_catalog, "
                            "unity_catalog.main.uc_privilege(' read volume ') AS read_volume"
                        ),
                        "expected_result": [{"use_catalog": "USE_CATALOG", "read_volume": "READ_VOLUME"}],
                    }
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT securable_name, principal FROM unity_catalog.main.grants "
                    "WHERE securable_type = 'table' AND privilege = unity_catalog.main.uc_privilege('select') "
                    "ORDER BY ALL"
                ),
                description="Who can read which table, with the privilege typed as in a GRANT statement",
            ),
        ]

    @classmethod
    def compute(
        cls, privilege: Annotated[pa.StringArray, Param(doc="A privilege name, e.g. 'USE CATALOG'; NULL gives NULL.")]
    ) -> Annotated[pa.StringArray, Returns()]:
        """Canonical spelling of every privilege."""
        return pa.array([normalize_privilege(p) for p in privilege.to_pylist()], type=pa.string())


SCALARS: list[type[Any]] = [UcQualifiedName, UcPrivilege]
