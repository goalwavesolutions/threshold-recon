"""Mapping config loader/saver."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "mapping.yaml"

# Tokens treated as "no value" when round-tripped through pandas/data-editor empty cells.
_BLANK_TOKENS = {"", "none", "nan", "null"}


def clean_optional_str(v: Any) -> str | None:
    """Coerce data-editor cell values (NaN/None/"None"/empty) to None or a real string."""
    if v is None:
        return None
    s = str(v).strip()
    if s.lower() in _BLANK_TOKENS:
        return None
    return s


def clean_str_list(v: Any) -> list[str]:
    """Parse a comma-or-list value, dropping NaN/None/blank tokens."""
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        items = v
    else:
        items = str(v).split(",")
    out = []
    for item in items:
        s = clean_optional_str(item)
        if s is not None:
            out.append(s)
    return out


@dataclass
class RcRule:
    rc: str
    type: str
    ref: float
    src: str
    composite_parent_stripe: str | None = None
    composite_components_internal: list[str] = field(default_factory=list)
    internal_only: bool = False
    values_are_subcategories: bool = False
    description: str = ""


@dataclass
class Mapping:
    cat_columns: list[str]
    rollup_categories_excluded: list[str]
    rc_rules: dict[str, RcRule]
    synthetic_fee_rcs: dict[str, dict[str, Any]]
    tie_tolerance: float

    @classmethod
    def load(cls, path: Path | str = DEFAULT_CONFIG_PATH) -> "Mapping":
        with open(path) as f:
            raw = yaml.safe_load(f)
        rc_rules: dict[str, RcRule] = {}
        for rc, m in raw["rc_mapping"].items():
            rc_rules[rc] = RcRule(
                rc=rc,
                type=m["type"],
                ref=float(m["ref"]),
                src=m["src"],
                composite_parent_stripe=clean_optional_str(m.get("composite_parent_stripe")),
                composite_components_internal=clean_str_list(m.get("composite_components_internal")),
                internal_only=bool(m.get("internal_only", False)),
                values_are_subcategories=bool(m.get("values_are_subcategories", False)),
                description=str(m.get("description") or ""),
            )

        # Defence-in-depth: drop any composite component refs that don't exist as rules.
        # (Stale data from prior bugs shouldn't crash the pipeline.)
        known = set(rc_rules)
        for rule in rc_rules.values():
            if rule.composite_components_internal:
                rule.composite_components_internal = [
                    c for c in rule.composite_components_internal if c in known
                ]
            if rule.composite_parent_stripe and rule.composite_parent_stripe not in known:
                rule.composite_parent_stripe = None
        return cls(
            cat_columns=raw["cat_columns"],
            rollup_categories_excluded=raw["rollup_categories_excluded"],
            rc_rules=rc_rules,
            synthetic_fee_rcs=raw.get("synthetic_fee_rcs", {}),
            tie_tolerance=float(raw.get("tie_tolerance", 0.01)),
        )

    def save(self, path: Path | str = DEFAULT_CONFIG_PATH) -> None:
        out = {
            "cat_columns": self.cat_columns,
            "rollup_categories_excluded": self.rollup_categories_excluded,
            "rc_mapping": {
                rc: {
                    "type": r.type,
                    "ref": r.ref,
                    "src": r.src,
                    **({"composite_parent_stripe": r.composite_parent_stripe}
                       if r.composite_parent_stripe else {}),
                    **({"composite_components_internal": r.composite_components_internal}
                       if r.composite_components_internal else {}),
                    **({"internal_only": True} if r.internal_only else {}),
                    **({"values_are_subcategories": True} if r.values_are_subcategories else {}),
                    **({"description": r.description} if r.description else {}),
                }
                for rc, r in self.rc_rules.items()
            },
            "synthetic_fee_rcs": self.synthetic_fee_rcs,
            "tie_tolerance": self.tie_tolerance,
        }
        with open(path, "w") as f:
            yaml.safe_dump(out, f, sort_keys=False, default_flow_style=False)
