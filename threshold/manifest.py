"""Versioned log of monthly source files: 2026-12 v1, v2, ..."""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path

INPUTS_DIR = Path("data/inputs")
OUTPUTS_DIR = Path("data/outputs")
MANIFEST_PATH = INPUTS_DIR / "manifest.json"


@dataclass
class ManifestEntry:
    period: str           # "2025-12"
    version: int          # 1, 2, ...
    reference: str        # "2025-12 v1"
    stripe_filename: str
    stored_stripe_path: str
    internal_filename: str
    stored_internal_path: str
    output_path: str | None
    exceptions_csv_path: str | None
    uploaded_at: str
    notes: str = ""
    overrides: dict = field(default_factory=dict)  # {ref: {"exception_type": str, "comment": str}}
    corrections: list = field(default_factory=list)  # list of Correction dicts (cell-level source fixes)


def _migrate_legacy_entry(e: dict) -> dict:
    """Upgrade a pre-split-upload entry (single combined file) to the two-file schema.

    Legacy entries had `original_filename` + `stored_input_path` (a single combined xlsx).
    Map both to the new stripe/internal fields so the entry is replayable — each reader
    will pick its own sheet from the combined file.
    """
    if "stripe_filename" not in e:
        legacy_name = e.pop("original_filename", "unknown.xlsx")
        legacy_path = e.pop("stored_input_path", "")
        e["stripe_filename"] = legacy_name
        e["internal_filename"] = legacy_name
        e["stored_stripe_path"] = legacy_path
        e["stored_internal_path"] = legacy_path
    # Backfill corrections for entries persisted before that field existed.
    e.setdefault("corrections", [])
    return e


def _load_raw() -> list[dict]:
    if not MANIFEST_PATH.exists():
        return []
    with open(MANIFEST_PATH) as f:
        raw = json.load(f)
    needs_save = False
    for e in raw:
        if "stripe_filename" not in e or "corrections" not in e:
            _migrate_legacy_entry(e)
            needs_save = True
    if needs_save:
        _save_raw(raw)
    return raw


def _save_raw(entries: list[dict]) -> None:
    INPUTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w") as f:
        json.dump(entries, f, indent=2)


def list_entries() -> list[ManifestEntry]:
    return [ManifestEntry(**e) for e in _load_raw()]


def next_version(period: str) -> int:
    existing = [e for e in list_entries() if e.period == period]
    return (max(e.version for e in existing) + 1) if existing else 1


def add_entry(
    period: str,
    stripe_filename: str,
    stripe_bytes: bytes,
    internal_filename: str,
    internal_bytes: bytes,
    output_path: str | None = None,
    exceptions_csv_path: str | None = None,
    notes: str = "",
    corrections: list | None = None,
) -> ManifestEntry:
    INPUTS_DIR.mkdir(parents=True, exist_ok=True)
    version = next_version(period)
    reference = f"{period} v{version}"

    stored_stripe = INPUTS_DIR / f"{period}_v{version}_stripe_{stripe_filename}"
    with open(stored_stripe, "wb") as f:
        f.write(stripe_bytes)
    stored_internal = INPUTS_DIR / f"{period}_v{version}_internal_{internal_filename}"
    with open(stored_internal, "wb") as f:
        f.write(internal_bytes)

    entry = ManifestEntry(
        period=period,
        version=version,
        reference=reference,
        stripe_filename=stripe_filename,
        stored_stripe_path=str(stored_stripe),
        internal_filename=internal_filename,
        stored_internal_path=str(stored_internal),
        output_path=output_path,
        exceptions_csv_path=exceptions_csv_path,
        uploaded_at=datetime.utcnow().isoformat(timespec="seconds") + "Z",
        notes=notes,
        corrections=corrections or [],
    )
    entries = _load_raw()
    entries.append(asdict(entry))
    _save_raw(entries)
    return entry


def update_entry(reference: str, **fields) -> ManifestEntry | None:
    entries = _load_raw()
    for e in entries:
        if e["reference"] == reference:
            e.update(fields)
            _save_raw(entries)
            return ManifestEntry(**e)
    return None


def get_entry(reference: str) -> ManifestEntry | None:
    for e in list_entries():
        if e.reference == reference:
            return e
    return None
