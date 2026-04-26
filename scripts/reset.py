"""Reset the app — wipe persisted runs (inputs, outputs, manifest) and start clean.

Usage:
    python3 scripts/reset.py

Mapping config is NOT reset — only persisted run data. Use this when the app is
broken or you can't reach the Streamlit "Reset" panel from the History page.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    n_in = n_out = 0
    for d_name, counter in (("data/inputs", "in"), ("data/outputs", "out")):
        d = ROOT / d_name
        if not d.exists():
            print(f"  (skipped: {d_name} does not exist)")
            continue
        for f in d.iterdir():
            if f.name == ".gitkeep":
                continue
            try:
                f.unlink()
                if counter == "in":
                    n_in += 1
                else:
                    n_out += 1
            except OSError as e:
                print(f"  (warning: could not delete {f}: {e})")
    print(f"Cleared {n_in} input file(s) from data/inputs/")
    print(f"Cleared {n_out} output file(s) from data/outputs/")
    print("Mapping config (config/mapping.yaml) was not touched.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
