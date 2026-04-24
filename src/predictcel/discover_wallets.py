from __future__ import annotations

import sys

from .main import main as run_main

LEGACY_FLAGS = {"--limit", "--trade-limit", "--min-score", "--output"}


def main() -> None:
    legacy_flags = [arg for arg in sys.argv[1:] if arg in LEGACY_FLAGS]
    if legacy_flags:
        joined = ", ".join(sorted(set(legacy_flags)))
        raise SystemExit(
            "Legacy discover_wallets.py flags are no longer supported ("
            f"{joined}"). Use the canonical config-driven command instead: "
            "python -m predictcel.main discover-wallets --config config/predictcel.example.json"
        )

    previous_argv = sys.argv
    try:
        sys.argv = [sys.argv[0], "discover-wallets", *sys.argv[1:]]
        run_main()
    finally:
        sys.argv = previous_argv


if __name__ == "__main__":
    main()
