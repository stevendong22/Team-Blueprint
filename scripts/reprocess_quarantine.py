"""
NovaCart — Quarantine Review & Reprocess Tool

Shows every bad row with its rejection reason, then optionally
re-injects corrected rows back into the landing zone so they flow
through Bronze → Silver → Gold on the next pipeline run.

Usage:
    # Review only — see what's in quarantine
    python -m scripts.reprocess_quarantine

    # Review a specific source
    python -m scripts.reprocess_quarantine --source orders

    # After fixing rows: clear quarantine for a source
    python -m scripts.reprocess_quarantine --source orders --clear
"""
from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.utils.config import Config


# ── helpers ────────────────────────────────────────────────────────────────

def _section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def _load_quarantine(q_dir: Path, source: str | None = None) -> dict[str, pd.DataFrame]:
    """Return {source_name: combined_dataframe} for all (or one) quarantine source."""
    result: dict[str, pd.DataFrame] = {}
    if not q_dir.exists():
        return result

    sources = [q_dir / source] if source else [p for p in q_dir.iterdir() if p.is_dir()]
    for src_path in sources:
        if not src_path.is_dir():
            print(f"  ✗ Source '{src_path.name}' not found in quarantine.")
            continue
        files = sorted(src_path.glob("*.parquet"))
        if not files:
            continue
        frames = []
        for f in files:
            try:
                frames.append(pd.read_parquet(f))
            except Exception as e:
                print(f"  Warning: could not read {f.name}: {e}")
        if frames:
            result[src_path.name] = pd.concat(frames, ignore_index=True)

    return result


def _reason_col(df: pd.DataFrame) -> str | None:
    return next((c for c in df.columns if "reason" in c.lower()), None)


# ── review ─────────────────────────────────────────────────────────────────

def review(q_dir: Path, source: str | None) -> None:
    data = _load_quarantine(q_dir, source)

    if not data:
        print("\n  ✓ No quarantined rows found.")
        return

    total_bad = sum(len(df) for df in data.values())
    print(f"\n  Total quarantined rows across all sources: {total_bad}")

    for src_name, df in data.items():
        _section(f"Source: {src_name}  ({len(df)} bad rows)")

        reason_col = _reason_col(df)
        display_cols = [c for c in df.columns if not c.startswith("_")]

        # Summary: count by reason
        if reason_col:
            print("\n  Rejection reasons:")
            for reason, count in df[reason_col].value_counts().items():
                print(f"    {count:>3}x  {str(reason)[:70]}")

        # Full row detail
        print(f"\n  Row detail:")
        pd.set_option("display.max_colwidth", 40)
        pd.set_option("display.width", 120)
        if display_cols:
            print(df[display_cols].to_string(index=True))
        else:
            print(df.to_string(index=True))

        if reason_col:
            print(f"\n  _quarantine_reason column:")
            for i, reason in enumerate(df[reason_col]):
                print(f"    [{i}] {reason}")


# ── clear (after upstream fix) ─────────────────────────────────────────────

def clear(q_dir: Path, source: str) -> None:
    src_path = q_dir / source
    if not src_path.exists():
        print(f"\n  ✗ No quarantine folder found for source '{source}'.")
        return

    files = list(src_path.glob("*.parquet"))
    if not files:
        print(f"\n  ✓ Quarantine for '{source}' is already empty.")
        return

    # Archive first
    archive_dir = src_path / "archived"
    archive_dir.mkdir(exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    for f in files:
        dest = archive_dir / f"{ts}_{f.name}"
        shutil.move(str(f), str(dest))

    print(f"\n  ✓ Cleared {len(files)} quarantine file(s) for '{source}'.")
    print(f"    Archived to: {archive_dir}")
    print(f"\n  Next step: fix the rows in the source file, then re-run:")
    print(f"    python -m src.pipeline --date <YYYY-MM-DD>")


# ── re-inject fixed rows ────────────────────────────────────────────────────

def reinject(q_dir: Path, landing_dir: Path, source: str) -> None:
    """
    After manually correcting bad rows, write them back to the landing zone
    so the next pipeline run picks them up through Bronze → Silver → Gold.

    This is a guided step — it prints what would be re-injected and asks
    for confirmation before writing anything.
    """
    data = _load_quarantine(q_dir, source)
    if not data:
        print(f"\n  ✗ No quarantined rows found for source '{source}'.")
        return

    df = data[source]
    reason_col = _reason_col(df)
    # Drop quarantine metadata columns before re-injecting
    clean_cols = [c for c in df.columns if not c.startswith("_")]
    clean_df = df[clean_cols].copy()

    print(f"\n  {len(clean_df)} rows ready to re-inject into landing zone for '{source}'.")
    print(f"  They will be written to: {landing_dir}")
    print()
    print(clean_df.to_string(index=False))
    print()

    confirm = input("  Re-inject these rows? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("  Cancelled — no files written.")
        return

    landing_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    out_path = landing_dir / f"reinjected_{ts}.parquet"
    clean_df.to_parquet(out_path, index=False)
    print(f"\n  ✓ Written to: {out_path}")
    print(f"  Now run:  python -m src.pipeline --date <YYYY-MM-DD>")


# ── CLI ────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Review and reprocess quarantined rows"
    )
    parser.add_argument(
        "--source", default=None,
        help="Limit to one source: orders, customers, or products"
    )
    parser.add_argument(
        "--clear", action="store_true",
        help="Archive and clear quarantine files for --source after fixing upstream"
    )
    parser.add_argument(
        "--reinject", action="store_true",
        help="Re-inject cleaned rows back into the landing zone for --source"
    )
    parser.add_argument("--config", default="config/pipeline.yaml")
    args = parser.parse_args(argv)

    config = Config.load(args.config)

    if args.clear:
        if not args.source:
            print("Error: --clear requires --source (e.g. --source orders)")
            return 1
        clear(config.quarantine, args.source)
        return 0

    if args.reinject:
        if not args.source:
            print("Error: --reinject requires --source (e.g. --source orders)")
            return 1
        landing_map = {
            "orders":    config.landing_orders.parent,
            "customers": config.landing_customers.parent,
            "products":  config.landing_products_db.parent,
        }
        landing = landing_map.get(args.source)
        if not landing:
            print(f"Error: unknown source '{args.source}'")
            return 1
        reinject(config.quarantine, landing, args.source)
        return 0

    # Default: review
    review(config.quarantine, args.source)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
