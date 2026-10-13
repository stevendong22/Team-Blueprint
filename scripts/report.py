"""
NovaCart pipeline report — summarises a single processing date.

Usage:
    python -m scripts.report --date 2025-11-09
    python -m scripts.report --date 2025-11-09 --config config/pipeline.yaml
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.utils.config import Config


# ── helpers ────────────────────────────────────────────────────────────────

def _section(title: str) -> None:
    width = 60
    print(f"\n{'─' * width}")
    print(f"  {title}")
    print(f"{'─' * width}")


def _read_parquet_safe(path: Path) -> pd.DataFrame | None:
    if path.exists():
        try:
            return pd.read_parquet(path)
        except Exception:
            return None
    return None


def _latest_run(state_dir: Path, date_str: str) -> dict | None:
    """Return the most-recent run record for the given date."""
    runs_file = state_dir / "run_history.jsonl"
    if not runs_file.exists():
        return None
    runs = [json.loads(line) for line in runs_file.read_text().splitlines() if line.strip()]
    matching = [r for r in runs if r.get("date") == date_str]
    return matching[-1] if matching else None


def _log_events(log_dir: Path, date_str: str) -> list[dict]:
    """Return all log events that reference the given date (most recent run)."""
    log_file = log_dir / "pipeline.jsonl"
    if not log_file.exists():
        return []
    events = [json.loads(line) for line in log_file.read_text().splitlines() if line.strip()]
    # Identify the timestamp of the most recent pipeline_end for this date
    ends = [e for e in events if e.get("event") == "pipeline_end" and e.get("date") == date_str]
    if not ends:
        return [e for e in events if e.get("date") == date_str]
    last_end_ts = ends[-1]["ts"]
    # Collect events that belong to the last run (between the prior pipeline_end and this one)
    last_run_events: list[dict] = []
    in_run = False
    prev_end_ts = ends[-2]["ts"] if len(ends) > 1 else None
    for e in events:
        ts = e.get("ts", "")
        if prev_end_ts and ts <= prev_end_ts:
            continue
        last_run_events.append(e)
        if ts >= last_end_ts:
            break
    return last_run_events


def _quarantine_summary(q_dir: Path, date_str: str) -> dict:
    """Count quarantined rows for each source; return {source: count}."""
    result: dict[str, int] = {}
    if not q_dir.exists():
        return result
    for source_dir in q_dir.iterdir():
        if not source_dir.is_dir():
            continue
        total = 0
        for pq in source_dir.glob("*.parquet"):
            try:
                df = pd.read_parquet(pq)
                total += len(df)
            except Exception:
                pass
        if total:
            result[source_dir.name] = total
    return result


# ── main report ────────────────────────────────────────────────────────────

def run_report(date_str: str, config: Config) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    print(f"\n{'=' * 60}")
    print(f"  NovaCart Pipeline Report")
    print(f"  Date: {date_str}   Generated: {now}")
    print(f"{'=' * 60}")

    # ── 1. Run history ─────────────────────────────────────────────────────
    _section("Pipeline Run")
    run = _latest_run(config.state, date_str)
    if run is None:
        print(f"  ✗ No run found for {date_str} in state/run_history.jsonl")
        print("    Run:  python -m src.pipeline --date", date_str)
        return

    status_icon = "✓" if run["status"] == "SUCCESS" else "✗"
    print(f"  {status_icon} Status:   {run['status']}")
    print(f"    Started:   {run['started_at']}")
    print(f"    Finished:  {run['finished_at']}")
    print(f"    Duration:  {run['duration_sec']:.3f}s")

    stages = run.get("stages", [])
    if stages:
        print()
        max_name = max(len(s["stage"]) for s in stages)
        for s in stages:
            icon = "✓" if s["status"] == "OK" else "✗"
            err = f"  → {s['error']}" if s.get("error") else ""
            print(f"    {icon} {s['stage']:<{max_name}}  {s['duration_sec']:.3f}s{err}")

    if run["status"] != "SUCCESS":
        print(f"\n  Error: {run['error']}")

    # ── 2. Row counts from logs ────────────────────────────────────────────
    _section("Row Counts (last run)")
    events = _log_events(config.logs, date_str)
    count_events = {
        "Bronze orders":  ("orders_bronze_written", "rows"),
        "Silver orders":  ("silver_orders_written", "rows"),
        "Gold orders":    ("fact_orders_written",   "rows"),
    }
    for label, (event_name, field) in count_events.items():
        matching = [e for e in events if e.get("event") == event_name]
        if matching:
            val = matching[-1].get(field, "?")
            print(f"  {label:<20}  {val:>6} rows")
        else:
            print(f"  {label:<20}  {'—':>6}")

    # Quarantined orders
    q_summary = _quarantine_summary(config.quarantine, date_str)
    q_total = sum(q_summary.values())
    if q_total:
        print(f"  {'Quarantined total':<20}  {q_total:>6} rows  ⚠")
    else:
        print(f"  {'Quarantined':<20}  {'0':>6} rows  ✓")

    # ── 3. Gold fact_orders for this date ──────────────────────────────────
    fact_path = config.gold / "fact_orders" / f"date={date_str}" / "data.parquet"
    df_fact = _read_parquet_safe(fact_path)
    if df_fact is not None and not df_fact.empty:
        _section("Gold — fact_orders")
        print(f"  Orders:    {len(df_fact)}")
        if "total_amount" in df_fact.columns:
            print(f"  Revenue:   ${df_fact['total_amount'].sum():,.2f}")
            print(f"  Avg order: ${df_fact['total_amount'].mean():,.2f}")
        if "status" in df_fact.columns:
            print()
            print("  Status breakdown:")
            for status, cnt in df_fact["status"].value_counts().items():
                print(f"    {status:<15}  {cnt}")
        print()
        print(f"  Columns: {', '.join(df_fact.columns.tolist())}")
    else:
        _section("Gold — fact_orders")
        print(f"  No data found at {fact_path}")

    # ── 4. Quarantine detail ───────────────────────────────────────────────
    if q_total:
        _section("Quarantine Detail")
        for source, count in q_summary.items():
            print(f"  Source: {source}  ({count} rows)")
            q_src = config.quarantine / source
            frames = [pd.read_parquet(p) for p in q_src.glob("*.parquet")]
            if frames:
                qdf = pd.concat(frames, ignore_index=True)
                # Show reason column
                reason_col = next(
                    (c for c in qdf.columns if "reason" in c.lower() or "quarantine" in c.lower()),
                    None,
                )
                if reason_col:
                    print(f"  {'Reason':<45} Count")
                    print(f"  {'─' * 45} ─────")
                    for reason, cnt in qdf[reason_col].value_counts().head(8).items():
                        short = str(reason)[:44]
                        print(f"  {short:<45} {cnt}")
    else:
        _section("Quarantine")
        print("  ✓ No rows quarantined for any source.")

    # ── 5. dim_customer summary ────────────────────────────────────────────
    dim_cust_path = config.gold / "dim_customer.parquet"
    df_cust = _read_parquet_safe(dim_cust_path)
    if df_cust is not None and not df_cust.empty:
        _section("Gold — dim_customer (cumulative)")
        total = len(df_cust)
        current = df_cust["_current"].sum() if "_current" in df_cust.columns else "?"
        history = total - (current if isinstance(current, int) else 0)
        print(f"  Total rows:    {total}  (current: {current}, history: {history})")
        if "_current" in df_cust.columns:
            scd2 = df_cust[df_cust["_current"] == False]
            if not scd2.empty:
                print(f"\n  SCD Type 2 — closed (historical) rows:")
                cols = ["customer_id"] + [
                    c for c in ["name", "city", "country", "_eff_start", "_eff_end"]
                    if c in scd2.columns
                ]
                print("  " + scd2[cols].head(5).to_string(index=False).replace("\n", "\n  "))

    # ── 6. Watermark ───────────────────────────────────────────────────────
    wm_file = config.state / "watermarks.json"
    if wm_file.exists():
        wm = json.loads(wm_file.read_text())
        if wm:
            _section("Watermarks")
            for src, ts in wm.items():
                print(f"  {src:<20}  {ts}")

    print(f"\n{'=' * 60}\n")


# ── CLI ────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="NovaCart pipeline report")
    parser.add_argument("--date",   required=True, help="Processing date YYYY-MM-DD")
    parser.add_argument("--config", default="config/pipeline.yaml")
    args = parser.parse_args(argv)

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"Error: --date must be YYYY-MM-DD, got: {args.date}")
        return 1

    config = Config.load(args.config)
    run_report(args.date, config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
