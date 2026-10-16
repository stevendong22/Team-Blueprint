"""
NovaCart Pipeline Monitoring Dashboard Generator

Reads pipeline logs and quarantine files, produces a self-contained
dashboard.html you can open in any browser.

Usage:
    python -m scripts.dashboard
    python -m scripts.dashboard --out reports/dashboard.html
"""
from __future__ import annotations

import argparse
import json
import glob
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# pandas/pyarrow only needed for parquet detail — falls back gracefully
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

ROOT = Path(__file__).parent.parent


# ── data loaders ───────────────────────────────────────────────────────────

def load_runs() -> list[dict]:
    """Most-recent run per date from run_history.jsonl (or runs.jsonl)."""
    for candidate in ["state/runs.jsonl", "state/run_history.jsonl"]:
        p = ROOT / candidate
        if p.exists():
            raw = [json.loads(l) for l in p.read_text().splitlines() if l.strip()]
            # Keep only the latest run per date
            by_date: dict[str, dict] = {}
            for r in raw:
                d = r.get("date", "")
                if d not in by_date or r.get("started_at", "") > by_date[d].get("started_at", ""):
                    by_date[d] = r
            return sorted(by_date.values(), key=lambda r: r["date"])
    return []


def load_log_events() -> list[dict]:
    p = ROOT / "logs" / "pipeline.jsonl"
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]


def _date_from_event(e: dict) -> str | None:
    """Extract YYYY-MM-DD from event's date field or embedded path."""
    import re
    if e.get("date"):
        return e["date"]
    path = e.get("path", "")
    m = re.search(r"date=(\d{4}-\d{2}-\d{2})", path)
    return m.group(1) if m else None


def row_counts_by_date(events: list[dict]) -> dict[str, dict]:
    """
    Returns {date: {bronze_orders, silver_orders, gold_orders,
                     quarantine_orders, quarantine_customers}}
    using events that carry a date field or a date= in their path.
    Quarantine events (no date) are distributed by matching the pipeline_end
    boundary for each date.
    """
    counts: dict[str, dict] = defaultdict(lambda: {
        "bronze_orders": 0, "silver_orders": 0, "gold_orders": 0,
        "quarantine_orders": 0, "quarantine_customers": 0,
    })

    # Pass 1 — events that carry an explicit date (bronze path, silver, gold, quarantine)
    # For quarantine we track by pipeline_end boundaries instead (see pass 2)
    for e in events:
        ev = e.get("event", "")
        rows = e.get("rows", e.get("count", 0)) or 0
        d = _date_from_event(e)
        if not d:
            continue
        if ev == "orders_bronze_written":
            counts[d]["bronze_orders"] = max(counts[d]["bronze_orders"], rows)
        elif ev == "silver_orders_written":
            counts[d]["silver_orders"] = max(counts[d]["silver_orders"], rows)
        elif ev == "fact_orders_written":
            counts[d]["gold_orders"] = max(counts[d]["gold_orders"], rows)

    # Pass 2 — quarantine events have no date; assign them by matching
    # the pipeline_end that processes each date (latest run only).
    # Each pipeline_end event carries a date — collect quarantine events
    # that occur immediately before it.
    pipeline_ends = [e for e in events if e.get("event") == "pipeline_end"]
    # Build per-date latest end timestamp
    latest_end: dict[str, str] = {}
    for e in pipeline_ends:
        d = e.get("date", "")
        if d and e["ts"] > latest_end.get(d, ""):
            latest_end[d] = e["ts"]

    # Sort events by timestamp
    sorted_events = sorted(events, key=lambda x: x.get("ts", ""))

    # For each date, find quarantine events between previous pipeline_end and this one
    sorted_dates = sorted(latest_end.keys())
    for idx, d in enumerate(sorted_dates):
        end_ts = latest_end[d]
        # Previous pipeline_end across ALL dates before this one
        prev_ends = [latest_end[od] for od in sorted_dates[:idx]]
        prev_ts = max(prev_ends) if prev_ends else ""
        q_orders = q_customers = 0
        for e in sorted_events:
            ts = e.get("ts", "")
            if ts <= prev_ts:
                continue
            if ts > end_ts:
                break
            ev = e.get("event", "")
            cnt = e.get("count", 0) or 0
            if ev == "orders_quarantined":
                q_orders += cnt
            elif ev == "customers_quarantined":
                q_customers += cnt
        counts[d]["quarantine_orders"]    = q_orders
        counts[d]["quarantine_customers"] = q_customers

    return counts


def load_quarantine_rows() -> list[dict]:
    """Read all quarantine parquet files; return list of row dicts."""
    if not HAS_PANDAS:
        return []
    rows = []
    # Support both timestamp-named files and dt=YYYY-MM-DD partitioned paths
    patterns = [
        "data/quarantine/**/*.parquet",
        "data/quarantine/*/*.parquet",
    ]
    seen = set()
    for pat in patterns:
        for path in glob.glob(str(ROOT / pat), recursive=True):
            if path in seen or "archived" in path:
                continue
            seen.add(path)
            try:
                df = pd.read_parquet(path)
                source = Path(path).parent.name
                # source may be timestamp — walk up to find orders/customers
                if source.startswith("20") or source.startswith("dt="):
                    source = Path(path).parent.parent.name
                df["_source"] = source
                df["_file"] = Path(path).name
                rows.extend(df.to_dict(orient="records"))
            except Exception:
                pass
    return rows


def quarantine_counts_by_date() -> dict[str, dict]:
    """Read actual parquet files and return {date: {quarantine_orders, quarantine_customers}}."""
    import re
    result: dict[str, dict] = defaultdict(lambda: {"quarantine_orders": 0, "quarantine_customers": 0})
    if not HAS_PANDAS:
        return result
    pattern = str(ROOT / "data/quarantine/**/*.parquet")
    for path in glob.glob(pattern, recursive=True):
        if "archived" in path:
            continue
        # Extract date from path: dt=2025-11-09
        m = re.search(r"dt=(\d{4}-\d{2}-\d{2})", path)
        if not m:
            continue
        date = m.group(1)
        source = Path(path).parent.parent.name  # orders or customers
        try:
            df = pd.read_parquet(path)
            count = len(df)
            if source == "orders":
                result[date]["quarantine_orders"] += count
            elif source == "customers":
                result[date]["quarantine_customers"] += count
        except Exception:
            pass
    return result


def error_reason_summary(rows: list[dict]) -> list[dict]:
    """Top error reasons with count and source."""
    reason_col = "_quarantine_reason"
    counts: dict[tuple, int] = defaultdict(int)
    for r in rows:
        reason = str(r.get(reason_col, r.get("_error_reason", "unknown")))
        # Truncate pydantic validation noise to the core message
        if "validation error" in reason.lower():
            # extract first field-level message
            lines = [l.strip() for l in reason.split("\n") if l.strip()]
            reason = lines[2] if len(lines) > 2 else lines[-1]
        source = str(r.get("_source", "unknown"))
        counts[(source, reason[:80])] += 1
    return sorted(
        [{"source": s, "reason": r, "count": c} for (s, r), c in counts.items()],
        key=lambda x: -x["count"],
    )[:15]


def recent_rows(rows: list[dict], n: int = 20) -> list[dict]:
    """Most recent quarantined rows, key columns only."""
    keep_cols = {
        "order_id", "customer_id", "amount", "status", "quantity",
        "email", "name", "city",
        "_quarantine_reason", "_error_reason", "_quarantined_at",
        "_source",
    }
    out = []
    for r in rows:
        clean = {k: v for k, v in r.items() if k in keep_cols}
        # normalise reason column name
        if "_error_reason" in clean and "_quarantine_reason" not in clean:
            clean["_quarantine_reason"] = clean.pop("_error_reason")
        out.append(clean)
    # Sort by quarantine time descending if available
    out.sort(key=lambda r: str(r.get("_quarantined_at", "")), reverse=True)
    return out[:n]


# ── business impact ────────────────────────────────────────────────────────

def compute_impact(runs: list[dict], counts: dict[str, dict], q_rows: list[dict]) -> dict:
    """
    Estimate cost and time savings.

    Assumptions (grounded in the case study):
      - $400K gap / 12,000 dropped rows  ≈  $33.33 revenue at risk per bad order row
      - Manual ETL + QA per date         ≈  4 hours  @ $75 / hr  = $300
      - Automated pipeline time is taken directly from run_history
    """
    REVENUE_PER_BAD_ORDER = 33.33   # $/row — from $400K / 12,000 case-study rows
    MANUAL_HRS_PER_RUN    = 4.0     # hours a data analyst would spend manually
    ANALYST_RATE          = 75.0    # $/hr

    total_q_orders = sum(c.get("quarantine_orders", 0) for c in counts.values())
    total_bronze   = sum(c.get("bronze_orders", 0) for c in counts.values())
    total_gold     = sum(c.get("gold_orders", 0) for c in counts.values())
    total_q_all    = sum(c.get("quarantine_orders", 0) + c.get("quarantine_customers", 0)
                         for c in counts.values())

    n_runs          = len(runs)
    total_auto_sec  = sum(r.get("duration_sec", 0) for r in runs)
    manual_hrs      = n_runs * MANUAL_HRS_PER_RUN
    auto_hrs        = total_auto_sec / 3600

    hrs_saved       = manual_hrs - auto_hrs
    cost_saved      = hrs_saved * ANALYST_RATE
    revenue_protected = total_q_orders * REVENUE_PER_BAD_ORDER

    quality_rate    = (total_gold / total_bronze * 100) if total_bronze else 100.0

    # Try to read real average order value from Gold parquet
    if HAS_PANDAS:
        try:
            import glob as _glob
            parts = _glob.glob(str(ROOT / "data/gold/fact_orders/**/*.parquet"), recursive=True)
            if parts:
                import pandas as _pd
                df = _pd.concat([_pd.read_parquet(p) for p in parts], ignore_index=True)
                if "total_amount" in df.columns and len(df):
                    avg_val = df["total_amount"].mean()
                    revenue_protected = total_q_orders * avg_val
        except Exception:
            pass

    return {
        "revenue_protected": revenue_protected,
        "hrs_saved": hrs_saved,
        "cost_saved": cost_saved,
        "quality_rate": quality_rate,
        "total_auto_sec": total_auto_sec,
        "manual_hrs": manual_hrs,
        "n_runs": n_runs,
        "total_q_orders": total_q_orders,
    }


# ── HTML template ──────────────────────────────────────────────────────────

def render_html(
    runs: list[dict],
    counts: dict[str, dict],
    error_summary: list[dict],
    recent: list[dict],
    generated_at: str,
    impact: dict,
    total_q_actual: int = 0,
) -> str:

    # Summary stats
    total_runs = len(runs)
    success_count = sum(1 for r in runs if r.get("status") == "SUCCESS")
    success_rate = round(success_count / total_runs * 100) if total_runs else 0
    total_bronze = sum(c["bronze_orders"] for c in counts.values())
    total_q = total_q_actual

    # Chart data
    dates = sorted(counts.keys())
    q_orders    = [counts[d]["quarantine_orders"] for d in dates]
    q_customers = [counts[d]["quarantine_customers"] for d in dates]
    bronze_rows = [counts[d]["bronze_orders"] for d in dates]
    gold_rows   = [counts[d]["gold_orders"] for d in dates]

    # Error reasons chart
    er_labels = [f"{e['source']}: {e['reason'][:45]}" for e in error_summary]
    er_counts = [e["count"] for e in error_summary]

    # Run table rows HTML
    def status_badge(s: str) -> str:
        color = "#198038" if s == "SUCCESS" else "#DA1E28"
        return f'<span style="background:{color};color:#fff;padding:2px 10px;border-radius:3px;font-size:11px;font-weight:700">{s}</span>'

    run_rows_html = ""
    for r in sorted(runs, key=lambda x: x["date"], reverse=True):
        d = r["date"]
        c = counts.get(d, {})
        q_total = c.get("quarantine_orders", 0) + c.get("quarantine_customers", 0)
        q_cell = f'<span style="color:#DA1E28;font-weight:700">{q_total}</span>' if q_total else "0"
        err = f'<span style="color:#DA1E28;font-size:11px">{r["error"]}</span>' if r.get("error") else "—"
        run_rows_html += f"""
        <tr>
          <td>{d}</td>
          <td>{status_badge(r.get("status","?"))}</td>
          <td>{r.get("duration_sec", 0):.3f}s</td>
          <td>{c.get("bronze_orders","—")}</td>
          <td>{c.get("silver_orders","—")}</td>
          <td>{c.get("gold_orders","—")}</td>
          <td>{q_cell}</td>
          <td>{err}</td>
        </tr>"""

    # Recent bad rows table
    if recent:
        all_keys = []
        for row in recent:
            for k in row:
                if k not in all_keys:
                    all_keys.append(k)
        # Order nicely
        priority = ["_source","order_id","customer_id","name","email","amount","status","quantity","city","_quarantine_reason","_quarantined_at","_file"]
        ordered_keys = [k for k in priority if k in all_keys] + [k for k in all_keys if k not in priority]

        thead = "".join(f"<th>{k}</th>" for k in ordered_keys)
        tbody = ""
        for row in recent:
            reason = str(row.get("_quarantine_reason", ""))
            cells = ""
            for k in ordered_keys:
                v = row.get(k, "")
                if v is None:
                    v = ""
                v = str(v)
                if k == "_quarantine_reason":
                    cells += f'<td style="color:#DA1E28;max-width:280px">{v[:80]}</td>'
                elif k == "_source":
                    cells += f'<td><span style="background:#EDF5FF;color:#0043CE;padding:2px 8px;border-radius:3px;font-size:11px">{v}</span></td>'
                else:
                    cells += f"<td>{v[:40]}</td>"
            tbody += f"<tr>{cells}</tr>"
        bad_rows_html = f'<table class="dtable"><thead><tr>{thead}</tr></thead><tbody>{tbody}</tbody></table>'
    else:
        bad_rows_html = '<p style="color:#6F6F6F;padding:16px">No quarantine parquet files found — run the pipeline first, or ensure pandas + pyarrow are installed.</p>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>NovaCart Pipeline Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.5.0/dist/chart.umd.js"></script>
<style>
  :root {{ color-scheme: light }}
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0 }}
  body {{ font-family: 'IBM Plex Sans', Helvetica, Arial, sans-serif; background: #f4f4f4; color: #161616 }}
  a {{ color: #0F62FE }}

  /* header */
  .header {{ background: #001D6C; color: #fff; padding: 20px 32px; display: flex; align-items: baseline; gap: 24px }}
  .header h1 {{ font-size: 22px; font-weight: 600 }}
  .header .sub {{ font-size: 13px; color: #A6C8FF }}

  /* layout */
  .page {{ max-width: 1400px; margin: 0 auto; padding: 24px 32px }}
  .section-title {{ font-size: 13px; font-weight: 700; color: #525252; text-transform: uppercase; letter-spacing: .06em; margin-bottom: 12px; margin-top: 32px }}

  /* stat cards */
  .cards {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 4px }}
  .card {{ background: #fff; border-top: 3px solid #0F62FE; padding: 20px 24px }}
  .card .num {{ font-size: 36px; font-weight: 700; color: #0F62FE; line-height: 1 }}
  .card .label {{ font-size: 12px; color: #6F6F6F; margin-top: 6px }}
  .card.warn .num {{ color: #DA1E28 }}
  .card.warn {{ border-top-color: #DA1E28 }}

  /* run table */
  .dtable {{ width: 100%; border-collapse: collapse; font-size: 13px; background: #fff }}
  .dtable th {{ background: #001D6C; color: #fff; padding: 10px 14px; text-align: left; font-weight: 600; font-size: 12px; white-space: nowrap }}
  .dtable td {{ padding: 9px 14px; border-bottom: 1px solid #E0E0E0; vertical-align: middle }}
  .dtable tr:hover td {{ background: #EDF5FF }}
  .table-wrap {{ overflow-x: auto; background: #fff }}

  /* charts */
  .charts {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px }}
  .chart-box {{ background: #fff; padding: 20px 24px }}
  .chart-box h3 {{ font-size: 14px; font-weight: 600; margin-bottom: 14px; color: #161616 }}
  .chart-wrap {{ height: 260px; position: relative }}

  /* impact cards */
  .impact-cards {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 4px }}
  .impact-card {{ background: #001D6C; padding: 20px 24px }}
  .impact-card .num {{ font-size: 30px; font-weight: 700; color: #fff; line-height: 1 }}
  .impact-card .label {{ font-size: 12px; color: #A6C8FF; margin-top: 6px }}
  .impact-card .note {{ font-size: 11px; color: #4589FF; margin-top: 8px; line-height: 1.4 }}

  /* alert box */
  .alert {{ background: #FFF1F1; border-left: 4px solid #DA1E28; padding: 12px 16px; font-size: 13px; color: #DA1E28; margin-bottom: 16px }}

  /* who gets notified box */
  .notify-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px }}
  .notify-card {{ background: #fff; padding: 18px 20px; border-left: 4px solid #0F62FE }}
  .notify-card h4 {{ font-size: 13px; font-weight: 700; color: #161616; margin-bottom: 6px }}
  .notify-card p {{ font-size: 12px; color: #525252; line-height: 1.5 }}
  .notify-card .role {{ font-size: 11px; color: #0F62FE; margin-bottom: 6px }}

  .footer {{ margin-top: 40px; padding-top: 16px; border-top: 1px solid #E0E0E0; font-size: 11px; color: #A8A8A8 }}
</style>
</head>
<body>

<div class="header">
  <h1>NovaCart Pipeline Dashboard</h1>
  <span class="sub">Generated {generated_at} &nbsp;·&nbsp; {total_runs} runs tracked &nbsp;·&nbsp; Indigo BioMed Data Operations</span>
</div>

<div class="page">

  {"" if total_q == 0 else f'<div class="alert">⚠ {total_q} quarantined rows across all dates — review the table below and run <code>python -m scripts.reprocess_quarantine</code></div>'}

  <!-- summary cards -->
  <div class="section-title">Pipeline Health</div>
  <div class="cards">
    <div class="card">
      <div class="num">{total_runs}</div>
      <div class="label">Total runs tracked</div>
    </div>
    <div class="card">
      <div class="num">{success_rate}%</div>
      <div class="label">Success rate ({success_count} / {total_runs})</div>
    </div>
    <div class="card">
      <div class="num">{total_bronze:,}</div>
      <div class="label">Total rows ingested (Bronze)</div>
    </div>
    <div class="card {"warn" if total_q > 0 else ""}">
      <div class="num">{total_q}</div>
      <div class="label">Total quarantined rows</div>
    </div>
  </div>

  <!-- estimated savings -->
  <div class="section-title">Pipeline Efficiency</div>
  <div class="impact-cards" style="grid-template-columns: repeat(2, 1fr)">
    <div class="impact-card">
      <div class="num">{impact["hrs_saved"]:.1f} hrs</div>
      <div class="label">Analyst time saved</div>
      <div class="note">{impact["n_runs"]} runs × 4 hrs manual ETL = {impact["manual_hrs"]:.0f} hrs estimated manual<br>Automated in {impact["total_auto_sec"]:.1f}s total</div>
    </div>
    <div class="impact-card">
      <div class="num">{impact["quality_rate"]:.1f}%</div>
      <div class="label">Data quality rate</div>
      <div class="note">Clean Gold rows ÷ total Bronze rows ingested</div>
    </div>
  </div>

  <!-- run history table -->
  <div class="section-title" style="margin-top:28px">Run History</div>
  <div class="table-wrap">
    <table class="dtable">
      <thead>
        <tr>
          <th>Date</th><th>Status</th><th>Duration</th>
          <th>Bronze Orders</th><th>Silver Orders</th><th>Gold Orders</th>
          <th>Quarantined</th><th>Error</th>
        </tr>
      </thead>
      <tbody>{run_rows_html}</tbody>
    </table>
  </div>

  <!-- charts -->
  <div class="section-title">Quarantine &amp; Volume</div>
  <div class="charts">
    <div class="chart-box">
      <h3>Quarantined Rows by Date</h3>
      <div class="chart-wrap">
        <canvas id="qChart"></canvas>
      </div>
    </div>
    <div class="chart-box">
      <h3>Bronze vs Gold Row Counts by Date</h3>
      <div class="chart-wrap">
        <canvas id="volChart"></canvas>
      </div>
    </div>
  </div>

  <div class="charts" style="margin-top:20px">
    <div class="chart-box">
      <h3>Top Error Reasons</h3>
      <div class="chart-wrap">
        <canvas id="errChart"></canvas>
      </div>
    </div>
    <div class="chart-box" style="display:flex;flex-direction:column;justify-content:center">
      <h3>Who Gets Notified?</h3>
      <div class="notify-grid" style="display:flex;flex-direction:column;gap:12px;margin-top:4px">
        <div class="notify-card">
          <div class="role">Immediate (pipeline failure or quarantine rate &gt; 10%)</div>
          <h4>Data Engineering Team</h4>
          <p>Run <code>python -m scripts.reprocess_quarantine</code> to review bad rows, identify root cause, clear the queue, and re-run after source is fixed.</p>
        </div>
        <div class="notify-card" style="border-left-color:#001D6C">
          <div class="role" style="color:#001D6C">Daily summary</div>
          <h4>Riley Chen — Director of Data Platforms</h4>
          <p>Receives a daily run health digest: success rate, quarantine counts, row volumes, and any schema drift warnings.</p>
        </div>
        <div class="notify-card" style="border-left-color:#198038">
          <div class="role" style="color:#198038">On SLA breach (&gt; 1 hr delay)</div>
          <h4>Alex Morgan — VP of Digital Health</h4>
          <p>Escalation if Gold tables are not ready within the 1-hour SLA window after source file drop.</p>
        </div>
      </div>
    </div>
  </div>

  <!-- recent bad rows -->
  <div class="section-title">Recent Quarantined Rows</div>
  <div class="table-wrap">
    {bad_rows_html}
  </div>

  <div class="footer">
    NovaCart, an Indigo BioMed Company &nbsp;·&nbsp;
    Dashboard generated by <code>python -m scripts.dashboard</code> &nbsp;·&nbsp;
    Data from <code>state/run_history.jsonl</code>, <code>logs/pipeline.jsonl</code>, <code>data/quarantine/</code>
  </div>

</div>

<script>
const BLUE  = '#0F62FE';
const NAVY  = '#001D6C';
const RED   = '#DA1E28';
const GREEN = '#198038';
const LBLUE = '#4589FF';

// Quarantine by date (stacked bar)
new Chart(document.getElementById('qChart'), {{
  type: 'bar',
  data: {{
    labels: {json.dumps(dates)},
    datasets: [
      {{ label: 'Orders quarantined',    data: {json.dumps(q_orders)},    backgroundColor: RED,   stack: 'q' }},
      {{ label: 'Customers quarantined', data: {json.dumps(q_customers)}, backgroundColor: '#FF8389', stack: 'q' }},
    ]
  }},
  options: {{
    responsive: true, maintainAspectRatio: false,
    plugins: {{ legend: {{ position: 'bottom', labels: {{ font: {{ size: 12 }} }} }} }},
    scales: {{
      x: {{ stacked: true, grid: {{ display: false }} }},
      y: {{ stacked: true, beginAtZero: true, ticks: {{ stepSize: 1 }} }}
    }}
  }}
}});

// Volume chart (Bronze vs Gold)
new Chart(document.getElementById('volChart'), {{
  type: 'bar',
  data: {{
    labels: {json.dumps(dates)},
    datasets: [
      {{ label: 'Bronze (ingested)', data: {json.dumps(bronze_rows)}, backgroundColor: NAVY }},
      {{ label: 'Gold (delivered)',  data: {json.dumps(gold_rows)},   backgroundColor: BLUE }},
    ]
  }},
  options: {{
    responsive: true, maintainAspectRatio: false,
    plugins: {{ legend: {{ position: 'bottom', labels: {{ font: {{ size: 12 }} }} }} }},
    scales: {{ x: {{ grid: {{ display: false }} }}, y: {{ beginAtZero: true }} }}
  }}
}});

// Error reasons (horizontal bar)
new Chart(document.getElementById('errChart'), {{
  type: 'bar',
  data: {{
    labels: {json.dumps(er_labels)},
    datasets: [{{ label: 'Count', data: {json.dumps(er_counts)}, backgroundColor: BLUE }}]
  }},
  options: {{
    indexAxis: 'y',
    responsive: true, maintainAspectRatio: false,
    plugins: {{ legend: {{ display: false }} }},
    scales: {{
      x: {{ beginAtZero: true, ticks: {{ stepSize: 1 }} }},
      y: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 11 }} }} }}
    }}
  }}
}});
</script>
</body>
</html>"""


# ── main ───────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate NovaCart pipeline dashboard")
    parser.add_argument("--out", default="dashboard.html", help="Output HTML file path")
    parser.add_argument("--config", default="config/pipeline.yaml")
    args = parser.parse_args(argv)

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    print("Reading run history...")
    runs = load_runs()
    print(f"  {len(runs)} unique dates found")

    print("Reading log events...")
    events = load_log_events()
    counts = row_counts_by_date(events)

    print("Reading quarantine files...")
    q_rows = load_quarantine_rows()
    q_by_date = quarantine_counts_by_date()
    # Merge real parquet counts into counts dict (overrides log-derived quarantine counts)
    for d, qc in q_by_date.items():
        if d not in counts:
            counts[d] = {"bronze_orders": 0, "silver_orders": 0, "gold_orders": 0,
                         "quarantine_orders": 0, "quarantine_customers": 0}
        counts[d]["quarantine_orders"]    = qc["quarantine_orders"]
        counts[d]["quarantine_customers"] = qc["quarantine_customers"]
    if not HAS_PANDAS:
        print("  pandas not available — bad-row detail table will be empty")
    else:
        print(f"  {len(q_rows)} quarantined rows loaded")

    err_summary = error_reason_summary(q_rows)
    recent = recent_rows(q_rows)

    print("Computing business impact...")
    impact = compute_impact(runs, counts, q_rows)

    out_path = ROOT / args.out
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Rendering dashboard → {out_path}")
    html = render_html(runs, counts, err_summary, recent, generated_at, impact, total_q_actual=len(q_rows))
    out_path.write_text(html, encoding="utf-8")

    print(f"✓ Done. Open in browser: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
