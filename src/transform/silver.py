"""Bronze → Silver: validate with Pydantic, dedupe, quarantine bad rows."""
from __future__ import annotations
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Type

import pandas as pd
from pydantic import BaseModel, ValidationError

from src.utils.logging_setup import log_event
from src.utils.schemas import OrderRow, CustomerRow, ProductRow


def _friendly_reason(exc: Exception, model_name: str = "") -> str:
    """Convert a Pydantic ValidationError (or any exception) into a short readable reason."""
    if not isinstance(exc, ValidationError):
        return str(exc)

    label = model_name or exc.title or "Row"
    parts = []
    for err in exc.errors():
        msg = err.get("msg", "")

        # Strip leading "Value error, " prefix that Pydantic adds
        msg = re.sub(r"^[Vv]alue error,\s*", "", msg)

        # "status 'refunded' not in {…}" → "OrderRow: Invalid status: refunded"
        m = re.search(r"status '([^']+)' not in", msg)
        if m:
            parts.append(f"{label}: Invalid status: {m.group(1)}")
            continue

        # "invalid email: foo-at-bar.com" → "CustomerRow: Invalid email: foo-at-bar.com"
        m = re.search(r"invalid email:\s*(\S+)", msg, re.IGNORECASE)
        if m:
            parts.append(f"{label}: Invalid email: {m.group(1)}")
            continue

        # Generic: use model name instead of raw field path
        parts.append(f"{label}: {msg}")

    return " | ".join(parts) if parts else str(exc)


def _validate_df(
    df: pd.DataFrame,
    model: Type[BaseModel],
    primary_key: str,
    quarantine_path: Path,
    logger: logging.Logger,
    source_name: str,
    date_str: str = "",
) -> pd.DataFrame:
    """Validate each row with Pydantic. Good rows → Silver, bad rows → quarantine."""
    good, bad = [], []
    for _, row in df.iterrows():
        try:
            model(**row.to_dict())
            good.append(row)
        except (ValidationError, Exception) as exc:
            row_dict = row.to_dict()
            row_dict["_error_reason"] = _friendly_reason(exc, model.__name__)
            row_dict["_quarantined_at"] = datetime.now(timezone.utc).isoformat()
            bad.append(row_dict)

    if bad:
        partition = f"dt={date_str}" if date_str else "dt=unknown"
        q_dir = quarantine_path / source_name / partition
        q_dir.mkdir(parents=True, exist_ok=True)
        out_file = q_dir / f"{source_name}_bad.parquet"
        pd.DataFrame(bad).to_parquet(out_file, index=False)
        log_event(logger, "WARNING", f"{source_name}_quarantined", count=len(bad))

    result = pd.DataFrame(good) if good else pd.DataFrame(columns=df.columns)

    # Deduplicate on primary key — keep last occurrence
    if primary_key in result.columns and not result.empty:
        before = len(result)
        result = result.drop_duplicates(subset=[primary_key], keep="last")
        dupes = before - len(result)
        if dupes:
            log_event(logger, "INFO", f"{source_name}_deduped", dropped=dupes)

    return result.reset_index(drop=True)


def build_silver_orders(
    date_str: str,
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    logger: logging.Logger,
) -> Path:
    src = bronze_dir / "orders" / f"date={date_str}" / "data.parquet"
    if not src.exists():
        log_event(logger, "WARNING", "silver_orders_no_bronze", date=date_str)
        return silver_dir / "orders" / f"date={date_str}" / "data.parquet"

    df = pd.read_parquet(src)
    df = _validate_df(df, OrderRow, "order_id", quarantine_dir, logger, "orders", date_str)

    out_dir = silver_dir / "orders" / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "silver_orders_written", rows=len(df), date=date_str)
    return out_path


def build_silver_customers(
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    logger: logging.Logger,
    date_str: str = "",
) -> Path:
    src = bronze_dir / "customers" / "data.parquet"
    if not src.exists():
        log_event(logger, "WARNING", "silver_customers_no_bronze")
        return silver_dir / "customers" / "data.parquet"

    df = pd.read_parquet(src)
    df = _validate_df(df, CustomerRow, "customer_id", quarantine_dir, logger, "customers", date_str)

    out_dir = silver_dir / "customers"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "silver_customers_written", rows=len(df))
    return out_path


def build_silver_products(
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    logger: logging.Logger,
    date_str: str = "",
) -> Path:
    src = bronze_dir / "products" / "data.parquet"
    if not src.exists():
        log_event(logger, "WARNING", "silver_products_no_bronze")
        return silver_dir / "products" / "data.parquet"

    df = pd.read_parquet(src)
    if df.empty:
        return silver_dir / "products" / "data.parquet"

    df = _validate_df(df, ProductRow, "product_id", quarantine_dir, logger, "products", date_str)

    out_dir = silver_dir / "products"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "silver_products_written", rows=len(df))
    return out_path
