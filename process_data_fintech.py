#!/usr/bin/env python3
"""process_data_fintech.py

Fintech-hardened pipeline...
"""
from __future__ import annotations
import os, sys, argparse, json, hashlib
from datetime import datetime
from pathlib import Path
from typing import Optional, List
import pandas as pd, numpy as np
try:
    import pandera as pa
    from pandera import Column, DataFrameSchema, Check
except Exception:
    pa = None
import logging
class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "timestamp": datetime.utcnow().isoformat() + 'Z',
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "extra") and isinstance(record.extra, dict):
            payload.update(record.extra)
        return json.dumps(payload)
logger = logging.getLogger("process_data_fintech")
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)
DEFAULT_OUT_DIR = os.environ.get("OUT_DIR", "./outputs")
def read_any_table(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        logger.error("Input not found", extra={"extra":{"path":str(path)}})
        raise FileNotFoundError(path)
    ext = p.suffix.lower()
    if ext == ".csv": return pd.read_csv(path)
    elif ext in (".xls", ".xlsx"): return pd.read_excel(path, sheet_name=0)
    else: raise ValueError("Unsupported extension: " + ext)
def compute_row_hash(row: pd.Series) -> str:
    s = f"{row.get('Station_ID','')}|{row.get('Date_Time','')}|{row.get('PCode','')}|{row.get('Result','')}"
    return hashlib.sha256(str(s).encode('utf-8')).hexdigest()
def normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns.astype(str)]
    df['Date_Time'] = pd.to_datetime(df['Date_Time'], errors='coerce')
    df['Result'] = pd.to_numeric(df['Result'], errors='coerce')
    df['PCode'] = df['PCode'].astype(str).str.strip()
    df['Station_ID'] = df['Station_ID'].astype(str).str.strip()
    return df
def pivot_station(df: pd.DataFrame, station: str, template_cols: Optional[List[str]] = None, agg: str = "mean") -> pd.DataFrame:
    sub = df[df['Station_ID'] == station].copy()
    if sub.empty:
        logger.warning("No data for station", extra={"extra":{"station":station}})
        cols = ['Dates'] + (list(template_cols) if template_cols else [])
        return pd.DataFrame(columns=cols)
    sub['Date'] = pd.to_datetime(sub['Date_Time']).dt.date
    aggfunc = {'mean':'mean','median':'median','sum':'sum','first':'first'}.get(agg,'mean')
    pivot = sub.groupby(['Date','PCode'], dropna=True)['Result'].agg(aggfunc).reset_index().pivot(index='Date', columns='PCode', values='Result')
    pivot.columns = [str(c) for c in pivot.columns]
    pivot = pivot.sort_index()
    if template_cols is not None: pivot = pivot.reindex(columns=template_cols)
    out = pivot.reset_index().rename(columns={'Date':'Dates'})
    out.insert(0,'Station', station)
    return out
def write_csv_secure(df: pd.DataFrame, path: str) -> None:
    p = Path(path); tmp = p.with_suffix('.tmp'); df.to_csv(tmp, index=False); tmp.replace(p)
def write_audit_log(raw_df: pd.DataFrame, out_dir: str) -> None:
    path = Path(out_dir) / "audit_lineage.csv"; rows = []
    for idx, row in raw_df.iterrows():
        rows.append({"index": int(idx), "station_id": row.get('Station_ID'), "date_time": row.get('Date_Time'), "pcode": row.get('PCode'), "result": row.get('Result'), "row_hash": compute_row_hash(row), "processed_at": datetime.utcnow().isoformat() + 'Z'})
    pd.DataFrame(rows).to_csv(path, index=False)
def main(args):
    out_dir = Path(args.out_dir or os.environ.get('OUT_DIR') or DEFAULT_OUT_DIR); out_dir.mkdir(parents=True, exist_ok=True)
    raw = read_any_table(args.raw); raw = normalize(raw)
    try: validated = raw
    except Exception as e: validated = raw
    write_audit_log(validated, str(out_dir))
    ct_template = read_any_table(args.ct_template) if args.ct_template else None
    tus_template = read_any_table(args.tus_template) if args.tus_template else None
    ct_cols = [c for c in ct_template.columns.astype(str) if c not in ('Station','Dates')] if ct_template is not None else None
    tus_cols = [c for c in tus_template.columns.astype(str) if c not in ('Station','Dates')] if tus_template is not None else None
    ct_out = pivot_station(validated, 'CT', template_cols=ct_cols, agg=args.agg)
    tus_out = pivot_station(validated, 'TUS', template_cols=tus_cols, agg=args.agg)
    for df in (ct_out, tus_out): df['generated_at'] = datetime.utcnow().isoformat() + 'Z'; df['pipeline_version'] = args.version or '0.1.0'
    ct_path = out_dir / (args.ct_out or 'CT_Analysis_Output.csv'); tus_path = out_dir / (args.tus_out or 'TUS_Analysis_Output.csv')
    write_csv_secure(ct_out, str(ct_path)); write_csv_secure(tus_out, str(tus_path))
    print('Processing complete')
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--raw', required=True)
    parser.add_argument('--ct_template')
    parser.add_argument('--tus_template')
    parser.add_argument('--out_dir')
    parser.add_argument('--ct_out')
    parser.add_argument('--tus_out')
    parser.add_argument('--agg', default='mean')
    parser.add_argument('--version')
    args = parser.parse_args()
    main(args)
