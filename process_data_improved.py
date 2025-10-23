#!/usr/bin/env python3
from __future__ import annotations
import argparse, os
from pathlib import Path
from typing import Optional
import pandas as pd
import numpy as np
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
def read_any_table(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    ext = p.suffix.lower()
    if ext == '.csv': return pd.read_csv(path)
    elif ext in ('.xls', '.xlsx'): return pd.read_excel(path, sheet_name=0)
    else: raise ValueError("Unsupported type")
def normalize_raw_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy(); df.columns = [c.strip() for c in df.columns.astype(str)]
    df['Date_Time'] = pd.to_datetime(df['Date_Time'], errors='coerce')
    df['Result'] = pd.to_numeric(df['Result'], errors='coerce')
    df['PCode'] = df['PCode'].astype(str).str.strip()
    df['Station_ID'] = df['Station_ID'].astype(str).str.strip()
    return df
def pivot_station(df: pd.DataFrame, station: str, template_cols: Optional[list]=None, agg='mean'):
    sub = df[df['Station_ID']==station].copy()
    if sub.empty: return pd.DataFrame(columns=['Station','Dates'] + (template_cols or []))
    sub['Date'] = pd.to_datetime(sub['Date_Time']).dt.date
    aggfunc = {'mean':'mean','median':'median','sum':'sum','first':'first'}.get(agg,'mean')
    pivot = sub.groupby(['Date','PCode'])['Result'].agg(aggfunc).reset_index().pivot(index='Date', columns='PCode', values='Result')
    pivot.columns = [str(c) for c in pivot.columns]
    if template_cols: pivot = pivot.reindex(columns=template_cols)
    out = pivot.reset_index().rename(columns={'Date':'Dates'})
    out.insert(0,'Station', station)
    return out
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--raw', required=True)
    parser.add_argument('--ct_template', required=False)
    parser.add_argument('--tus_template', required=False)
    parser.add_argument('--out_dir', default='outputs')
    parser.add_argument('--agg', default='mean')
    args = parser.parse_args()
    raw = read_any_table(args.raw); raw = normalize_raw_df(raw)
    ct_template = read_any_table(args.ct_template) if args.ct_template else None
    tus_template = read_any_table(args.tus_template) if args.tus_template else None
    ct_cols = [c for c in ct_template.columns.astype(str) if c not in ('Station','Dates')] if ct_template is not None else None
    tus_cols = [c for c in tus_template.columns.astype(str) if c not in ('Station','Dates')] if tus_template is not None else None
    ct_out = pivot_station(raw, 'CT', template_cols=ct_cols, agg=args.agg)
    tus_out = pivot_station(raw, 'TUS', template_cols=tus_cols, agg=args.agg)
    os.makedirs(args.out_dir, exist_ok=True)
    ct_out.to_csv(os.path.join(args.out_dir, 'CT_Analysis_Output.csv'), index=False)
    tus_out.to_csv(os.path.join(args.out_dir, 'TUS_Analysis_Output.csv'), index=False)
    logger.info("Done")
if __name__ == '__main__': main()
