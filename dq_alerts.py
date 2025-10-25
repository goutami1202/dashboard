from typing import Dict, List, Any
import pandas as pd, numpy as np
def compute_missing_rates(df: pd.DataFrame) -> Dict[str, float]:
    rates = {}
    for c in df.columns:
        rates[c] = df[c].isna().mean()
    return rates
def detect_outliers_zscore(df: pd.DataFrame, cols: List[str], z_thresh: float = 3.0) -> Dict[str, int]:
    out = {}
    for c in cols:
        series = pd.to_numeric(df[c], errors='coerce').dropna()
        if series.empty:
            out[c] = 0; continue
        mean = series.mean(); std = series.std(ddof=0)
        if std == 0 or np.isnan(std): out[c] = 0; continue
        z = (series - mean) / std; out[c] = int((z.abs() > z_thresh).sum())
    return out
def check_thresholds(missing_rates: Dict[str,float], outlier_counts: Dict[str,int], rules: Dict[str,Any]) -> List[Dict[str,Any]]:
    alerts = []; max_missing = rules.get('max_missing', 0.2)
    for c, r in missing_rates.items():
        if r >= max_missing: alerts.append({'type':'missing_rate', 'column':c, 'rate':float(r), 'threshold':max_missing})
    max_outliers = rules.get('max_outliers', 10)
    for c, cnt in outlier_counts.items():
        if cnt >= max_outliers: alerts.append({'type':'outliers', 'column':c, 'count':int(cnt), 'threshold':max_outliers})
    return alerts
def send_alert_stub(alert: Dict[str,Any]): print('ALERT', alert)
