import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import sys

def read_any(path):
    ext = Path(path).suffix.lower()
    if ext in (".xls", ".xlsx"):
        return pd.read_excel(path, engine="openpyxl")
    return pd.read_csv(path, low_memory=False)

def find_column(df, keywords):
    for c in df.columns:
        name = str(c).strip().lower().replace(" ", "_")
        for key in keywords:
            if key in name:
                return c
    return None

def normalize_any_file(path):
    df = read_any(path)
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # --- Detect core columns ---
    date_col = find_column(df, ["date", "time", "timestamp", "datetime", "recorded"])
    station_col = find_column(df, ["station", "id", "branch", "location", "sensor"])
    result_col = find_column(df, ["value", "amount", "result", "reading", "score", "price", "metric"])

    # --- Create base DataFrame ---
    if date_col:
        try:
            df["Date_Time"] = pd.to_datetime(df[date_col], errors="coerce")
        except Exception:
            df["Date_Time"] = pd.to_datetime("today")
    else:
        df["Date_Time"] = [datetime.today() - timedelta(minutes=i) for i in range(len(df))]

    # --- Assign Station_ID ---
    if station_col:
        df["Station_ID"] = df[station_col].astype(str).fillna("CT")
    else:
        df["Station_ID"] = "CT"

    # --- Detect numeric columns for dynamic PCode mapping ---
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    # If no numeric columns, use any detected "Result" or create dummy
    if not numeric_cols and result_col:
        numeric_cols = [result_col]
    elif not numeric_cols:
        # If truly nothing numeric, create one dummy numeric column
        df["Random_Result"] = np.random.uniform(0, 100, size=len(df))
        numeric_cols = ["Random_Result"]

    # --- Melt into PCode / Result structure ---
    df_melted = df.melt(
        id_vars=["Station_ID", "Date_Time"],
        value_vars=numeric_cols,
        var_name="PCode",
        value_name="Result"
    )

    # Clean up
    df_melted["PCode"] = df_melted["PCode"].astype(str).fillna("X1")
    df_melted["Result"] = pd.to_numeric(df_melted["Result"], errors="coerce").fillna(0)

    # --- Save normalized CSV ---
    out_path = str(path) + ".normalized.csv"
    df_melted.to_csv(out_path, index=False)

    return out_path


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python preprocess_upload.py <path>")
        sys.exit(2)

    path = sys.argv[1]
    try:
        out = normalize_any_file(path)
        print(out)
    except Exception as e:
        print(path)