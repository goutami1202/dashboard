#!/usr/bin/env python3
"""
preprocess_upload.py
Automatically cleans, normalizes, and fills required columns for the FinTech Dashboard pipeline.
Ensures columns like Date_Time, Result, and PCode always exist.
"""

import sys
import os
import pandas as pd
from pathlib import Path

# Columns your pipeline expects — add here if new ones appear later
REQUIRED_COLUMNS = ["Date_Time", "Result", "PCode"]

def normalize_columns(df):
    """Standardize column names: strip, replace spaces, remove symbols."""
    df.columns = (
        df.columns.str.strip()
        .str.replace(r"[^0-9a-zA-Z]+", "_", regex=True)
        .str.strip("_")
    )
    return df

def ensure_datetime_column(df):
    """Ensure 'Date_Time' column exists."""
    if "Date_Time" in df.columns:
        return df

    # Detect possible date/time columns
    candidates = [c for c in df.columns if any(x in c.lower() for x in ["date", "time", "timestamp", "created", "datetime"])]
    if candidates:
        chosen = candidates[0]
        try:
            df["Date_Time"] = pd.to_datetime(df[chosen], errors="coerce")
        except Exception:
            df["Date_Time"] = pd.to_datetime(df[chosen].astype(str), errors="coerce")
        print(f"✅ Mapped '{chosen}' → 'Date_Time'", file=sys.stderr)
    else:
        df["Date_Time"] = pd.date_range("2025-01-01", periods=len(df), freq="H")
        print("⚠️ No timestamp column found; synthetic 'Date_Time' created.", file=sys.stderr)

    df = df[df["Date_Time"].notna()]
    return df

def ensure_required_columns(df):
    """Add any missing required columns with default placeholders."""
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = "Unknown"
            print(f"⚠️ Missing column '{col}' — added placeholder values.", file=sys.stderr)
    return df

def clean_numeric_columns(df):
    """Convert currency / numeric columns to float where possible."""
    for c in df.columns:
        if df[c].dtype == object:
            try:
                df[c] = (
                    df[c]
                    .astype(str)
                    .str.replace(",", "")
                    .str.replace("₹", "")
                    .str.replace("$", "")
                    .str.replace("%", "")
                    .str.strip()
                )
                # Convert to float if numeric
                try:
                    df[c] = pd.to_numeric(df[c])
                except (ValueError, TypeError):
                    pass  # Keep as string if conversion fails
            except Exception:
                continue
    return df

def preprocess_file(path):
    """Load, clean, and save preprocessed data."""
    ext = Path(path).suffix.lower()
    if ext not in [".csv", ".xlsx", ".xls"]:
        raise ValueError(f"Unsupported file type: {ext}")

    if ext == ".csv":
        df = pd.read_csv(path)
    else:
        df = pd.read_excel(path, engine="openpyxl")

    df = normalize_columns(df)
    df = ensure_datetime_column(df)
    df = clean_numeric_columns(df)
    df = ensure_required_columns(df)

    df.dropna(axis=0, how="all", inplace=True)
    df.dropna(axis=1, how="all", inplace=True)

    cleaned_path = Path("uploads") / (Path(path).stem + "_cleaned.csv")
    os.makedirs(cleaned_path.parent, exist_ok=True)
    df.to_csv(cleaned_path, index=False)

    print(cleaned_path)  # web_app.py reads this stdout
    return cleaned_path

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 preprocess_upload.py <path_to_file>", file=sys.stderr)
        sys.exit(1)

    file_path = sys.argv[1]
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}", file=sys.stderr)
        sys.exit(2)

    try:
        preprocess_file(file_path)
    except Exception as e:
        print(f"❌ Error during preprocessing: {e}", file=sys.stderr)
        sys.exit(3)

if __name__ == "__main__":
    main()