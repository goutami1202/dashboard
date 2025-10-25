import pandas as pd
import plotly.express as px
import plotly.io as pio
import argparse
import sys
from pathlib import Path

def prepare(df, station):
    cols = [c for c in df.columns if c not in ('Station','Dates','generated_at','pipeline_version')]
    melt = df.melt(id_vars=['Dates'], value_vars=cols, var_name='PCode', value_name='Value')
    melt['Station'] = station
    melt['Dates'] = pd.to_datetime(melt['Dates'])
    return melt

def main():
    parser = argparse.ArgumentParser(description='Generate dashboard from CT and TUS analysis files')
    parser.add_argument('--ct_input', help='CT analysis input file (CSV)')
    parser.add_argument('--tus_input', help='TUS analysis input file (CSV)')
    parser.add_argument('--output', default='dashboard.html', help='Output HTML filename')
    parser.add_argument('--out_dir', default='outputs', help='Output directory')
    
    args = parser.parse_args()
    
    out_dir = Path(args.out_dir)
    out_dir.mkdir(exist_ok=True)
    
    data_frames = []
    
    # Read CT data if file exists
    if args.ct_input and Path(args.ct_input).exists():
        try:
            ct = pd.read_csv(args.ct_input)
            ct_melt = prepare(ct, "CT")
            data_frames.append(ct_melt)
            print(f"✅ Loaded CT data from {args.ct_input}")
        except Exception as e:
            print(f"⚠️ Error loading CT data from {args.ct_input}: {e}")
    elif Path(out_dir / "CT_Analysis_Output.csv").exists():
        try:
            ct = pd.read_csv(out_dir / "CT_Analysis_Output.csv")
            ct_melt = prepare(ct, "CT")
            data_frames.append(ct_melt)
            print("✅ Loaded default CT data")
        except Exception as e:
            print(f"⚠️ Error loading default CT data: {e}")
    
    # Read TUS data if file exists
    if args.tus_input and Path(args.tus_input).exists():
        try:
            tus = pd.read_csv(args.tus_input)
            tus_melt = prepare(tus, "TUS")
            data_frames.append(tus_melt)
            print(f"✅ Loaded TUS data from {args.tus_input}")
        except Exception as e:
            print(f"⚠️ Error loading TUS data from {args.tus_input}: {e}")
    elif Path(out_dir / "TUS_Analysis_Output.csv").exists():
        try:
            tus = pd.read_csv(out_dir / "TUS_Analysis_Output.csv")
            tus_melt = prepare(tus, "TUS")
            data_frames.append(tus_melt)
            print("✅ Loaded default TUS data")
        except Exception as e:
            print(f"⚠️ Error loading default TUS data: {e}")
    
    if not data_frames:
        print("❌ No data files found to generate dashboard")
        sys.exit(1)
    
    # Combine all data
    df_all = pd.concat(data_frames, ignore_index=True)
    
    # Generate dashboard
    fig = px.line(df_all, x="Dates", y="Value", color="PCode", facet_col="Station",
                  title="CT and TUS Station Time Series", markers=True)
    
    output_path = out_dir / args.output
    pio.write_html(fig, file=str(output_path), full_html=True, include_plotlyjs="cdn")
    print(f"✅ Dashboard generated at {output_path}")

if __name__ == "__main__":
    main()
