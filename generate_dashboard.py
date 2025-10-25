import pandas as pd
import plotly.express as px
import plotly.io as pio
from pathlib import Path

out_dir = Path("outputs")
ct = pd.read_csv(out_dir / "CT_Analysis_Output.csv")
tus = pd.read_csv(out_dir / "TUS_Analysis_Output.csv")

def prepare(df, station):
    cols = [c for c in df.columns if c not in ('Station','Dates','generated_at','pipeline_version')]
    melt = df.melt(id_vars=['Dates'], value_vars=cols, var_name='PCode', value_name='Value')
    melt['Station'] = station
    melt['Dates'] = pd.to_datetime(melt['Dates'])
    return melt

ct_melt = prepare(ct, "CT")
tus_melt = prepare(tus, "TUS")
df_all = pd.concat([ct_melt, tus_melt])

fig = px.line(df_all, x="Dates", y="Value", color="PCode", facet_col="Station",
              title="CT and TUS Station Time Series", markers=True)

pio.write_html(fig, file="outputs/dashboard.html", full_html=True, include_plotlyjs="cdn")
print("✅ Dashboard generated at outputs/dashboard.html")
