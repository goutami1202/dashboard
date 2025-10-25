from pathlib import Path
import pandas as pd, numpy as np
from datetime import datetime, timedelta
def generate(days=60, stations=('CT','TUS'), pcode_count=12, seed=42):
    np.random.seed(seed); start = datetime(2021,1,1); rows = []
    pcodes = [f'Data {i+1}' for i in range(pcode_count)]
    for s in stations:
        for d in range(days):
            date = start + timedelta(days=d)
            for p in pcodes:
                mean = 10 + (hash(s) % 5) + (int(p.split()[1]) % 7)
                val = np.random.normal(loc=mean, scale=2.0)
                if np.random.rand() < 0.05: val = None
                rows.append({'Station_ID': s, 'Date_Time': date.strftime('%Y-%m-%d'), 'PCode': p, 'Result': val})
    df = pd.DataFrame(rows); return df
if __name__ == '__main__':
    df = generate(days=60, pcode_count=12)
    out = Path('outputs') / 'synthetic_raw_data.csv'; out.parent.mkdir(parents=True, exist_ok=True); df.to_csv(out, index=False); print('Wrote', out)
