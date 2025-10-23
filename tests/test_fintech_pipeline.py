import subprocess, sys
from pathlib import Path
def test_fintech_pipeline_runs_and_writes_outputs(tmp_path):
    data_dir = Path('.').resolve()
    out_dir = data_dir / 'outputs'
    cmd = [sys.executable, str(data_dir / 'process_data_fintech.py'),
           '--raw', str(data_dir / '1 Raw Data.xlsx'),
           '--ct_template', str(data_dir / '2 CT Analysis.xlsx'),
           '--tus_template', str(data_dir / '3 TUS Analysis.xlsx'),
           '--out_dir', str(out_dir),
           '--version', '0.1.0']
    res = subprocess.run(cmd, check=False)
    assert res.returncode == 0
    assert (out_dir / 'CT_Analysis_Output.csv').exists()
    assert (out_dir / 'TUS_Analysis_Output.csv').exists()
    assert (out_dir / 'audit_lineage.csv').exists()
