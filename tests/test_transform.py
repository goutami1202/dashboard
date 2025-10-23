import pandas as pd
from pathlib import Path
def test_outputs_exist():
    out_dir = Path('outputs')
    assert (out_dir / 'CT_Analysis_Output.csv').exists()
    assert (out_dir / 'TUS_Analysis_Output.csv').exists()
