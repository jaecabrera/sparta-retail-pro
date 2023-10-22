import os
import stat
from pathlib import Path

import pandas as pd

from extract import sgtime as sgtz
from icecream import ic


class Loader:

    @staticmethod
    def load_csv(delayed: bool, script_path: Path):
        raw_data_path_dir = script_path / "data" / "raw"

        # go to raw output files, list them, and store to raw_names var
        os.chmod(raw_data_path_dir, stat.S_IRWXO)
        output_dir: Path = script_path / "data" / "raw"
        raw_names = os.listdir(output_dir)
        ic(output_dir)
        ic(raw_names)
        if sgtz.now().split('-')[0] == raw_names[-1].split('-')[0]:
            ic(sgtz.now().split('-')[0])

            # If present day found then proceed to transform.
            to_transform = raw_names[-1]
            raw = pd.read_csv(output_dir / str(to_transform), index_col=0)
            ic(raw)
            return raw

        elif delayed:
            to_transform = raw_names[-1]
            raw = pd.read_csv(output_dir / str(to_transform), index_col=0)
            return raw
