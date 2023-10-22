# coding: utf-8
import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

import janitor
import janitor.ml
import numpy as np
import pandas as pd
from tqdm import tqdm
from typing import Any
from icecream import ic

load_dotenv('scripts/.env')

# collection path / contains raw extracted data from start_extract()
COLLECTION_PATH = Path(os.getenv("COLLECTION_PATH"))

# date collection path / contains a new column 'date' based from collection path
DATE_COLLECTION_PATH = Path(os.getenv("DATE_COLLECTION_PATH"))


class NoUpdates(Exception):
    def __init__(self, message="No new updates from collection path (raw data)"):
        self.message = message
        super().__init__(self.message)


def data_names_to_series(path_of_data) \
        -> np.ndarray[Any]:
    """
    Access and extract names from the directory and get values
    :param path_of_data: Contains all data from raw collection path
    :return: Date values
    """
    data = pd.Series(
        [i.name.strip('_market.csv') for i in path_of_data.iterdir() if i.suffix == '.csv'],
        name='market_collection') \
        .to_frame()

    data_values = np.ravel(data.values)
    return data_values


class UpdateManager:

    def __init__(self, old_dp, new_dp):
        self.old_data_path: Path = old_dp
        self.new_data_path: path = new_dp

    def get_updates(self) \
            -> Path:
        """
        Select new files from extraction.
        :return: Path of new files.
        """
        raw = data_names_to_series(self.old_data_path)
        updated = data_names_to_series(self.new_data_path)

        try:
            new_data = list(set(raw) - set(updated))[0]
            file_name = new_data + '_market.csv'
            return self.old_data_path / file_name

        except (NoUpdates, IndexError):
            raise NoUpdates()


class MarketData:

    def __init__(self, path):
        self.win_path: pathlib.WindowsPath = path
        self.file_name: str = path.name
        self.data: pd.DataFrame = pd.read_csv(path, index_col=0)

    @property
    def insert_date(self):
        collection_date = self.file_name.strip('_market.csv')
        self.data = self.data.assign(date=collection_date)

        self.data['date'] = pd.to_datetime(
            self.data['date'],
            format="%m%d%Y-%H%M")

        self.data = self.data.reorder_columns(['date', 'itemid'])

    def dump(self):
        self.data.to_csv(DATE_COLLECTION_PATH / self.file_name, index=0)


if __name__ == '__main__':

    # update manager class
    manager = UpdateManager(COLLECTION_PATH, DATE_COLLECTION_PATH).get_updates()

    updated_data_list = [manager]
    for i in tqdm(updated_data_list):
        m = MarketData(path=i)
        m.insert_date
        m.dump()

    ic(pd.Series(updated_data_list, name='market_collection') \
        .to_frame())