# coding: utf-8
import os
import re
import sys
from pathlib import Path
from icecream import ic


from dataclasses import dataclass
from datetime import datetime

import pandas as pd
from icecream import ic

from tqdm import tqdm
from dotenv import load_dotenv
from scripts.cleaning_pipeline.assembler import DataAssembler
from scripts.cleaning_pipeline.lagoon_flavoring import TransformerPipe
from scripts.cleaning_pipeline.transformer import Transformer


# load environment variables
load_dotenv('scripts/.env')
# collection path / contains raw extracted data from start_extract()
COLLECTION_PATH = Path(os.getenv("COLLECTION_PATH"))
# date collection path / contains a new column 'date' based from collection path
DATE_COLLECTION_PATH = Path(os.getenv("DATE_COLLECTION_PATH"))
# parquet path / contains the dataframe used for analytics
PARQUET_PATH = Path(os.getenv("PARQUET_PATH"))
# json path / contains the dataframe used for business-requirements analytics
JSON_PATH = Path(os.getenv("JSON_PATH"))


class RawData:
    """
    Raw data holds all the dataframe from batching. It's only
    used when doing a batch transformation.
    """

    def __init__(self, path):
        self.win_path: pathlib.WindowsPath = path
        self.file_name: str = path.name
        self.data: pd.DataFrame = pd.read_csv(path)


def clean_transform(dataframe) \
        -> DataAssembler:
    """
    Transforms dataframe using Transformer and Data Assembler pipeline.
    :param dataframe:
    :return:
    """
    assembler = Transformer(dataframe, TransformerPipe)
    data_assembler = DataAssembler(assembler.transformed_df)
    return data_assembler


def read_updated() \
        -> pd.DataFrame:
    """
    Read the saved food & beverage parquet data. and returns
    a copy of it.
    :return:
    """
    fb_data = pd.read_parquet(PARQUET_PATH)

    return fb_data.copy()


def get_latest_collection(dataframe: pd.DataFrame) \
        -> str:
    """

    :return:
    """
    latest_data = dataframe.date.sort_values(ascending=False) \
        .reset_index() \
        .iloc[0] \
        .to_frame() \
        .T

    latest_data['date'] = pd.to_datetime(latest_data.date)
    latest_entry_string = latest_data.date.dt.strftime('%m%d%Y')[0]
    return latest_entry_string


def get_csv_list(raw_filepath: Path) \
        -> list:
    """

    :param raw_filepath:
    :return:
    """
    csv_list_from_raw = [i for i in raw_filepath.iterdir() if i.suffix == '.csv']
    return csv_list_from_raw


def get_latest_date(dataframe):
    """

    :return:
    """
    updated = [i.name for i in get_csv_list(Path(COLLECTION_PATH))]
    all_latest_collection = [i[:8] for i in updated]
    search = [re.search(get_latest_collection(dataframe), x) for x in all_latest_collection]

    match_loc = []

    for i, x in enumerate(search):
        if x is not None:
            match_loc.append(i)

    return updated[match_loc[0] + 1:]


def get_latest_modified_file(directory: Path):
    """

    :return:
    """
    latest_file = None
    latest_mtime = 0

    for file in directory.iterdir():
        mtime = os.path.getmtime(file)
        if mtime > latest_mtime:
            latest_mtime = mtime
            latest_file = file

    return latest_file


def get_updated_dataframe(dataframe):
    """

    :return:
    """

    if len(dataframe) > 1:

        to_append_dataframe = [pd.read_csv(DATE_COLLECTION_PATH / i) for i in get_latest_date(dataframe)]
        appended_dataframe = pd.concat(to_append_dataframe, ignore_index=True)

        return appended_dataframe

    if len(dataframe) == 1:

        latest_modified = get_latest_modified_file(DATE_COLLECTION_PATH)
        latest_modified_df = pd.DataFrame(latest_modified)

        return latest_modified_df

