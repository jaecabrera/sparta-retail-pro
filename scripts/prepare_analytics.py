# A data transformation script for clients: e.g. Quaker
import os
from pathlib import Path

import janitor
import pandas as pd
import pandas_flavor as pf


def transform_analytics(parquet_path: Path)\
        -> pd.DataFrame:
    """
    Transforms the dataframe to create aggregations and business requirements.

    :return: Complete dataframe ready for dashboard
    :rtype: Pandas DataFrame
    """
    adf = load_quaker_from_parquet(parquet_path)
    adf = adf.expand_datetime()
    get_diff_units_sold(adf)

    return adf


def load_quaker_from_parquet(parquet_path: Path) \
        -> pd.DataFrame:
    """
    Load parquet dataframe. from data/analytics directory

    :param parquet_path:
    :type parquet_path: Path
    :return: Dataframe output of extraction and cleaning pipeline
    :rtype: Pandas DataFrame
    """
    df = pd.read_parquet(parquet_path)
    quaker = df.filter_column_isin('brand', ['Quaker'])

    return quaker


@pf.register_dataframe_method
def expand_datetime(df) \
        -> pd.DataFrame:
    """
    Create month name and day feature.
    :param df:
    :return: Pandas DataFrame with month name and day.
    :rtype: Pandas DataFrame
    """

    df['month'] = df.date.dt.month_name()
    df['day'] = df.date.dt.day

    return df


def get_diff_units_sold(quaker_dataframe: pd.DataFrame)\
        -> None:
    """
    Create a new series as difference for units_sold feature, similar
    to a running total.
    :param quaker_dataframe: Filtered Dataframe from load_parquet_file
    :type quaker_dataframe: Pandas DataFrame
    """
    all_quaker_items = quaker_dataframe.itemid.unique()

    def calculate_difference(df, item_id)\
            -> None:
        """

        :param df:
        :param item_id:
        """
        filtered_data = df[df.itemid == item_id]
        idx_filtered_data = filtered_data.index.to_list()
        sorted_data = filtered_data.sort_values('date')
        units_sold_diff = sorted_data.units_sold.diff()
        df.loc[idx_filtered_data, 'units_sold_diff'] = units_sold_diff

    for i in all_quaker_items:
        calculate_difference(quaker_dataframe, i)
