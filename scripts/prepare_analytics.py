# A data transformation script for clients: e.g. Quaker
import os
from pathlib import Path
from typing import Tuple, Any, List, Dict

import janitor
import pandas as pd
import pandas_flavor as pf
from pandas import DataFrame
import re


def transform_analytics(parquet_path: Path) \
        -> dict[str, DataFrame]:
    """
    Transforms the dataframe to create aggregations and business requirements.

    :return: Complete dataframe ready for dashboard
    :rtype: Pandas DataFrame
    """
    pc = load_client_data(parquet_path)

    for k, v in pc.items():
        v.expand_datetime()
        get_diff_units_sold(v)

    pepsi, coke = pc.get('Pepsi'), pc.get('Coke')
    pep_transformed = create_product_name(df=pepsi, data_business_key='Pepsi')
    ck_transformed = create_product_name(df=coke, data_business_key='Coke')

    analytics_data = pd.concat([pep_transformed, ck_transformed], axis=0).reset_index(drop=True)
    return analytics_data


def load_client_data(parquet_path: Path) \
        -> dict[str, DataFrame]:
    """
    Load parquet dataframe. from data/analytics directory

    :param parquet_path:
    :type parquet_path: Path
    :return: Dataframe output of extraction and cleaning pipeline
    :rtype: Pandas DataFrame
    """
    df = pd.read_parquet(parquet_path)
    pepsi = df.filter_column_isin('brand', ['Pepsi']).reset_index(drop=True)

    # filter pepsi
    pepsi_filtered = pepsi[pepsi.name.str.contains('Pepsi')].reset_index(drop=True)

    coke = df.filter_column_isin('brand', ['Coca-Cola']).reset_index(drop=True)
    client_data = {'Pepsi': pepsi_filtered, 'Coke': coke}
    return client_data


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


def get_diff_units_sold(quaker_dataframe: pd.DataFrame) \
        -> None:
    """
    Create a new series as difference for units_sold feature, similar
    to a running total.
    :param quaker_dataframe: Filtered Dataframe from load_parquet_file
    :type quaker_dataframe: Pandas DataFrame
    """
    all_quaker_items = quaker_dataframe.itemid.unique()

    def calculate_difference(df, item_id) \
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


def create_product_name(df: pd.DataFrame, data_business_key: str):

    if data_business_key == 'Pepsi':

        pepsi_products = ["Regular Soda", "Black"]
        df_filtered = df[df.name.str.contains('Pepsi')].reset_index(drop=True)
        df_filtered['product_name'] = [pepsi_products[0] if re.search(pepsi_products[0], i) else "Pepsi Black" for i in
                                       df_filtered.name]

        return df

    if data_business_key == 'Coke':

        coke_products = {
            "Coca-Cola No Sugar": "No Sugar",
            "Coca-Cola Zero Sugar": "No Sugar",
            "Coca-Cola Original Taste": "Original Taste",
            "Coca-Cola Light": "Light",
            "Coca-Cola Light Taste": "Light",
            "Coca-Cola Zero Sugar Ultimate Collection": "Zero",
            "Coca-Cola Zero Mini Can": "Zero",
            "Coca-Cola Zero Sugar": "Zero",
            "Coca-Cola Zero Sugar Ultimate Collection": "Zero"
        }

        df['product_name'] = df.name.copy(deep=True)
        janitor.find_replace(df, match="regex", product_name=coke_products)

        multi_pack_index = df.filter_string('name', '\sMultipack', regex=True, complement=False).index
        df.loc[multi_pack_index, 'pack_type'] = 'multi-pack'

        return df
