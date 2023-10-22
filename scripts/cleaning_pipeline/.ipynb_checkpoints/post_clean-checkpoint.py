import pandas as pd
import re
import numpy as np
from dataclasses import dataclass

from pands.core.common import flatten
import janitor
import warnings
from pathlib import Path
from datetime import datetime


@dataclass(init=False)
class PostCleaner:

    def __init__(self, basic, selling, rating):
        self.basic = basic
        self.rating = rating
        self.selling = selling
        self.con_features = [
            'itemid',
            'brand',
            'category_names',
            'category',
            'name',
            'pack_size',
            'pack_type']
        self.var_features = [
            'price_median',
            'raw_discount',
            'price_before_discount',
            'units_sold',
            'stock',
            'status',
            'item_page',
            'like_count',
            'comment_count',
            'views',
            'no_rating',
            'star_1',
            'star_2',
            'star_3',
            'star_4',
            'star_5',
            'rating',
            'low_price_guarantee',
            'on_flash_sale',
            'can_use_bundle_deal',
            'can_use_cod',
            'can_use_wholesale',
            'show_free_shipping']

    def process_basic(self):

        self.basic = self.basic['brand'].str.lower()
        self.basic = self.basic['name'].str.lower()
        self.basic = self.basic.sort_values('brand')

    def create_document(self):
        root = self.basic[self.con_features]
        node = pd.concat([self.rating, self.selling], axis=1)
        node = node[self.var_features]
        node.to_json()





def add_indicator_feature(expression:re.Pattern, dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    :description:
            Search for string pattern from Pandas Series and
        map match with 0 or 1.
    :expression:
    :expression type: re.Pattern
    :dataframe:
    :dataframe type: pd.DataFrame
    :return: pd.DataFrame
    """
    return dataframe.name \
        .apply(lambda x: 1 if expression.search(x) else 0)


def extract_indicator_feature(expression:re.Pattern, dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    :description:

            Search for string pattern from Pandas Series and
        extract the pattern and apply it to a new series.

    :param expression: Regex Compiled Expression.
    :expression type: re.Pattern
    :param dataframe:
    :dataframe type:
    :return: pd.DataFrame
    """
    return dataframe.name \
        .apply(lambda x: expression.search(x).group() if expression.search(x) else 0)


def clean_name_strings_from_units(clean_brand_names):
    """
    :description:
    :param clean_brand_names:
    :return:
    """
    # remove measurements in name
    s1 = []
    for x in clean_brand_names:
        try:
            measure = re.compile(r'\b\d+(\.\d+)?\s*(g|grams|kg|kilograms|ml|milliliters|l|liters|oz|ounces|kilo|kl)\b')
            k = measure.sub('', x)
            s1.append(k)
        except TypeError:
            s1.append(np.nan)

    # remove special characters and commas
    s2 = []
    for x in s1:
        try:
            measure = re.compile(r'[^a-zA-Z0-9\s]')
            k = measure.sub('', x)
            s2.append(k)
        except TypeError:
            s2.append(np.nan)

    # remove pieces
    s3 = []
    for x in s2:
        try:
            pieces = re.compile(r'\b\d\spieces|\b\dpieces|\b\d\dpcs|\b\dpcs')
            k = pieces.sub('', x)
            s3.append(k)
        except TypeError:
            s3.append(np.nan)

    # remove 1x190g etc.
    s4 = []
    for x in s3:
        try:
            pack_x_digit = re.compile(
                r'\b\d{1,4}x\d{1,4}(?:g|grams|kg|kilograms|ml|milliliters|l|liters|oz|ounces|kilo|kl)\b')
            k = pack_x_digit.sub('', x)
            s4.append(k)
        except TypeError:
            s4.append(np.nan)

    # remove 1x190g etc.
    s5 = []
    for x in s4:
        try:
            pack_of = re.compile(r'\b(pack\sof\s\d{1,})\b')
            k = pack_of.sub('', x)
            s5.append(k)
        except TypeError:
            s5.append(np.nan)

    return s5