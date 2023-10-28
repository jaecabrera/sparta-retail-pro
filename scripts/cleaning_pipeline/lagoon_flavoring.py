import os
import re

import numpy as np
import pandas as pd
import pandas_flavor as pf
from icecream import ic
from pandas.core.common import flatten
from dotenv import load_dotenv
from pathlib import Path


load_dotenv('scripts/.env')
MASTER_BRAND_PATH = Path(os.getenv('MASTER_BRAND_PATH'))


@pf.register_dataframe_accessor('tr_pipe')
class TransformerPipe:

    def __init__(self, df):
        self.df = df

    def convert_id_type(self):
        id_series = ['itemid', 'shopid', 'category']
        self.df[id_series] = self.df[id_series].astype('object')
        return self.df

    def cl_product_brand(self) -> pd.DataFrame:
        brand_series = self.df.brand
        brand_series \
            .astype('string') \
            .replace({
            '0': np.nan,
            'None': np.nan,
            'nan': np.nan})
        self.df.brand = brand_series

        return self.df

    def cl_product_price(self) -> pd.DataFrame:
        price = ['price_before_discount', 'price_min', 'price_max']

        for s in price:
            self.df[s] = [*map(lambda p: p[:-5], self.df[s].astype('string'))]

        return self.df

    def cl_product_rating(self) -> pd.DataFrame:
        rating_series = self.df.product_total_rating \
            .apply(lambda num: np.round(num, 2))

        self.df.product_total_rating.update(rating_series)

        return self.df

    def create_pack_size_desc(self):
        from functools import reduce

        pack_size_series = self.df.pack_size.fillna('X 0')
        pack_list = pack_size_series.to_numpy()
        re_pack_size = re.compile("X\s\d*")
        _size = [re_pack_size.findall(x) for x in pack_list]
        _size_reduce = reduce(lambda x, y: x + y, _size)

        pack_series = pd.Series(_size_reduce) \
            .str.split(' ', expand=True)[1]

        return self.df.assign(pack_type=pack_series)

    def create_median_price(self):

        median_array = (self.df.price_min.astype(dtype='int') + self.df.price_max.astype(dtype='int')) / 2
        median_array = median_array.astype('int')

        return self.df.assign(price_median=median_array)

    def create_pages(self):
        """
        :description:
            Assigns a page number for each product.
        """
        batch = 120
        total_page = self.df.shape[0]
        items_per_page = int(total_page / batch)

        pages = []
        j = 0

        for i in np.arange(0, batch):
            p = list(np.tile(j, items_per_page))
            pages.append(p)
            j += 1

        pages = [*flatten(pages)]

        # returns nan due to some error
        return self.df.assign(item_page=np.nan)

    def create_cat_names(self):

        def map_category(var):

            names = {
                "100632": "Mom & Baby",
                "100629": "Food & Beverage",
                "100630": "Beauty",
                "100001": "Health",
                "100631": "Pets",
                "100636": "Home & Living",
                "100640": "Automobiles"}

            str_var_val = str(var)
            if str_var_val in names.keys():
                name = names.get(str_var_val)
                return name

            else:
                return np.nan

        category_names_map = [*map(map_category, self.df.category)]
        return self.df.assign(category_names=category_names_map)

    def create_pack_names(self):

        def map_pack(var):
            int_var = int(var)
            if int_var == 1:
                return 'single-pack'
            if int_var > 1:
                return 'multi-pack'
            else:
                return np.nan

        category_names_map = [*map(map_pack, self.df.pack_type)]
        self.df.pack_type.update(category_names_map)
        return self.df

    def fix_fill_names(self) -> pd.DataFrame:

        from pathlib import Path
        path_cwd = Path.cwd()
        reference = pd.read_json(MASTER_BRAND_PATH, typ='series')
        self.df = self.df.reset_index(drop=True)
        null_df = self.df[self.df.brand.isna()][['name', 'brand']]

        reference = reference.str.lower()
        null_df['name'] = null_df.name.str.lower()

        # create a new series with matching brand names
        self.df.loc[self.df[self.df['brand'].isna() == True].index, 'brand'] = null_df['name'] \
            .apply(lambda x: match_brand_name(x, ref=reference))

        return self.df


# create function to match brand name
def match_brand_name(value, ref):
    for brand in ref:
        try:
            if re.match(brand, value):
                fill_name = re.match(brand, value).group()
                return fill_name
        except TypeError:
            continue
