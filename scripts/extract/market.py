import logging
import os
import sys
import time
from configparser import SectionProxy
from dataclasses import dataclass
from pathlib import Path
from typing import Generator, List, Any

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from numpy.typing import NDArray

from .manager import PathDefaults
from .sgtime import now as sgtz

logging.basicConfig(format='%(asctime)s | %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)

load_dotenv('../.env')

CONFIG_PATH = Path(os.getenv("CONFIG_PATH"))
TEMP_PATH = Path(os.getenv("TEMP_PATH"))
COLLECTION_PATH = Path(os.getenv("COLLECTION_PATH"))


@dataclass
class MarketParams:
    category_names: List[str]
    category_id: List[str]
    count: List[int]


class ShopRequest:

    def __init__(self, params: MarketParams, request_config=CONFIG_PATH):

        # Market parameters
        self.category_names = params.category_names
        self.category_id = params.category_id
        self.count = params.count

        # Starting requests
        self.config = request_config
        self.compile_request()
        self.start_request()
        self.save_to_csv()

    def compile_request(self) -> NDArray[Any]:
        """
        Store a combined string of all the product categories into
        one url string. And assign as attribute to `self.request_url` of class.
        """

        # parameters
        item_per_page = 25
        n_items = 3000
        total_page = int(n_items / item_per_page)

        # config base and tail url
        base = self.config['REQUEST_PATH']['BASE']
        tail = self.config['REQUEST_PATH']['TAIL']
        sort = self.config['REQUEST_PATH']['SORT']

        # combining string objects using numpy
        categories_join = "2C%".join(
            [i for i in self.config['ITEM_CODE'].values()])
        pagination = np.arange(0, n_items, item_per_page).astype('str')
        full_base = base + categories_join + tail
        full_base_total_page = np.tile(full_base, total_page)
        full_base_total_pagination = np.char.add(full_base_total_page, pagination)
        full_base_total_pagination_sort: NDArray[Any] = np.char.add(
            full_base_total_pagination, sort)

        return full_base_total_pagination_sort

    def start_request(self) -> None:
        """
        Store a retail data. And assign as attribute to `self.data` of class.
        :returns: None
        """
        import json
        self.data = []
        session = requests.Session()
        headers = {"x-api-source": "pc", "af-ac-enc-dat": "null"}
        session.headers.update(headers)
        for page_links in self.compile_request():
            r = session.get(page_links, headers=headers)
            open(TEMP_PATH / "data_temp-data.txt", 'wb') \
                .write(r.content)

            with open(TEMP_PATH / "data_temp-data.txt", 'rb') as dict_obj:
                string = dict_obj.readlines()
                dict_obj.close()

            dict_parsed = json.loads(string[0])
            data_as_string = json.dumps(dict_parsed)
            data_as_json = json.loads(data_as_string)
            self.data.append(data_as_json)

    def request_products(self, max_pages=None) -> Generator[dict[str, Any], dict[str, Any], None]:
        """
        :description:
        :return: Generator Object
        """
        try:
            for data in self.data[:max_pages]:
                try:
                    for entry in data['data']['items']:
                        val = entry['item_basic']
                        yield {
                            'date_collected': sgtz(),
                            'itemid': val['itemid'],
                            'shopid': val['shopid'],
                            'category': val['catid'],
                            'name': val['name'],
                            'pack_size': val['pack_size'],
                            'price_before_discount': val['price_before_discount'],
                            'price_min': val['price_min'],
                            'price_max': val['price_max'],
                            'raw_discount': val['raw_discount'],
                            'discount': val['discount'],
                            'brand': val['brand'],
                            'like_count': val['liked_count'],
                            'comment_count': val['cmt_count'],
                            'views': val['view_count'],
                            'prod_rate_star_0': val['item_rating']['rating_count'][5],
                            'prod_rate_star_1': val['item_rating']['rating_count'][4],
                            'prod_rate_star_2': val['item_rating']['rating_count'][3],
                            'prod_rate_star_3': val['item_rating']['rating_count'][2],
                            'prod_rate_star_4': val['item_rating']['rating_count'][1],
                            'prod_rate_star_5': val['item_rating']['rating_count'][0],
                            'product_total_rating': val['item_rating']['rating_star'],
                            'stock': val['stock'],
                            'units_sold': val['historical_sold'],
                            'status': val['status'],
                            'low_price_guarantee': val['has_lowest_price_guarantee'],
                            'shop_location': val['shop_location'],
                            'shop_is_on_flash_sale': val['is_on_flash_sale'],
                            'shop_is_preferred_plus_seller': val['is_preferred_plus_seller'],
                            'feature_lowest_price_guarantee': val['has_lowest_price_guarantee'],
                            'feature_can_use_bundle_deal': val['can_use_bundle_deal'],
                            'feature_can_use_cod': val['can_use_cod'],
                            'feature_can_use_wholesale': val['can_use_wholesale'],
                            'feature_show_free_shipping': val['show_free_shipping'],
                            'product_image_variation': val['images'],
                            'product_text_variation': val['tier_variations'],
                        }
                except TypeError:
                    print("None")
                    continue

        except KeyError as e:
            if self.data[0]['error'] == 90309999:
                print("ShopeeAPI(ErrorCode): 90309999")
                time.sleep(3)
                sys.exit()

    def save_to_csv(self) -> None:
        """
        :description:
            extract data using for loop then save to .csv using pandas module.
        Note:
            Data retrieved might be slower. Using modules
            like numpy will be more efficient for bigger data requests.

        :returns: None
        """
        self.pandas_dataframe = pd.DataFrame([i for i in self.request_products()])
        save_name = "_market.csv"
        time_now = str(sgtz())
        file_name = time_now + save_name
        save_path_name = COLLECTION_PATH / file_name
        self.pandas_dataframe.to_csv(save_path_name)
