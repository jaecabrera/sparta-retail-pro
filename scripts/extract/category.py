import json
import logging
import os
from pathlib import Path
from typing import Generator, List, Any
from dotenv import load_dotenv

import pandas as pd
import requests

from .sgtime import now as sgtz

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set the desired log level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Log to the console
        logging.FileHandler('category.log')  # Log to a file (optional)
    ]
)

load_dotenv()
TEMP_PATH = Path(os.getenv("TEMP_PATH"))


def query():
    """
    :description:
            Sends a request for the data on the current categories.
        Category ID, Parent categories, Names, and total
        counts of each category. The counts in this query will be used
        as the upper LIMIT of our main market request.

            Existing counts of the category are the current listed
        product and that will be extracted.

    :return: Pandas DataFrame Object
    """

    def request_facets() -> Generator[dict[str, int], dict[str, int], None]:
        """
        :description:
                The generator function which requests and loads the json
            object from the market query data.
        :return: Generator Object
        """
        r = requests.get('https://shopee.ph/api/v4/search/search_mart_facet')

        facet_temp_path = TEMP_PATH / "facet_temp-data.txt"
        open(facet_temp_path, 'wb').write(r.content)

        with open(facet_temp_path, 'rb') as dict_obj:
            string = dict_obj.readlines()
            dict_obj.close()

        dict_parsed = json.loads(string[0])
        data_as_string = json.dumps(dict_parsed)
        data_as_json = json.loads(data_as_string)

        for facets in data_as_json['data']['facets']:
            yield {
                'category_name': facets['category']['display_name'],
                'cat_id': facets['catid'],
                'cat_count': facets['count']
            }

    try:
        facet_dataframe = pd.DataFrame([i for i in request_facets()])
        time = sgtz()
        facet_dataframe.to_json(TEMP_PATH / f'{time}_facet.json')

        return facet_dataframe

    except Exception as e:
        """ This exception in case that eCommerce site blocks the market facet request. Then it will raise this error"""
        logging.info(f"An exception of {e} has occurred. Reading previous category data instead")
        facet_dataframe = pd.read_json(TEMP_PATH / '09272023-1102_facet.json')
        return facet_dataframe


class CategoryRequest:
    """
    class:: CategoryRequest
    :module: extract
    :description:
            Extracts the number of product listings in the current category
        for scraping. The object contains the request and logs extracted
        files in flat texts.
    """

    def __init__(self, item_series, path):
        self.item_series = item_series
        self.compile_request()
        self.start_request(path)

    def compile_request(self) -> List[Any]:
        base = "https://shopee.ph/api/v4/item/get?itemid="
        tail = "&shopid=238345847"
        cat_requests = [base + str(i) + tail for i in self.item_series]

        return cat_requests

    def start_request(self, temp_save_path):
        self.data = []
        for page_links in self.compile_request():
            r = requests.get(page_links)

            open(temp_save_path / "cat_req_temp-data.txt", 'wb').write(r.content)

            with open(temp_save_path / "cat_req_temp-data.txt", 'rb') as dict_obj:
                string = dict_obj.readlines()
                dict_obj.close()

            dict_parsed = json.loads(string[0])
            data_as_string = json.dumps(dict_parsed)
            data_as_json = json.loads(data_as_string)
            self.data.append(data_as_json)
