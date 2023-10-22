from pathlib import Path
from typing import List

import pandas as pd
from pyarrow import feather

from cleaning_pipeline.extract import sgtime as sgtz


class Transformer:
    def __init__(self, input_frame: pd.DataFrame, pipe_flavor):
        self.transformed_df = None
        self.input_frame = input_frame
        self.transform()
        self.pipe_flavor = pipe_flavor

    def transform(self) -> None:
        """
        Note: Dataframe is returned and may change the original df. make sure to copy.
          [1] clean product brand, transform 0, None, nan to numpy.nan
          [2] clear the last 5 zeros ([:-5]) in discounted price series
          [3] clean product rating, by converting it to int type instead of float (5.0 star etc.)
          [4] append median prices (price_min + price_max) / 2 to price before discount series.
          [5] create new series (from facet data) join to df with series name: category_name
        """

        self.transformed_df = self.input_frame \
            .tr_pipe.convert_id_type() \
            .tr_pipe.cl_product_brand() \
            .tr_pipe.cl_product_price() \
            .tr_pipe.cl_product_rating() \
            .tr_pipe.create_pack_size_desc() \
            .tr_pipe.create_median_price() \
            .tr_pipe.create_pages() \
            .tr_pipe.create_cat_names() \
            .tr_pipe.create_pack_names()\
            .tr_pipe.fix_fill_names()


def write_as_feather(data: List) -> None:
    frame_names: List = ["item", "rate", "sell"]
    for frames, names in zip(data, frame_names):
        dir_name = names + "_feather"
        output_name = sgtz.now() + "_" + names + "_data.feather"
        output_path = str(Path.cwd().parent / "database" / "2_sqlite" / dir_name / output_name)
        feather.write_feather(frames, output_path)
