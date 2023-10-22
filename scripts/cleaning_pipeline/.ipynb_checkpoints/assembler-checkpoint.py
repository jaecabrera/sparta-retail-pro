import re
import janitor


# noinspection PyUnreachableCode
class DataAssembler:
    """
    :pipeline:
    :description:
            Data Assembler Class is an object that can organize the clean transformed data.
        The reason for this is to carefully assemble and reclean the features to
        preserve data integrity all throughout the process.

    """

    def __init__(self, dataframe):
        self.rating_df = None
        self.selling_df = None
        self.basic_df = None
        self.dataframe = dataframe
        self.basic_info_features = [
            'category', 'category_names',
            'brand', 'pack_size', 'pack_type',
            'itemid', 'name', 'price_median',
            'raw_discount', 'price_before_discount',
            'units_sold', 'stock', 'status', 'item_page']

        self.item_stats_features = [
            'like_count', 'comment_count', 'views',
            'prod_rate_star_0', 'prod_rate_star_1', 'prod_rate_star_2',
            'prod_rate_star_3', 'prod_rate_star_4', 'prod_rate_star_5',
            'product_total_rating']

        self.selling_features = [
            'low_price_guarantee', 'on_flash_sale',
            'can_use_bundle_deal', 'can_use_cod',
            'can_use_wholesale', 'show_free_shipping']

    def __repr__(self):
        memory_usage = self.dataframe.memory_usage(deep=True).sum().sum()
        return f"""{memory_usage=}"""

    def assemble_data(self) -> list:
        def clean_selling_feature_names(names) -> list:
            """
            remove extra prefix from 'selling' columns such as feature_, and shop_is_
            :returns: A dict object for pyjanitor.rename_columns() input
            """
            name_sf_clean_1 = [re.sub(r'(feature_)', '', i) for i in names]
            sf_new_names = [re.sub(r'(shop_is_)', '', i) for i in name_sf_clean_1]
            # sf_new_names = {k: v for k, v in zip(names, name_sf_clean_2)}
            return sf_new_names

        old_col_names = self.dataframe.columns
        clean_col_names = clean_selling_feature_names(old_col_names)
        self.dataframe.columns = clean_col_names

        self.basic_df = self.dataframe[self.basic_info_features]
        self.selling_df = self.dataframe[self.selling_features]
        self.rating_df = self.dataframe[self.item_stats_features].rename_columns({
            'prod_rate_star_0': 'no_rating',
            'prod_rate_star_1': 'star_1',
            'prod_rate_star_2': 'star_2',
            'prod_rate_star_3': 'star_3',
            'prod_rate_star_4': 'star_4',
            'prod_rate_star_5': 'star_5',
            'product_total_rating': 'rating'})

        # temporary remove date names
        # select dataframe based on features
        return [self.basic_df, self.selling_df, self.rating_df]
