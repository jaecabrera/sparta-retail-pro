from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from icecream import ic
from prefect import flow, task
from scripts.clean_transform import *
from scripts.date_insert import *
from scripts.extract import category
from scripts.extract.manager import PathDefaults
from scripts.extract.market import MarketParams, ShopRequest
from scripts.info.notification_sys import send_emails, get_facet, EmailParams
from scripts.prepare_analytics import *

SCRIPT_CWD = Path.cwd().parents[0]

load_dotenv('scripts/.env')

# collection path / contains raw extracted data from start_extract()
COLLECTION_PATH = Path(os.getenv("COLLECTION_PATH"))
# date collection path / contains a new column 'date' based from collection path
DATE_COLLECTION_PATH = Path(os.getenv("DATE_COLLECTION_PATH"))
# parquet path / contains the dataframe used for analytics
PARQUET_PATH = Path(os.getenv("PARQUET_PATH"))
# json path / contains the dataframe used for business-requirements analytics
JSON_PATH = Path(os.getenv("JSON_PATH"))
# temp path /
TEMP_PATH = Path(os.getenv("TEMP_PATH"))


@flow(log_prints=True)
def start_extract():
    print('extracting')
    """
    :description:
        Start point of flow pipeline. Extracting
    data from website then notify email.
    """
    path_default = PathDefaults()
    category_dataframe = category.query()
    names = list(category_dataframe.category_name.values)
    ids = list(category_dataframe.cat_id.values)
    counts = list(category_dataframe.cat_count.values)
    print(category_dataframe)

    market_params = MarketParams(
        category_names=names,
        category_id=ids,
        count=counts
    )

    ShopRequest(
        market_params,
        request_config=path_default.parse_market_params())

    notify_email()


@task
def notify_email() -> None:
    """
    Notify email address of the collected data.
    """
    print('email-notified')
    email_params = EmailParams(subject="Lagoon: Market Report")
    data_table = get_facet(TEMP_PATH)
    send_emails(email_params, data_table)


@flow()
def transform() -> None:
    """
    Transformation flow that includes cleaning and transforming dates. +
    Preparing the data for business requirements.
    """
    clean_dates()
    clean_transform_dates()
    prep_requirements()


@task
def clean_dates():

    manager = UpdateManager(COLLECTION_PATH, DATE_COLLECTION_PATH).get_updates()

    updated_data_list = [manager]
    for i in tqdm(updated_data_list):
        m = MarketData(path=i)
        m.insert_date
        m.dump()


@task
def clean_transform_dates() -> None:
    """
    Clean and transform the updated dataframe with dates. This
    function includes filtering and extracting the food & beverage category.
    """
    # read updated data
    df = read_updated()

    # create path variable of all data collected and create data collection variable dataframe.
    updated_df = get_updated_dataframe(df)

    # problems with clean transform either NoneType data or invalid path.
    clt = clean_transform(updated_df).assemble_data()

    # read previous data
    open_db = pd.read_parquet(PARQUET_PATH)

    # concat previous data and new data
    concat_db = pd.concat([open_db, clt], ignore_index=True)

    # filter data by category
    filtered_concat = concat_db \
        .filter_column_isin('category_names', ['Food & Beverage']) \
        .reset_index(drop=True)

    # change to datetime format
    filtered_concat['date'] = pd.to_datetime(filtered_concat.date)

    # Use title case for brand
    filtered_concat['brand'] = filtered_concat.brand.str.title()

    # save to parquet in local directory
    filtered_concat.to_parquet(PARQUET_PATH)
    filtered_concat.to_json(JSON_PATH)


@task
def prep_requirements():
    """
    Export and transform data based on client specifications.
    """
    analytics_df = transform_analytics(PARQUET_PATH)
    analytics_df.to_csv("data/analytics/client_pepsi_coke.csv")
    print("[Tableau Linked] Analytics data successfully created.")


if __name__ == '__main__':
    start_extract()
    transform()
