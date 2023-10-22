import os
from pathlib import Path

import dotenv as en
from icecream import ic

env_file = '.env'

# collection path / contains raw extracted data from start_extract()
COLLECTION_PATH: str = str(Path.cwd().parent / "data" / "collection")

# date collection path / contains a new column 'date' based from collection path
DATE_COLLECTION_PATH: str = str(Path.cwd().parent / "data" / "date_collection_data")

# parquet path / contains the dataframe used for analytics
PARQUET_PATH: str = str(Path.cwd().parent / "data" / "analytics" / "food_bev.parquet")

# JSON Path
JSON_PATH: str = str(Path.cwd().parent / "data" / "analytics" / "food_bev.json")

# parquet path / contains the dataframe used for analytics
MASTER_BRAND_PATH: str = str(Path.cwd().parent / "ref" / "master_brand_names_revised.json")

# parquet path / contains the dataframe used for analytics
TEMP_PATH: str = str(Path.cwd().parent / "data" / "temp")

# parquet path / contains the dataframe used for analytics
CONFIG_PATH: str = str(Path.cwd().parent / "config" / "request_config.ini")


en.set_key(env_file, 'COLLECTION_PATH', COLLECTION_PATH)
en.set_key(env_file, 'DATE_COLLECTION_PATH', DATE_COLLECTION_PATH)
en.set_key(env_file, 'PARQUET_PATH', PARQUET_PATH)
en.set_key(env_file, 'MASTER_BRAND_PATH', MASTER_BRAND_PATH)
en.set_key(env_file, 'JSON_PATH', JSON_PATH)
en.set_key(env_file, 'TEMP_PATH', TEMP_PATH)
en.set_key(env_file, 'CONFIG_PATH', CONFIG_PATH)
en.load_dotenv()

ic(os.getenv('COLLECTION_PATH'))
ic(os.getenv('DATE_COLLECTION_PATH'))
ic(os.getenv('PARQUET_PATH'))
ic(os.getenv('JSON_PATH'))
ic(os.getenv('TEMP_PATH'))
ic(os.getenv('CONFIG_PATH'))
