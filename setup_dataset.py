"""
In this file we load csv file and store it to sqlite database
"""
from typing import Iterator

import dotenv
import os
from pathlib import Path
from datetime import datetime
import sqlite3
import pandas as pd
from tqdm import tqdm

from models import BehaviorData, MemberData
from utils.db_utils import create_db_session

dotenv.load_dotenv()
data_dir_path = Path(os.getenv("DATA_DIR_PATH"))
assert data_dir_path.exists(), f"please set DATA_DIR_PATH in .env file."

# create database
_ = sqlite3.connect('database.db')

# Define the SQLAlchemy session
session = create_db_session()


def get_member_df() -> pd.DataFrame:
    global data_dir_path
    data_types = {
        'ShopMemberId': str,
        'RegisterSourceTypeDef': str,
        'RegisterDateTime': str,
        'Gender': str,
        'Birthday': str,
        'APPRefereeId': str,
        'APPRefereeLocationId': str,
        'IsAppInstalled': bool,
        'IsEnableEmail': bool,
        'IsEnablePushNotification': bool,
        'IsEnableShortMessage': bool,
        'FirstAppOpenDateTime': str,
        'LastAppOpenDateTime': str,
        'MemberCardLevel': int,
        'CountryAliasCode': str,
    }
    df = pd.read_csv(data_dir_path / 'MemberData.csv', dtype=data_types)
    df['RegisterDateTime'] = pd.to_datetime(df['RegisterDateTime'], format='ISO8601')
    df['Birthday'] = df['Birthday'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
    df = df.drop(columns=['APPRefereeId', 'APPRefereeLocationId', 'FirstAppOpenDateTime', 'LastAppOpenDateTime'])
    return df


def get_behavior_df_iter(file_name: [str, Path]) -> Iterator[pd.DataFrame]:
    """Load behavior data from a CSV file and return a DataFrame iterator.

    Args:
        file_name: str or Path object, the name of the file to load.

    Yields:
        pd.DataFrame: Each chunk of the loaded DataFrame.
    """
    global data_dir_path
    data_types = {
        'Tunnel': str,
        'Device': str,
        'FullvisitorId': str,
        'DeviceId': str,
        'ShopMemberId': str,
        'HitTime': float,
        'Language': str,
        'CountryAliasCode': str,
        'Version': str,
        'UTMSource': str,
        'UTMMedium': str,
        'UTMName': str,
        'Behavior': str,
        'RegisterTunnel': str,
        'CategoryId': str,
        'SalePageId': str,
        'UnitPrice': float,
        'Qty': float,
        'TotalSalesAmount': float,
        'CurrencyCode': str,
        'TradesGroupCode': str,
        'SearchTerm': str,
        'ContentType': str,
        'ContentName': str,
        'ContentId': str,
        'PageType': str,
        'EventTime': float
    }
    chunk_size = 1_000_000  # Number of rows per chunk

    for chunk in pd.read_csv(data_dir_path / 'BehaviorData' / file_name, chunksize=chunk_size, dtype=data_types):
        chunk = chunk.dropna(subset=['HitTime', 'EventTime'])
        chunk['HitTime'] = chunk['HitTime'].astype(int)
        chunk['EventTime'] = chunk['EventTime'].astype(int)
        chunk['HitTime'] = pd.to_datetime(chunk['HitTime'], unit='ms')
        chunk['EventTime'] = pd.to_datetime(chunk['EventTime'], unit='ms')
        yield chunk


def df_to_db(model_cls, df: pd.DataFrame):
    """Load a dataframe into a database table using the given model class."""
    if not hasattr(df_to_db, 'cls_existingpks'):
        df_to_db.cls_existingpks = dict()

    chunk_size = 50_000  # Number of records to add in each chunk
    primary_key = model_cls.__table__.primary_key.columns.keys()[0]  # Get the primary key column name
    primary_key_attr = getattr(model_cls, primary_key)  # Get the primary key attribute of the model class

    # Collect existing primary keys from the database
    if model_cls not in df_to_db.cls_existingpks:
        print('create skip list..')
        existing_primary_keys = set()  # Keep track of existing primary keys in the database
        for existing_record in session.query(primary_key_attr).all():
            existing_primary_keys.add(existing_record[0])
        df_to_db.cls_existingpks[model_cls] = existing_primary_keys
        print('skip list created')

    records = []
    for index, record in enumerate(tqdm(df.to_dict(orient='records'))):
        primary_key_value = record[primary_key]
        if primary_key_value in df_to_db.cls_existingpks[model_cls]:
            continue

        records.append(model_cls(**record))

        if len(records) % chunk_size == 0:
            session.bulk_save_objects(records)
            session.commit()
            records = []  # Reset the records list after committing the chunk

    # Add any remaining records
    if records:
        session.bulk_save_objects(records)
        session.commit()


def main():
    print('import MemberData.csv to database')
    df_to_db(MemberData, get_member_df())

    behavior_file_names = sorted(f for f in os.listdir(data_dir_path / 'BehaviorData') if ".csv" in f)
    num = 0
    for file_name in behavior_file_names:
        print(f'import {file_name} to database')
        this_num = 0
        for df_chunk in get_behavior_df_iter(file_name):
            df_chunk['id'] = (df_chunk.index + num)
            this_num += len(df_chunk)
            df_to_db(BehaviorData, df_chunk)
        num += this_num


if __name__ == '__main__':
    main()
