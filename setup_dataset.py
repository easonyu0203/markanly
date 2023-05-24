"""
In this file we load csv file and store it to sqlite database
"""

import dotenv
import os
from pathlib import Path
from datetime import datetime
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
from tqdm import tqdm

from models import BehaviorData, MemberData, Base

dotenv.load_dotenv()
data_dir_path = Path(os.getenv("DATA_DIR_PATH"))
assert data_dir_path.exists(), f"please set DATA_DIR_PATH in .env file."

# create database
_ = sqlite3.connect('database.db')

# Define the SQLAlchemy engine and session
engine = create_engine('sqlite:///database.db')
Session = sessionmaker(bind=engine)
session = Session()


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


def get_behavior_df(file_name: [str, Path]) -> pd.DataFrame:
    """load behavior data from csv file and return a dataframe
    param: file_name: str or Path object
    """
    global data_dir_path
    data_types = {
        'Tunnel': str,
        'Device': str,
        'FullvisitorId': str,
        'DeviceId': str,
        'ShopMemberId': str,
        'HitTime': int,
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
        'EventTime': int
    }
    df = pd.read_csv(data_dir_path / 'BehaviorData' / file_name, dtype=data_types)
    df['HitTime'] = pd.to_datetime(df['HitTime'], unit='ms')
    df['EventTime'] = pd.to_datetime(df['EventTime'], unit='ms')
    return df


def df_to_db(model_cls, df: pd.DataFrame):
    """Load a dataframe into a database table using the given model class."""
    chunk_size = 10_000  # Number of records to add in each chunk
    primary_key = model_cls.__table__.primary_key.columns.keys()[0]  # Get the primary key column name
    records = []
    for index, record in enumerate(tqdm(df.to_dict(orient='records'))):
        # Check if the record already exists in the database
        primary_key_value = record[primary_key]
        existing_record = session.query(model_cls).filter_by(**{primary_key: primary_key_value}).first()
        if existing_record:
            continue

        records.append(model_cls(**record))

        if len(records) % chunk_size == 0:
            session.add_all(records)
            session.commit()
            records = []  # Reset the records list after committing the chunk

    # Add any remaining records
    if records:
        session.add_all(records)
        session.commit()


def main():
    print('import MemberData.csv to database')
    df_to_db(MemberData, get_member_df())

    behavior_file_names = os.listdir(data_dir_path / 'BehaviorData')
    for file_name in behavior_file_names:
        print(f'import {file_name} to database')
        df_to_db(BehaviorData, get_behavior_df(file_name))


if __name__ == '__main__':
    main()
