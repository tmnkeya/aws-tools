import csv
import time
import sys
import boto3
import argparse
import random

from CrudAndSimpleIngestionExample import CrudAndSimpleIngestionExample as DBManager
from botocore.config import Config

# DATABASE_NAME = "devops"
# TABLE_NAME = "host_metrics"
# HT_TTL_HOURS = 12
# CT_TTL_DAYS = 90

if __name__ == '__main__':

    # parser = argparse.ArgumentParser()
    # parser.add_argument("-n", "--n_samples", help="This file will be used for ingesting records")
    # args = parser.parse_args()

    # if args.n_samples is None:
    #     n_samples = 10
    # else:
    #     n_samples = int(args.n_samples)
    session = boto3.Session()
    write_client = session.client('timestream-write',
                                  config=Config(read_timeout=20,
                                                max_pool_connections=5000,
                                                retries={'max_attempts': 10}))
    ts_db = DBManager(write_client)
    
    ts_db.delete_table()
    ts_db.delete_database()
    # sys.exit(0)

    ts_db.create_database()
    ts_db.describe_database()
    ts_db.list_databases()
    ts_db.create_table()
    ts_db.describe_table()
    ts_db.list_tables()
    ts_db.update_table()

    # ts_db.delete_table()
    # ts_db.delete_database()
