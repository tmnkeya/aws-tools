import csv
import json
import time
import sys
import boto3
import argparse
import random
from botocore.config import Config

class CrudAndSimpleIngestionExample:
    def __init__(self, client):
        self.client = client
        self.config_file = None

    def setup(self, config_file="config.json"):
        self.config_file = config_file
        
        with open(self.config_file, 'r') as fp:
           config_data = json.load(fp)
           self.database_name = config_data['database_name']
           self.table_name = config_data['table_name']
           self.ht_ttl_hours = config_data['ht_ttl_hours']
           self.ct_ttl_days = config_data['ct_ttl_days']
        
    def create_database(self):
        print("Creating Database")
        try:
            self.client.create_database(DatabaseName=self.database_name)
            print("Database [%s] created successfully." % self.database_name)
        except self.client.exceptions.ConflictException:
            print("Database [%s] exists. Skipping database creation" % self.database_name)
        except Exception as err:
            print("Create database failed:", err)

    def describe_database(self):
        print("Describing database")
        try:
            result = self.client.describe_database(DatabaseName=self.database_name)
            print("Database [%s] has id [%s]" % (self.database_name, result['Database']['Arn']))
        except self.client.exceptions.ResourceNotFoundException:
            print("Database doesn't exist")
        except Exception as err:
            print("Describe database failed:", err)

    def update_database(self, kms_id):
        print("Updating database")
        try:
            result = self.client.update_database(DatabaseName=self.database_name, KmsKeyId=kms_id)
            print("Database [%s] was updated to use kms [%s] successfully" % (self.database_name,
                                                                              result['Database']['KmsKeyId']))
        except self.client.exceptions.ResourceNotFoundException:
            print("Database doesn't exist")
        except Exception as err:
            print("Update database failed:", err)

    def list_databases(self):
        print("Listing databases")
        try:
            result = self.client.list_databases(MaxResults=5)
            self._print_databases(result['Databases'])
            next_token = result.get('NextToken', None)
            while next_token:
                result = self.client.list_databases(NextToken=next_token, MaxResults=5)
                self._print_databases(result['Databases'])
                next_token = result.get('NextToken', None)
        except Exception as err:
            print("List databases failed:", err)

    def create_table(self):
        print("Creating table")
        retention_properties = {
            'MemoryStoreRetentionPeriodInHours': self.ht_ttl_hours,
            'MagneticStoreRetentionPeriodInDays': self.ct_ttl_days
        }
        try:
            self.client.create_table(DatabaseName=self.database_name, TableName=self.table_name,
                                     RetentionProperties=retention_properties)
            print("Table [%s] successfully created." % self.table_name)
        except self.client.exceptions.ConflictException:
            print("Table [%s] exists on database [%s]. Skipping table creation" % (
                self.table_name, self.database_name))
        except Exception as err:
            print("Create table failed:", err)

    def update_table(self):
        print("Updating table")
        retention_properties = {
            'MemoryStoreRetentionPeriodInHours': self.ht_ttl_hours,
            'MagneticStoreRetentionPeriodInDays': self.ct_ttl_days
        }
        try:
            self.client.update_table(DatabaseName=self.database_name, TableName=self.table_name,
                                     RetentionProperties=retention_properties)
            print("Table updated.")
        except Exception as err:
            print("Update table failed:", err)

    def describe_table(self):
        print("Describing table")
        try:
            result = self.client.describe_table(DatabaseName=self.database_name, TableName=self.table_name)
            print("Table [%s] has id [%s]" % (self.table_name, result['Table']['Arn']))
        except self.client.exceptions.ResourceNotFoundException:
            print("Table doesn't exist")
        except Exception as err:
            print("Describe table failed:", err)

    def list_tables(self):
        print("Listing tables")
        try:
            result = self.client.list_tables(DatabaseName=self.database_name, MaxResults=5)
            self.__print_tables(result['Tables'])
            next_token = result.get('NextToken', None)
            while next_token:
                result = self.client.list_tables(DatabaseName=self.database_name,
                                                 NextToken=next_token, MaxResults=5)
                self.__print_tables(result['Tables'])
                next_token = result.get('NextToken', None)
        except Exception as err:
            print("List tables failed:", err)

    def write_records(self):
        
        print("Writing records")
        current_time = self._current_milli_time()
        dimensions = [
            {'Name': 'region', 'Value': 'us-east-1'},
            {'Name': 'az', 'Value': 'az1'},
            {'Name': 'hostname', 'Value': 'host1'}
        ]

        cpu_utilization = {
            'Dimensions': dimensions,
            'MeasureName': 'cpu_utilization',
            'MeasureValue': '13.5',
            'MeasureValueType': 'DOUBLE',
            'Time': current_time
        }

        memory_utilization = {
            'Dimensions': dimensions,
            'MeasureName': 'memory_utilization',
            'MeasureValue': '40',
            'MeasureValueType': 'DOUBLE',
            'Time': current_time
        }

        records = [cpu_utilization, memory_utilization]

        try:
            result = self.client.write_records(DatabaseName=self.database_name, TableName=self.table_name,
                                               Records=records, CommonAttributes={})
            print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except Exception as err:
            print("Error:", err)

    def write_records_with_common_attributes(self):
        print("Writing records extracting common attributes")
        current_time = self._current_milli_time()

        dimensions = [
            {'Name': 'region', 'Value': 'us-east-1'},
            {'Name': 'az', 'Value': 'az1'},
            {'Name': 'hostname', 'Value': 'host1'}
        ]

        common_attributes = {
            'Dimensions': dimensions,
            'MeasureValueType': 'DOUBLE',
            'Time': current_time
        }

        cpu_utilization = {
            'MeasureName': 'cpu_utilization',
            'MeasureValue': '13.5'
        }

        memory_utilization = {
            'MeasureName': 'memory_utilization',
            'MeasureValue': '40'
        }

        records = [cpu_utilization, memory_utilization]

        try:
            result = self.client.write_records(DatabaseName=self.database_name, TableName=self.table_name,
                                               Records=records, CommonAttributes=common_attributes)
            print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except Exception as err:
            print("Error:", err)

    def delete_table(self):
        print("Deleting Table")
        try:
            result = self.client.delete_table(DatabaseName=self.database_name, TableName=self.table_name)
            print("Delete table status [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except self.client.exceptions.ResourceNotFoundException:
            print("Table [%s] doesn't exist" % self.table_name)
        except Exception as err:
            print("Delete table failed:", err)

    def delete_database(self):
        print("Deleting Database")
        try:
            result = self.client.delete_database(DatabaseName=self.database_name)
            print("Delete database status [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except self.client.exceptions.ResourceNotFoundException:
            print("database [%s] doesn't exist" % self.database_name)
        except Exception as err:
            print("Delete database failed:", err)

    @staticmethod
    def _current_milli_time():
        return str(int(round(time.time() * 1000)))

    @staticmethod
    def __print_tables(tables):
        for table in tables:
            print(table['TableName'])

    @staticmethod
    def _print_databases(databases):
        for database in databases:
            print(database['DatabaseName'])




if __name__ == '__main__':

    session = boto3.Session()

    # Overwrite or reconfigure some session-related parameters here.
    write_client = session.client('timestream-write',
                                  config=Config(read_timeout=20,
                                                max_pool_connections=5000,
                                                retries={'max_attempts': 10}))
    
    ts_db = CrudAndSimpleIngestionExample(write_client)

    ts_db.setup('config.json')
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

    import time
    time.sleep(10)
    
