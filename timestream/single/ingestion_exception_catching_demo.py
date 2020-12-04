import sys
import json
import time
import boto3
import psutil
import argparse
from botocore.config import Config
from timeit import default_timer as timer
import pandas as pd

# Batch size
def prepare_record(measure_name, measure_value, isCA = False):
    """
    isCA == False: Prepare a record as a batch mode. I.e, dimensions fields are included in each record.
    isCA == True: Prepare a record as a batch mode but utilize common attribute. I.e, dimensions fields are NOT included in each record.
    """
    if isCA == True: 
        record = {
            'Time': str(current_time),
            'MeasureName': measure_name,
            'MeasureValue': str(measure_value),
            'MeasureValueType': 'DOUBLE'
        }
    else:
        record = {
            'Time': str(current_time),
            'Dimensions': dimensions,
            'MeasureName': measure_name,
            'MeasureValue': str(measure_value),
            'MeasureValueType': 'DOUBLE'
        }
    return record

def write_records(records, isCA=False):
    """
    isCA == False: Prepare and insert a record as a batch mode. I.e, dimensions fields are included in each record.
    isCA == True: Prepare and insert a record as a batch mode but utilize common attribute. I.e, dimensions fields are NOT included in each record.
    """
    try:
        if isCA == True:
            result = write_client.write_records(DatabaseName=DATABASE_NAME,
                                                TableName=TABLE_NAME,
                                                Records=records,             # records is alreay prepared without Dimensions entry by prepare_record()
                                                CommonAttributes={'Dimensions': dimensions}) # thus, set its CommonAttributes 
            
        else:
            result = write_client.write_records(DatabaseName=DATABASE_NAME,
                                                TableName=TABLE_NAME,
                                                Records=records,             # records is prepared with Dimensions entry
                                                CommonAttributes={})         # thus, not set its CommonAttributes
            
        status = result['ResponseMetadata']['HTTPStatusCode']
        # print("Processed %d records. WriteRecords Status: %s" %
        #       (len(records), status))
        
    except write_client.exceptions.RejectedRecordsException as err:
        # print("Got a Rejected Records Exception")
        # print("RejectedRecords: ", err)
        print(err.response)
        # print(err.response["Error"])
        
        # for rr in err.response["RejectedRecords"]:
        #     print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
        #     print("Some other records were written successfully. Causion. Unreliable")
        return -1

    except write_client.exceptions.ValidationException as err:
        print("Got a ValidationException")
        print(err.response['ResponseMetadata'])
        # for rr in err.response['ResponseMetadata']:
        #     print(rr)
        return -1

    
    except Exception as err:
        print("Error:", err)
        print(err.response)
        print(err.response["Error"])
        return -1

if __name__ == '__main__':

    is_print = False

    parser = argparse.ArgumentParser(prog = 'TimestreamSampleContinuousDataIngestorApplication',
                                     description='Execute a example application generating and ingesting time series data.')
    parser.add_argument('--batch-size', '-b',
                        type = int,
                        default = 10,
                        help = "the number of records in a batch (default: 10). It must be between 1 and 100")    
    parser.add_argument('--common-attributes', '-c',
                        type = eval,
                        choices = [True, False],
                        default = False,
                        help = "True or False for use of common attributes in write_records")
    parser.add_argument('--total-records-cnt', '-n',
                        type = int,
                        default = 100,
                        help = "the number of total records (rows) inserted into a table (default: 100)")
    args = parser.parse_args()

    batch_size = args.batch_size
    is_ca = args.common_attributes
    total_records_cnt = args.total_records_cnt

    # Configuration parameters (put into global scope and spread into above functions (sorry).
    # Note that the database and table must exist in order to insert any data into the table.
    config_file = 'config.json'
    with open(config_file, 'r') as fp:
        config_data = json.load(fp)
        DATABASE_NAME= config_data['database_name']
        TABLE_NAME = config_data['table_name']
        COUNTRY = config_data['country']    # for dimensions
        CITY = config_data['city']         # for dimensions
        HOSTNAME = config_data['host_name'] # for dimensions

    resultDF = pd.DataFrame(columns=['is_common_attributes', 'batch_size', 'total_records_cnt', 'success_records_cnt', 'elapsed_time_sec'])
                
    # print("ENDED: isCA = {}, batch_size = {}, total_records_cnt = {}".format(is_ca, batch_size, total_records_cnt))
    # sys.exit(0)
    session = boto3.Session()
    write_client = session.client('timestream-write',
                                  config=Config(
                                      read_timeout=20,
                                      max_pool_connections=5000,
                                      retries={'max_attempts': 10}))
    query_client = session.client('timestream-query')

    dimensions = [
        {'Name': 'country', 'Value': COUNTRY},
        {'Name': 'city', 'Value': CITY},
        {'Name': 'hostname', 'Value': HOSTNAME},
    ]

    records = []
    timestamps = []

    # total_records_cnt should be smaller than memory retention period length in second.
    past_time = int(time.time()*1000) - total_records_cnt * 1000 # back to the past in total_records_cnt x ms

    # The below is made for erronous timestamps to show Rejected Record Exception
    for step in range(0, total_records_cnt):
        if step % 111 == 50 :
            timestamps.append(77) # Do something bad here.
        else:
            timestamps.append(past_time + step * 50)
    # print(timestamps)

    
    i = 0
    failed_write_records_calls  = 0
    start = timer()
    while i < total_records_cnt:
        current_time = timestamps[i]
        cpu_utilization = psutil.cpu_percent()
        memory_utilization = psutil.virtual_memory().percent
        swap_utilization = psutil.swap_memory().percent
        disk_utilization = psutil.disk_usage('/').percent

        if is_ca == True:  # batch with common attributes
            if i%4 == 0:
                records.append(prepare_record('cpu_utilization', cpu_utilization, True)) # is_ca ==  True
            elif i%4 == 1:
                records.append(prepare_record('memory_utilization', memory_utilization, True))
            elif i%4 == 2:
                records.append(prepare_record('swap_utilization', swap_utilization, True))
            elif i%4 == 3:
                records.append(prepare_record('disk_utilization', disk_utilization, True))

            if is_print:
                print("records {} - cpu {} - memory {} - swap {} - disk {}".format(
                    len(records),
                    cpu_utilization,
                    memory_utilization,
                    swap_utilization,
                    disk_utilization))

            if len(records) == batch_size:
                ret = write_records(records, True) # CA
                records = []
                if ret == -1:
                    failed_write_records_calls  += 1
                    continue
                
        else: # batch with out common attributes
            if i%4 == 0:
                records.append(prepare_record('cpu_utilization', cpu_utilization)) # is_ca ==  False
            elif i%4 == 1:
                records.append(prepare_record('memory_utilization', memory_utilization))
            elif i%4 == 2:
                records.append(prepare_record('swap_utilization', swap_utilization))
            elif i%4 == 3:
                records.append(prepare_record('disk_utilization', disk_utilization))

            if is_print:
                print("records {} - cpu {} - memory {} - swap {} - disk {}".format(
                    len(records),
                    cpu_utilization,
                    memory_utilization,
                    swap_utilization,
                    disk_utilization))
                
            if len(records) == batch_size:
                ret = write_records(records) # Not using Common Attributes
                records = []
                if ret == -1:
                    failed_write_records_calls  += 1
                    continue

        # time.sleep(INTERVAL)
        i += 1

    end = timer()
    elapsed_time_sec = end - start
    print("ENDED: isCA = {}, batch_size = {}, total_records_cnt (confirmed) = {}, failed_write_records_calls = {}, elapsed_time_sec = {}".format(is_ca, batch_size, total_records_cnt, failed_write_records_calls, elapsed_time_sec))

    # result = {'is_common_attributes': is_ca, 'batch_size': batch_size, 'total_records_cnt': total_records_cnt, 'success_records_cnt': i+1 , 'elapsed_time_sec': elapsed_time_sec}
    # resultDF = resultDF.append(result, ignore_index = True)
    # print(resultDF)
    # resultDF.to_csv('results.csv', index=False)

    # https://medium.com/perlego/amazon-timestream-101-3b097db680cf
    # Error: An error occurred (RejectedRecordsException) when calling the WriteRecords operation: One or more records have been rejected. See RejectedRecords for details.
    # NOTE: Any consecutive testing, make sure all time stamps are valid.
    # Error: An error occurred (ValidationException) when calling the WriteRecords operation: Invalid time for record.
    # To above it, sleep for a while
    # time.sleep(60)
                


