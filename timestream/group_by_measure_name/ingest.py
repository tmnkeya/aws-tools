from collections import defaultdict, namedtuple
import random, string
import os
import json
import time
import sys, traceback
from timeit import default_timer as timer
import numpy as np
import datetime
import threading
import argparse
from pathlib import Path
import signal
from botocore.config import Config
import boto3
import pprint

#######################################
###### Dimension model for schema #####
#######################################

regionIad = "us-east-1"
regionCmh = "us-east-2"
regionSfo = "us-west-1"
regionPdx = "us-west-2"
regionDub = "eu-west-1"
regionNrt = "ap-northeast-1"
regions = [regionIad, regionCmh, regionSfo, regionPdx, regionDub, regionNrt]
cellsPerRegion = {
    regionIad : 15, regionCmh : 2, regionSfo: 6, regionPdx : 2, regionDub : 10, regionNrt: 5
}
siloPerCell = {
    regionIad : 3, regionCmh: 2, regionSfo: 2, regionPdx: 2, regionDub : 2, regionNrt: 3
}

microserviceApollo = "apollo"
microserviceAthena = "athena"
microserviceDemeter = "demeter"
microserviceHercules = "hercules"
microserviceZeus = "zeus"
microservices = [microserviceApollo, microserviceAthena, microserviceDemeter, microserviceHercules, microserviceZeus]

instance_r5_4xl = "r5.4xlarge"
instance_m5_8xl = "m5.8xlarge"
instance_c5_16xl = "c5.16xlarge"
instance_m5_4xl = "m5.4xlarge"

instanceTypes = {
    microserviceApollo: instance_r5_4xl,
    microserviceAthena: instance_m5_8xl,
    microserviceDemeter: instance_c5_16xl,
    microserviceHercules: instance_r5_4xl,
    microserviceZeus: instance_m5_4xl
}

osAl2 = "AL2"
osAl2012 = "AL2012"

osVersions = {
    microserviceApollo: osAl2,
    microserviceAthena: osAl2012,
    microserviceDemeter: osAl2012,
    microserviceHercules: osAl2012,
    microserviceZeus: osAl2
}

instancesForMicroservice = {
    microserviceApollo: 3,
    microserviceAthena: 1,
    microserviceDemeter: 1,
    microserviceHercules: 2,
    microserviceZeus: 3
}

processHostmanager = "host_manager"
processServer = "server"

processNames = {
    microserviceApollo: [processServer],
    microserviceAthena: [processServer, processHostmanager],
    microserviceDemeter: [processServer, processHostmanager],
    microserviceHercules: [processServer],
    microserviceZeus: [processServer]
}

jdk8 = "JDK_8"
jdk11 = "JDK_11"

jdkVersions = {
    microserviceApollo: jdk11,
    microserviceAthena: jdk8,
    microserviceDemeter: jdk8,
    microserviceHercules: jdk8,
    microserviceZeus: jdk11
}

# "Low precision" measure names. For exercise group by measure name, we inflate these measure names into "high precision measure names"
# to do this each measure name is appended with string between '000' and '100', thus, there are 101 distinct strings. 
measureCpuUser = 'cpu_user'
measureCpuSystem = 'cpu_system'
measureCpuIdle = 'cpu_idle'
measureCpuIowait = 'cpu_iowait'
measureCpuSteal = 'cpu_steal'
measureCpuNice = 'cpu_nice'
measureCpuSi = 'cpu_si'
measureCpuHi = 'cpu_hi'
measureMemoryFree = 'memory_free'
measureMemoryUsed = 'memory_used'
measureMemoryCached = 'memory_cached'
measureDiskIoReads = 'disk_io_reads'
measureDiskIoWrites = 'disk_io_writes'
measureLatencyPerRead = 'latency_per_read'
measureLatencyPerWrite = 'latency_per_write'
measureNetworkBytesIn = 'network_bytes_in'
measureNetworkBytesOut = 'network_bytes_out'
measureDiskUsed = 'disk_used'
measureDiskFree = 'disk_free'
measureFileDescriptors = 'file_descriptors_in_use'

measureTaskCompleted = 'task_completed'
measureTaskEndState = 'task_end_state'
measureGcReclaimed = 'gc_reclaimed'
measureGcPause = 'gc_pause'

measuresForMetrics = [measureCpuUser, measureCpuSystem, measureCpuIdle, measureCpuIowait,
                      measureCpuSteal, measureCpuNice, measureCpuSi, measureCpuHi,
                      measureMemoryFree, measureMemoryUsed, measureMemoryCached, measureDiskIoReads,
                      measureDiskIoWrites, measureLatencyPerRead, measureLatencyPerWrite, measureNetworkBytesIn,
                      measureNetworkBytesOut, measureDiskUsed, measureDiskFree, measureFileDescriptors]

measuresForEvents = [measureTaskCompleted, measureTaskEndState, measureGcReclaimed, measureGcPause, measureMemoryFree]

measureValuesForTaskEndState = ['SUCCESS_WITH_NO_RESULT', 'SUCCESS_WITH_RESULT', 'INTERNAL_ERROR', 'USER_ERROR', 'UNKNOWN', 'THROTTLED']
selectionProbabilities = [0.2, 0.7, 0.01, 0.07, 0.01, 0.01]

DimensionsMetric = namedtuple('DimensionsMetric', 'region cell silo availability_zone microservice_name instance_type os_version instance_name')
DimensionsEvent = namedtuple('DimensionsEvent', 'region cell silo availability_zone microservice_name instance_name process_name, jdk_version')

def generateRandomAlphaNumericString(length = 5):
    rand = random.Random(12345)
    x = ''.join(rand.choice(string.ascii_letters + string.digits) for x in range(length))
    return x

def generateDimensions(scaleFactor):
    instancePrefix = generateRandomAlphaNumericString(8)
    dimensionsMetrics = list()
    dimenstionsEvents = list()

    for region in regions:
        cellsForRegion = cellsPerRegion[region]
        siloForRegion = siloPerCell[region]
        for cell in range(1, cellsForRegion + 1):
            for silo in range(1, siloForRegion + 1):
                for microservice in microservices:
                    cellName = "{}-cell-{}".format(region, cell)
                    siloName = "{}-cell-{}-silo-{}".format(region, cell, silo)
                    numInstances = scaleFactor * instancesForMicroservice[microservice]
                    for instance in range(numInstances):
                        az = "{}-{}".format(region, (instance % 3) + 1)
                        instanceName = "i-{}-{}-{:04}.amazonaws.com".format(instancePrefix, microservice, instance)
                        instanceType = instanceTypes[microservice]
                        osVersion = osVersions[microservice]
                        metric = DimensionsMetric(region, cellName, siloName, az, microservice, instanceType, osVersion, instanceName)
                        dimensionsMetrics.append(metric)

                        jdkVersion = jdkVersions[microservice]
                        for process in processNames[microservice]:
                            event = DimensionsEvent(region, cellName, siloName, az, microservice, instanceName, process, jdkVersion)
                            dimenstionsEvents.append(event)

    return (dimensionsMetrics, dimenstionsEvents)

def createWriteRecordCommonAttributes(dimensions):
    return { "Dimensions": [{ "Name": dimName, "Value": getattr(dimensions, dimName), "DimensionValueType": "VARCHAR"} for dimName in dimensions._fields] }

# Added batchSize and groupByMeasureName
def createRandomMetrics(hostId, timestamp, timeUnit, batchSize, groupByMeasureName):

    records = list()
    # per measure_name (20)
    recordsCpuUser = list()
    recordsCpuSystem = list()
    recordsCpuSteal = list()
    recordsCpuIdle = list()
    recordsCpuIowait = list()
    recordsCpuNice = list()
    recordsCpuSi = list()
    recordsCpuHi = list()
    recordsMemoryFree = list()
    recordsMemoryUsed = list()
    recordsMemoryCached = list()
    recordsDiskIoReads = list()
    recordsDiskIoWrites = list()
    recordsLatencyPerRead = list()
    recordsLatencyPerWrite = list()
    recordsNetworkBytesIn = list()
    recordsNetworkBytesOut = list()
    recordsDiskUsed = list()
    recordsDiskFree = list()
    recordsFileDescriptors = list()

    # Emulate high precision measurements (0 to 100, thus 101 entries) for 101 variables of the same metric name
    timestamp -= 101
    for i in range(101):
        #1
        if hostId in highUtilizationHosts:
            cpuUser = 85.0 + 10.0 * random.random()
        elif hostId in lowUtilizationHosts:
            cpuUser = 10.0 * random.random()
        else:
            cpuUser = 35.0 + 30.0 * random.random()
        record  = createRecord(measureCpuUser, cpuUser, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsCpuUser.append(record)

        # 2..7
        totalOtherUsage = 0.0
        value = random.random()
        totalOtherUsage += value
        record = createRecord(measureCpuSystem, value, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsCpuSystem.append(record)
        
        value = random.random()
        totalOtherUsage += value
        record = createRecord(measureCpuSteal, value, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsCpuIdle.append(record)
        
        value = random.random()
        totalOtherUsage += value
        record = createRecord(measureCpuIowait, value, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsCpuIowait.append(record)
                
        value = random.random()
        totalOtherUsage += value
        record = createRecord(measureCpuNice, value, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsCpuNice.append(record)
        
        value = random.random()
        totalOtherUsage += value
        record = createRecord(measureCpuHi, value, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsCpuHi.append(record)
        
        value = random.random()
        totalOtherUsage += value
        record = createRecord(measureCpuSi, value, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsCpuSi.append(record)
        
        # 8
        cpuIdle = max([100 - cpuUser - totalOtherUsage, 0])
        record = createRecord(measureCpuIdle, cpuIdle, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsCpuIdle.append(record)
        
        # 9..20
        value = 100.0 * random.random()
        record = createRecord(measureMemoryFree, value, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsMemoryFree.append(record)

        value = 100.0 * random.random()
        record = createRecord(measureMemoryUsed, value, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsMemoryUsed.append(record)
        
        value = 100.0 * random.random()
        record = createRecord(measureMemoryCached, value, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsMemoryCached.append(record)
        
        value = 100.0 * random.random()
        record = createRecord(measureDiskIoReads, value, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsDiskIoReads.append(record)

        value = 100.0 * random.random()
        record = createRecord(measureDiskIoWrites, value, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsDiskIoWrites.append(record)
        
        value = 100.0 * random.random()
        record = createRecord(measureLatencyPerRead, value, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsLatencyPerRead.append(record)
        
        value = 100.0 * random.random()
        record = createRecord(measureLatencyPerWrite, value, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsLatencyPerWrite.append(record)
        
        value = 100.0 * random.random()
        record = createRecord(measureNetworkBytesIn, value, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsNetworkBytesIn.append(record)
        
        value = 100.0 * random.random()
        record = createRecord(measureNetworkBytesOut, value, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsNetworkBytesOut.append(record)
        
        value = 100.0 * random.random()
        record = createRecord(measureDiskUsed, value, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsDiskUsed.append(record)
        
        value = 100.0 * random.random()
        record = createRecord(measureDiskFree, value, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsDiskFree.append(record)
        
        value = 100.0 * random.random()
        record = createRecord(measureFileDescriptors, value, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsFileDescriptors.append(record)

        timestamp += 1 # increment
    
    # batchSize, groupByMeasureName
    if groupByMeasureName == False:
        return records[0:batchSize] # no bother shuffle
    else:
        combined_list = [recordsCpuUser, recordsCpuSystem, recordsCpuSteal, recordsCpuIdle, recordsCpuIowait, 
                         recordsCpuNice, recordsCpuSi, recordsCpuHi, recordsMemoryFree, recordsMemoryUsed, 
                         recordsMemoryCached, recordsDiskIoReads, recordsDiskIoWrites, recordsLatencyPerRead, recordsLatencyPerWrite, 
                         recordsNetworkBytesIn, recordsNetworkBytesOut, recordsDiskUsed, recordsDiskFree, recordsFileDescriptors]
        # return recordsCpuUser[0:batchSize]
        # print(combined_list[random.randrange(20)][0:batchSize])
        # time.sleep(5)
        return combined_list[random.randrange(20)][0:batchSize]

# Added batchSize and groupByMeasureName
def createRandomEvent(timestamp, timeUnit, batchSize, groupByMeasureName):
    records = list()
    recordsTaskCompleted = list()
    recordsTaskEndState = list()
    recordsGcReclaimed = list()
    recordsGcPause = list()
    recordsMemoryFree = list()    

    timestamp -= 101
    for i in range(101):
        record = createRecord(measureTaskCompleted, random.randint(0, 500), "BIGINT", timestamp, timeUnit)
        records.append(record)
        recordsTaskCompleted.append(record)
        
        record = createRecord(measureTaskEndState, np.random.choice(measureValuesForTaskEndState, p=selectionProbabilities), "VARCHAR", timestamp, timeUnit)
        records.append(record)
        recordsTaskEndState.append(record)
        
        value = 100.0 * random.random()
        record = createRecord(measureGcReclaimed, value, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsGcReclaimed.append(record)
        
        value = 100.0 * random.random()
        record = createRecord(measureGcPause, value, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsGcPause.append(record)
        
        value = 100.0 * random.random()
        record = createRecord(measureMemoryFree, value, "DOUBLE", timestamp, timeUnit)
        records.append(record)
        recordsMemoryFree.append(record)

        timestamp += 1 # increment
    #
    if groupByMeasureName == False:
        return records[0:batchSize]
    else:
        combined_list = [recordsTaskCompleted, recordsTaskEndState, recordsGcReclaimed, recordsGcPause, recordsMemoryFree]
        # return (recordsTaskCompleted[0:batchSize])
        # print(combined_list[random.randrange(5)][0:batchSize])
        return combined_list[random.randrange(5)][0:batchSize]    

def createRecord(measureName, measureValue, valueType, timestamp, timeUnit):
    return {
        "MeasureName": measureName,
        "MeasureValue": str(measureValue),
        "MeasureValueType": valueType,
        "Time": str(timestamp),
        "TimeUnit": timeUnit
    }

seriesId = 0
timestamp = int(time.time())
sigInt = False
lock = threading.Lock()
utilizationRand = random.Random(12345)
lowUtilizationHosts = []
highUtilizationHosts = []

def signalHandler(sig, frame):
    global sigInt
    global lock
    global sigInt

    with lock:
        sigInt = True

#########################################
######### Ingestion Thread ###############
#########################################
class IngestionThread(threading.Thread):
    def __init__(self, tsClient, threadId, args, dimensionMetrics, dimensionEvents):
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.args = args
        self.dimensionMetrics = dimensionMetrics
        self.dimensionEvents = dimensionEvents
        self.client = tsClient
        self.databaseName = args.databaseName
        self.tableName = args.tableName
        self.numMetrics = len(dimensionMetrics)
        self.numEvents = len(dimensionEvents)

        # Take batch size and group by measure name flag.
        self.batchSize = args.batch_size
        self.groupByMeasureName = args.group_by_measure_name
        assert isinstance(self.batchSize, int) and self.batchSize > 0 and self.batchSize <= 101
        assert isinstance(self.groupByMeasureName, bool)

    def run(self):
        global seriesId
        global timestamp
        global lock

        timings = list()
        success = 0
        idx = 0

        while True:
            with lock:
                if sigInt == True:
                    print("Thread {} exiting.".format(self.threadId))
                    break

                seriesId += 1
                if seriesId >= self.numMetrics + self.numEvents:
                    seriesId = 0
                    timestamp = int(time.time())
                    now = datetime.datetime.now()
                    print("""Resetting to first series from thread: [{}] at time {}. 
                    Timestamp set to: {}.""".format(self.threadId, now.strftime("%Y-%m-%d %H:%M:%S"), timestamp))

                localSeriesId = seriesId
                localTimestamp = timestamp  # Base time stamp

            while True:
                if localSeriesId < self.numMetrics:
                    commonAttributes = createWriteRecordCommonAttributes(self.dimensionMetrics[localSeriesId])
                    records = createRandomMetrics(seriesId, localTimestamp, "SECONDS", self.batchSize, self.groupByMeasureName)
                else:
                    commonAttributes = createWriteRecordCommonAttributes(self.dimensionEvents[localSeriesId - self.numMetrics])
                    records = createRandomEvent(localTimestamp, "SECONDS", self.batchSize, self.groupByMeasureName)
                if len(records) >0:
                    break

            idx += 1
            start = timer()
            try:
                writeResult = writeRecords(self.client, self.databaseName, self.tableName, commonAttributes, records)
                success += 1
            except Exception as e:
                # print(e)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_exception(exc_type, exc_value, exc_traceback, limit=2, file=sys.stdout)
                requestId = "RequestId: {}".format(e.response['ResponseMetadata']['RequestId'])
                print(requestId)
                print(json.dumps(commonAttributes, indent=2))
                print(json.dumps(records, indent=2))
                continue
            finally:
                end = timer()
                timings.append(end - start)

            requestId = writeResult['ResponseMetadata']['RequestId']
            if idx % 100 == 0:
                now = datetime.datetime.now()
                print("{}. {}. {}. RequestId: {}. Time: {}.".format(self.threadId, idx, now.strftime("%Y-%m-%d %H:%M:%S"), requestId, round(end - start, 3)))

        self.success = success
        self.timings = timings


#########################################
######### Ingest load ###################
#########################################

def ingestRecords(tsClient, dimensionsMetrics, dimensionsEvents, args):
    numThreads = args.concurrency

    ingestionStart = timer()
    timings = list()
    threads = list()

    for threadId in range(numThreads):
        print("Starting ThreadId: {}".format(threadId + 1))
        thread = IngestionThread(tsClient, threadId + 1, args, dimensionsMetrics, dimensionsEvents)
        thread.start()
        threads.append(thread)

    success = 0
    for t in threads:
        t.join()
        success += t.success
        timings.extend(t.timings)

    print("Total={}, Success={}, Avg={}, Stddev={}, 50thPerc={}, 90thPerc={}, 99thPerc={}".format(len(timings), success,
                                                                                                  round(np.average(timings), 3),
                                                                                                  round(np.std(timings), 3), round(np.percentile(timings, 50), 3),
                                                                                                  round(np.percentile(timings, 90), 3), round(np.percentile(timings, 99), 3)))

    ingestionEnd = timer()
    print("Total time to ingest: {} seconds".format(round(ingestionEnd - ingestionStart, 2)))

#########################################
######### Timestream API calls ##########
#########################################
def createWriteClient(region, profile = None):
    if profile == None:
        print("Using credentials from the environment")

    print("Connecting to timestream ingest in region: ", region)
    config = Config()
    if profile != None:
        session = boto3.Session(profile_name = profile)
        client = session.client(service_name = 'timestream-write',
                                region_name = region, config = config)
    else:
        session = boto3.Session()
        client = session.client(service_name = 'timestream-write',
                                region_name = region, config = config)
    return client

def describeTable(client, databaseName, tableName):
    response = client.describe_table(DatabaseName = databaseName, TableName = tableName)
    print("Table Description:")
    pprint.pprint(response['Table'])

def writeRecords(client, databaseName, tableName, commonAttributes, records):
    return client.write_records(DatabaseName = databaseName, TableName = tableName,
                                CommonAttributes = (commonAttributes), Records = (records))

#########################################
######### Main ##########
#########################################
if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog = 'Demo on the single/multiple threads, variable batch sizes and grouping by measure_name',
                                     description='Ingesting synthetic DevOps-type time series data. Concurrent, variable batch sizes and measure name grouping')

    parser.add_argument('--database-name', '-d', dest="databaseName", action = "store", required = True,
                        help = "The database name in Amazon Timestream - must be already created.")
    parser.add_argument('--table-name', '-t', dest="tableName", action = "store", required = True,
                        help = "The table name in Amazon Timestream - must be already created.")
    parser.add_argument('--endpoint', '-e', action = "store", required = True,
                        help="Specify the service region endpoint. E.g. 'us-east-1'")
    parser.add_argument('--concurrency', '-c', action = "store", type = int, default = 10,
                        help = "Number of concurrent ingestion threads (default: 10)")
    parser.add_argument('--host-scale', dest = "hostScale", action = "store", type = int, default = 10,
                        help = "The scale factor that determines the number of hosts emitting events and metrics (default: 10).")
    parser.add_argument('--profile', action = "store", type = str, default= None, help = "The AWS Config profile to use.")

    parser.add_argument('--batch-size', '-b',
                        type = int,
                        default = 10,
                        help = """The number of records in a batch (default: 10). 
                        If set for > 100, it causes ValidationException as a response of the actual ingestion.
                        Just for exercises, set 101 as the upper limit of this command line option. If set 102 or higher, 
                        the simple sanity check fails (in class IngestionThread), rather than actual ingestion after record batching""")
    
    parser.add_argument('--group-by-measure-name', '-g',
                        type = eval,
                        choices = [True, False],
                        default = False,
                        help = "True or False for grouping the same measure name into a batch in write_records for record batching")
    
    args = parser.parse_args()
    print(args)

    hostScale = args.hostScale       # scale factor for the hosts.

    dimensionsMetrics, dimensionsEvents = generateDimensions(hostScale)

    print("(Low precision) Dimensions for metrics: {}".format(len(dimensionsMetrics)))
    print("(Low precision) Dimensions for events: {}".format(len(dimensionsEvents)))

    hostIds = list(range(len(dimensionsMetrics)))
    utilizationRand.shuffle(hostIds)
    lowUtilizationHosts = frozenset(hostIds[0:int(0.2 * len(hostIds))])
    highUtilizationHosts = frozenset(hostIds[-int(0.2 * len(hostIds)):])

    ## Register sigint handler
    signal.signal(signal.SIGINT, signalHandler)

    ## Verify the table
    try:
        tsClient = createWriteClient(args.endpoint, profile=args.profile)
        describeTable(tsClient, args.databaseName, args.tableName)
    except Exception as e:
        print(e)
        sys.exit(0)

    ## Run the ingestion load.
    ingestRecords(tsClient, dimensionsMetrics, dimensionsEvents, args)
