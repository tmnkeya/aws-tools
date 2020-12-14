# Single ingestion process (thread)

## Overview

* Code demonstrates three basic ingestion scenarios of recording batching.
* Batching of records count 1 to 100. Batch-size 1 is an single record ingestion scenarios.
* It can be either with or without use of common attributes.
* Code also demonstrates simple reponse time measurements for ingestion operations.
* Code also demonstrates exception cathing. One is RejectedRecordsException and another is ValidationException.

## Setup
* Prerequisite
  At your python 3.x environment. Have EC2 (oregon) assigned a role for creating, deleting TimeStream databases and tables.
* pipreqs .
* pip install -r requirements

##Demo
* config.json
  Configure your parameters.

* manage.py: delete and create a database and an empty table for ingestion exercies
  Usage: python manage.py

  Note that we run this and make sure to wait for a while so that the instances are consistently in its existence in the server side.
  If you set up the instances, and immediately do ingestion etc, you may find errors due to non-exisiting database or tables.
  (Eventual consistency?)
  
* ingestion.py: one scenarios
  Ex. python ingestion -b 10 -n 1000 -c True
      batch size 10, total records 1000, use of common attribute yes.

* ingestion_loop.py: looping a prescribed combinations of batch sizes, total records count, and common attributes (True or False) and output results (csv)

* ingestion_rejected_records_exception.py

  Ex. python ingestion_rejected_records_exception.py
      This generates RejectedRecordsException, as code deliverately duplicates a few timestamp values to intentionally cause this exception.
      
  Ex. pythion ingestion_rejected_records_exception.py -b 101 -n 100
      This works as the argument batch is set to 101, code aggregates up to 100 records to call a write_records(). But it leaves 1 record uningested and finishes.
      
  Ex. pythion ingestion_rejected_records_exception.py -b 101 -n 101
      This generates ValidationException, as it aggregates 101 records and try to call a write_records(). 

##Ref:
* https://docs.aws.amazon.com/timestream/latest/developerguide/ts-limits.html
* https://docs.aws.amazon.com/timestream/latest/developerguide/APITimestreamSpecificErrors.html

# Note that this code originally derived from this blog.
* https://aws.amazon.com/blogs/aws/store-and-access-time-series-data-at-any-scale-with-amazon-timestream-now-generally-available/

  Added database management element (manage.py).


