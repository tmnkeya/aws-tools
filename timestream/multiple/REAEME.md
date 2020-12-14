# Key modifications

Two sources of the awslab source code are customized:

* https://github.com/awslabs/amazon-timestream-tools/tree/ed70cf03415bb1b95e3a27fe36cfdb57e2b04358/sample_apps/python
* https://github.com/awslabs/amazon-timestream-tools/tree/ed70cf03415bb1b95e3a27fe36cfdb57e2b04358/tools/continuous-ingestor

1. Took database management (database and table) to make up tsdb_manager.py

2. Took continuous ingestion and added 21th entries of a "records" list called 'measurePhysical' which
   has two elements temperatures and humidity to make up ts_cont_ingestor.py
