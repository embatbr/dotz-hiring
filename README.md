# dotz-hiring

*Solution for Dotz data engineering hiring challenge*


## Architecture

The ideal architecture using [GCP](https://cloud.google.com/) is to have the raw data (the CSV files provided) stored in Cloud Storage (data lake), process it (clean and transform into a more suitable format - if needed) using Cloud Dataflow, store the processed files (may have more than 3) in Cloud Storage again and use BigQuery to explore and extract reports.

Althought this is the best strategy, the development must be executed in intermediary stages. Before using the cloud, it is necessary to test the logic. Due to this choice, the architecture will be provided in incremental stages

### Stage 1: Complete local environment

The local environment is composed of

- Data Lake: a directory in the repository's root named **storage**, with the following subdirectories: **raw** and **processed**. This represents the data lake;
- Logic: the logic ought to be executed in Cloud Dataflow. To represent it in this early development stage, the directory named **dataflow** will contain the Python files (the logic must be modular);
- DW: a local PostgreSQL will be used as data warehouse. This will be replaced by BigQuery (or other GCP suitable tool).

The ETL, roughly, is:

1. Data stored in data lake's **raw** section is read by Dataflow;
2. Dataflow processed the data;
3. The data is stored in the data lake again, although this time in the **processed** section;
4. Data is uploaded to the DW;
5. Reports are generated.
