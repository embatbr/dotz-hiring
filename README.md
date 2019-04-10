# dotz-hiring

*Solution for Dotz data engineering hiring challenge*


## Architecture

The GCP project ID is **dotz-hiring**.

### Data Lake

The data lake is a bucket in GCP Storage. This bucket is divided in sections **raw**, for the datasets provided, and **processed** for ETL results.

### Logic

GCP Dataflow is used to execute the logic, that is basically read the raw data, process it (distributing in proper table-base files) and store in the data lake (section **processed**).

Besides Dataflow, GCP App Engine is used to manage the entire ETL. After the Dataflow stage, the processed data is consolidated into GCP BigQuery.

### DW

A data warehouse with BigQuery, used only after the ETL ends and to create reports.


## Development flow

### 1. Setting the data lake

1. Created a bucket named **dotz-hiring-datalake**. This is the data lake;
2. Uploaded the [provided files](./storage/raw):
    - Command: `gsutil cp -r ./storage/raw gs://dotz-hiring-datalake/raw`;
    - Files located [here](https://console.cloud.google.com/storage/browser/dotz-hiring-datalake/raw);
    - Viewers with permission to read objects.
