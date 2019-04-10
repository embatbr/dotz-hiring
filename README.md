# dotz-hiring

*Solution for Dotz data engineering hiring challenge*


## Architecture

The GCP project ID is **dotz-hiring**.

### Data Lake

The data lake is a bucket in GCP Storage. This bucket is divided in sections **raw**, for the datasets provided, and **processed** for ETL results.

### Logic

GCP Dataflow is used to execute the logic, that is basically read the raw data, process it (distributing in proper table-based files) and store in the data lake (section **processed**).

Besides Dataflow, GCP App Engine is used to manage the entire ETL. After the Dataflow stage, the processed data is consolidated into GCP BigQuery.

### DW

A data warehouse with BigQuery, used only after the ETL ends and to create reports.


## Development flow

### 1. Setting the data lake

1. Created a bucket named **dotz-hiring-datalake**. This is the data lake;
2. Uploaded the [provided files](./storage/raw):
    - Command: `gsutil cp -r ./storage/raw gs://dotz-hiring-datalake/raw`;
    - Files located [here](https://console.cloud.google.com/storage/browser/dotz-hiring-datalake/raw).

### 2. Modeling the objects

The raw files contain 3 types of data (already given with it's proper tables):

- components (comp_boss): details the tubes components (i.e., a tube section, a shoulder and etc.);
- materials (bill_of_materials): each row is a set of tubes (you may use a simple tube, conected with a should at one end and a "ternary" shoulder at other end);
- pricing (price_quote): prices quoted in relation to the amount required, the date and etc.

These files are each one related to a table. The most basic one is **components**. It's records are referenced in **materials**, and these are referenced in table **pricing**.

Tables schemas are defined in directory *schemas*. The tables are defined initially in a SQL for a PostgreSQL-like database. This step is intended to give me a better perspective. After that, when the Dataflow logic is completed, the BigQuery tables will be defined.

### 3. Writing ETL

The ideal ETL for this challenge is to have a cron job running (Cloud Scheduler API, maybe) and starting the Dataflow script to execute all the logic. After that, the job uploads the clean data to BigQuery.

Initially it will be written only with Dataflow, and the cron job will be added after, if needed.
