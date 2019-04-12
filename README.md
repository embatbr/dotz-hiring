# dotz-hiring

*Solution for Dotz data engineering hiring challenge*


## Architecture

The GCP project ID is **dotz-hiring**.

### Data Lake

The data lake is a bucket in GCP Storage. This bucket is divided in sections **raw**, for the datasets provided, and **processed** for ETL results.

### Logic

GCP Dataflow is used to execute the logic, that is basically read the raw data, process it (distributing in proper table-based files) and store in the data lake (section **processed**).

### DW

A data warehouse with BigQuery, used only after the ETL ends and to create reports.


## Installing

Create a virtual environment and install the package for Apache Beam:

```bash
$ mkvirtualenv --python=$(which python2) dotz-hiring
$ pip install apache-beam[gcp]
```


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

These files are each one related to an external table. The most basic one is **components**. It's records are referenced in **materials**, and these are referenced in table **pricing**.

Tables schemas are defined in directory *schemas*. To create the tables:

```bash
$ bq mk --table dotz-hiring:tubulation.components schemas/components.json
$ bq mk --table dotz-hiring:tubulation.materials schemas/materials.json
$ bq mk --table dotz-hiring:tubulation.pricing schemas/pricing.json
```


### 3. Writing ETL

The ETL is written in Python 2.7 and executed in Cloud Dataflow.

I created a service account named **master** with role owner and created a key. After downloading the credentials file, an environment variable **GOOGLE_APPLICATION_CREDENTIALS** containing the credentials filepath is exported and the job is submitted:

```bash
$ export GOOGLE_APPLICATION_CREDENTIALS=credentials/dotz-hiring-a64a44a8ad2b.json
$ python dataflow.py --process (components|materials|pricing)
```

The ETL **extracts** data from CSV files, **transforms** it and **loads** to Cloud Storage and Cloud BigQuery.
