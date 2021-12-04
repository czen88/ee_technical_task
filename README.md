## Exercise Instructions

This is a bootstrap project to load interesting data from a Stack Exchange dataset into a data warehouse.
You are free to change anything about this bootstrap solution as you see fit, so long as it can still be executed by a reviewer.

- The project is set up to use Pipenv & Python 3.8
- SQLite3 provides an infrastructure-free simple data warehouse stand-in
- Facilites for linting etc. are provided as scripts and integrated with Pipenv

[scripts/fetch_data.sh](scripts/fetch_data.sh) is provided to download and decompress the dataset.

Your task is to make the Posts and Tags content available in an SQLite3 database.
[src/main.py](src/main.py) is provided as an entrypoint, and has an example of parsing the source data.
[src/db.py](src/db.py) is empty, but the associated test demonstrates interaction with an SQLite3 database.
You should ensure your code is correctly formatted and lints cleanly.

You will aim to make it convenient for data scientists to execute analytics-style queries reliably over the Posts and Tags tables.
You will be asked to demonstrate the solution, including:
- how you met the data scientist needs
- how you did (or would) ensure data quality
- what would need to change for the solution scale to work with a 10TB dataset with new data arriving each day

## How to run data load

### Fetch Data
Firstly we need to download data for Stack Exchange dataset which will be stored in [uncommitted](uncommitted) folder.
```
pipenv run fetch_data
```

### Run Data Load
Execute below command to load XML data into SQLite.
```
pipenv run python src/main.py
```
The process does the following:
- Parse XML for Posts and Tags datasets
- Store data in staging area as Parquet files in [uncommitted](uncommitted) folder
- Run validation checks using Pydeequ library
- Create posts_tags table to store many-to-many relationship between Posts and Tags
- Store data in SQLite database [warehouse.db](uncommitted/warehouse.db) if checks are passed

Results of validation checks are stored in `check_results` table

## Assumptions
- We are doing full data load. Incremental data load may be a better strategy provided that data source can support it

## Key design decisions
- Using PySpark for handling data load and validation. This ensures that solution can scale to work with 10TB dataset.
- Using [spark-xml](https://github.com/databricks/spark-xml) library to parse XML files.
- Using [pydeequ](https://github.com/awslabs/python-deequ) library for data validation.
- Using SQLite DB as requested in the task. To scale this solution it is suggested to use production-grade 
  data warehouse such as [Snowflake](https://www.snowflake.com/), [Amazon Redshift](https://aws.amazon.com/redshift/) or similar.
- Data checks implemented are not covering all possible cases. The aim here is just to showcase the functionality

## How to work with data
For Data Scientists data analysis it is proposed to use the following approach:
- Join posts and tags tables using posts_tags table (PostId and TagId columns are FK to posts and tags tables)
- Use Spark for complex data analysis and for developing data science models
- Use Data Warehouse for ad-hoc data analysis and reporting

## Production Deployment Ideas
Production deployment may involve using below services:
- Storage: AWS S3, Parquet
- Compute: AWS Lambda / AWS Glue / AWS EMR
- Data Warehouse: AWS Redshift / Snowflake

## Development
### Run Tidy and Lint
```
pipenv run tidy
pipenv run lint
```

### Run Tests
```
pipenv run test
```
