# Data Lake with AWS (EMR Cluster)

This repository serves as a submission for Udacity data engineering nanodegree.

## Project Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## How to Run?

This section describes how to get use this repositrory.


**Python environemnt setup**
```
pip install -r requirements.txt
```
In order to run the table creation and the ETLs you will need a "dwh.cfg" file
in the same folder as the .py files with the below structure.
```
[CLUSTER]
HOST=''
DB_NAME=''

```

**Initialize the database**
```
python create_tables.py
```

**Run the ETL pipeline**
```
python etl.py
```

## Project Structure
```
\create_tables.py --> script to create the database and tables
\etl.py --> script which runs the etl pipeline
\sql_queries.py --> contains SQL queries run throughout the project
\setup_redshift.ipynb --> jupyter notebook to setup redshift, IAM roles.
```
