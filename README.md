# Data Modelling with AWS Data Warehouses (Redshift)

This repository serves as a submission for Udacity data engineer nanodegree.

## Project Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## How to Run?

This section describes how to get use this repositrory.

**Redshift Cluster Setup**

To run this project you will need connectivity to an AWS Redshift database.
The setup_redshift.ipynb notebook will setup the cluster.
In order for the notebook to run you will need a "dwh_udacity.cgf" file in 
the same folder as the notebook, with below structure.
```
[AWS]
KEY=
SECRET=

[DWH] 
DWH_CLUSTER_TYPE=multi-node
DWH_NUM_NODES=4
DWH_NODE_TYPE=dc2.large

DWH_IAM_ROLE_NAME=
DWH_CLUSTER_IDENTIFIER=
DWH_DB=
DWH_DB_USER=dwhuser
DWH_DB_PASSWORD=
DWH_PORT=
```

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
DB_USER=''
DB_PASSWORD=''
DB_PORT=''

[IAM_ROLE]
ARN=

[S3]
LOG_DATA=''
LOG_JSONPATH=''
SONG_DATA=''
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

## Database design

The goal of the project is to run efficient queries on song playing analytics which is mainly sotred in the songplays table.
The design of the tables remains the same as before but the main difference
now is taht the DB is deployed in a IAAS environemnt which is highly scalable.

The ETL is integrated to read from AWS S3 buckets into staging table on the
Redshift cluster. Then additional queries trasnform and move the data into
their final tables to be used for analytics.
