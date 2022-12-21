# Data Lake with AWS (EMR Cluster)

This repository serves as a submission for Udacity data engineering nanodegree.

## Project Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## What does the project do?
This project reads data from a specified location on S3 or locally.
Transforms it into a desired Dim-Fact schema and writes the results back to either S3 or local disk.

## How to Run?

This section describes how to get use this repositrory.

to run this project all it takes is the below command
```
python etl.py
```
However there are some configurations which depends on where you want to read data from:

### Read from S3 / Write to S3

For AWS S3 connection you will need a dl.cfg file next to etl.py with the below contents:
```
[AWS]
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
```
Fill it with your credentials.

You will also need a bucket for the output.
The name of the bucket can be adjusted on line 119:
```
output_data = "s3a://output-udacity/" #s3
```

### Read from Localhost / Write to Localhost

To read the data locally you will first need the data. Located next to etl.py.
```
data/
    song-data
        A
        ...
    log-data
        2018-11-02-events.json
        ...
output/
```

You will comment out lines 118 and 119 and use lines 120 and 121.
```
# input_data ="s3a://udacity-dend/" #s3
# output_data = "s3a://output-udacity/" #s3
input_data = "data" # localhost
output_data = "output" # locahost
```

You will also need to adjust the reading pattern on line 53:
```
'log-data/*/*'  ->  'log-data/*.json'
```
The left version works on S3 and the right version works on local files.


