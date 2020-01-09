# Managing a Data Lake for Sparkify

This project summarises efforts to extract raw songplay data and log data from a S3 bucket, to transform those and ingest them back to S3 as a set of analytical tables: 1 fact table (songplays) and 4 dimension tables (songs, artists, users, and datetime). 

# ETL schema

# Files

### etl.py
Main Python script for extracting raw data, transforming columns, creating fact and dimension tables and loading them back to S3.

### dl.cfg
Where one should place their AWS credentials to access S3.

# How to execute

Clone the remote repository:
```
git clone https://github.com/tuliorc/dl_sparkify.git
```

Go into your new local repository:
```
cd dl_sparkify
```

Make sure you have Python3 installed in your computer:
```
python -V
```

In case you don't, install it:
```
sudo apt-get update
sudo apt-get install python3.6
```
Then, execute the ETL script to load the songs/log JSON data into Spark dataframes and ingest them back to S3 as analytics tables:
```
python3 etl.py
```
This will allow you to perform queries efficiently on the new S3 bucket later on.
