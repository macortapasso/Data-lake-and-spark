# Data Lake with Spark

## **Description**

A music streaming startup, Sparkify, wants to build a data lake to levarege to the analytics teams capabilities and help them continue finding insights into what songs their users are listening to.

The project consists of extracting JSON files from the AWS S3, set the dimensions and fact tables, develop the AWS infrastructure as code (IaC) to run a Spark job in EMR, and, finally, load parquet files back into S3, in the Data Lake.


## **Code**

- app.log: logs from etl.py execution.
- Data Exploration.ipynb: notebook used to explore json files before building the ETL.
- dl.cfg: parameters from AWS.
- emr_bootstrap.sh: shell to set EMR cluster and call the etl.py from S3. The etl.py must be stored at s3://udacity-de-python-etl-script, a public bucket.
- etl.py: spark job that processes the song and log json files.
- local.py: run etl.py locally.
- main.py: run etl.py on AWS EMR.


## **Dataset**

### Song Dataset
Songs dataset is a subset of [Million Song Dataset](http://millionsongdataset.com/).

Sample Record :
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

### Log Dataset

Sample Record :
```
{"artist": null, "auth": "Logged In", "firstName": "Walter", "gender": "M", "itemInSession": 0, "lastName": "Frye", "length": null, "level": "free", "location": "San Francisco-Oakland-Hayward, CA", "method": "GET","page": "Home", "registration": 1540919166796.0, "sessionId": 38, "song": null, "status": 200, "ts": 1541105830796, "userAgent": "\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"", "userId": "39"}
```

## **Database schema**


### Fact Table 
**songplays** - records in log data associated with song plays i.e. records with page `NextSong`

```
ts, year, month, user_id, level, song_id, artist_id, session_id, location, user_agent
```

###  Dimension Tables
**users**  - users in the app
```
user_id, first_name, last_name, gender, level
```
**songs**  - songs in music database
```
song_id, title, artist_id, year, duration
```
**artists**  - artists in music database
```
artist_id, name, location, latitude, longitude
```
**time**  - timestamps of records in  **songplays**  broken down into specific units
```
ts, start_time, hour, day, week, month, year, weekday
```

## **Running**

First of all, it is required to fill up the dl.cfg file with *aws_access_key_id* and *aws_secret_access_key*.

To run locally:
```
python local.py
```

To run in AWS EMR:

The bootstrap will call etl.py at s3://udacity-de-python-etl-script.
```
python main.py
```


## **References**

* [Create an EMR cluster and submit a job using Boto3](https://kulasangar.medium.com/create-an-emr-cluster-and-submit-a-job-using-boto3-c34134ef68a0/)
* [Getting Started with PySpark on AWS EMR](https://towardsdatascience.com/getting-started-with-pyspark-on-amazon-emr-c85154b6b921/)
