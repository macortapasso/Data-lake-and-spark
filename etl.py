import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


def create_spark_session():
    """
    Create Spark Sessin on EMR cluster.

    return:
        spark session
    """
    spark = (
        SparkSession.builder.appName("Data Lake App")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
        .getOrCreate()
    )
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process the files that contain the songs and store the output data in an S3 bucket.
    Store song_table and artists_table into S3 data lake.

    args:
        spark: spark session
        input_date: AWS S3 bucket where the files to be processed are stored
        output_date: AWS S3 bucket where the processed data are stored
    """

    # get filepath to song data file
    song_data = input_data + "song-data/A/A/*"

    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_table")

    # extract columns to create songs table
    songs_table = spark.sql(
        """
           select 
               song_id,
               title,
               artist_id,
               year,
               duration
           from 
               song_table
           where
               song_id is not null
        """
    )

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(
        output_data + "songs_table/"
    )

    # extract columns to create artists table
    artists_table = spark.sql(
        """
            select 
                distinct artist_id,
                artist_name,
                artist_location,
                artist_latitude,
                artist_longitude
            from 
                song_table
            where
                artist_id is not null
        """
    )

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists_table/")


def process_log_data(spark, input_data, output_data):
    """
    Process the files that contain the logs and store the output data in an S3 bucket.
    Store artist_table, songplay_table, and time table into S3 data lake.

    args:
        spark: spark session
        input_date: AWS S3 bucket where the files to be processed are stored
        output_date: AWS S3 bucket where the processed data are stored
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    df.createOrReplaceTempView("log_table")

    # extract columns for users table
    users_table = spark.sql(
        """            
           select 
               distinct  userId user_id,
               firstName first_name,
               lastName last_name,
               gender,
               level 
           from 
               log_table    
           where 
               userId is not null
        """
    )

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users_table/")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000.0)
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(
        lambda x: datetime.utcfromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S")
    )
    df = df.withColumn("datetime", get_datetime(df.timestamp))

    df.createOrReplaceTempView("log_table")

    # extract columns to create time table
    time_table = spark.sql(
        """
            select 
                distinct ts,
                datetime start_time,
                extract(hour from datetime) as hour,
                extract(day from datetime) as day,
                extract(week from datetime) as week, 
                extract(month from datetime) as month,
                extract(year from datetime) as year, 
                extract(dayofweek from datetime) as day_of_week
            from 
                log_table
        """
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time_table/")

    # Use song and artists table produce songplays table
    song_df = spark.read.parquet(output_data + "songs_table/")
    song_df.createOrReplaceTempView("songs_table")
    artist_df = spark.read.parquet(output_data + "artists_table/")
    artist_df.createOrReplaceTempView("artists_table")
    artist_song_df = spark.sql(
        """
            select
                a.artist_id,
                s.song_id,
                a.artist_name,
                s.title,
                s.duration
            from 
                songs_table s
            inner join 
                artists_table a
            on s.artist_id = a.artist_id
        """)
    artist_song_df.createOrReplaceTempView("artists_songs")

    # use time table to find year and month
    time_table.createOrReplaceTempView("time_table")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql(
        """
            select  
                t.ts,
                t.year,
                t.month,
                l.userId as user_id,
                l.level,
                s.song_id,
                s.artist_id,
                l.sessionId as session_id,
                l.location,
                l.userAgent as user_agent
            from 
                log_table l
            
            inner join
                artists_songs s
            on l.song = s.title and l.artist = s.artist_name and l.length = s.duration
            
            inner join 
                time_table t
            on t.ts = l.ts
        """
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(
        output_data + "songplays_table/"
    )


def main():
    spark = create_spark_session()

    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-de-output-data-lake/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
