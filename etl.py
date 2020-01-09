import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import hour, dayofmonth, weekofyear, month, year, dayofweek
from datetime import datetime
from pyspark.sql.types import *
    
config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    """
    This function returns a spark session to work with data loaded from S3.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    This function uses SparkSession to read raw songs data from S3, transform it and save it back to S3
    in the form of two dimension tables: songs (which is partitioned by year and artist_id) and artists.
    """
    # set filepath to folder containing all song data files
    song_data = input_data + "song-data/*/*/*/*.json"
    
    # read song data file
    print("Loading songs data from " + input_data + " ...")
    songs_df = spark.read.json(song_data)
    print("Loading complete!")

    # extract columns to create songs table
    songs_table = songs_df.select(['year', 'artist_id', 'song_id', 'title', 'duration']).dropDuplicates()
    
    print("Writing songs data into " + output_data)
    # write songs table to partitioned parquet files to optimise and save space
    songs_table.write.format('parquet').mode('overwrite').partitionBy("year", "artist_id").save(output_data + 'songs')
    print("Writing complete!")
    
    # extract columns to create artists table
    # since we are creating a artists table from a songs table, artists possibly are duplicated, hence it's best to drop those.
    artists_table = songs_df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']).dropDuplicates()
    
    # write artists table to parquet files
    print("Writing artists data into " + output_data)
    artists_table.write.format('parquet').mode('overwrite').save(output_data + 'artists')
    print("Writing complete!")


def process_log_data(spark, input_data, output_data):
    """
    This function uses SparkSession to read raw songs data from S3, transform it and save it back to S3
    in the form of three dimension tables: users, time and songplays. The last two are partitioned by year and month.
    """
    
    # set filepath to folder containing all log data files
    log_data = input_data + "log_data/*/*/*.json"
    
    # read log data file and filter it by song plays only
    print("Loading log data from " + input_data + "...")
    log_df = spark.read.json(log_data)
    print("Loading complete!")
    log_df = log_df.filter(log_df.page == 'NextSong')

    # extract columns for users table      
    users_table = log_df.select(['userId', 'firstName', 'lastName', 'gender', 'level']).dropDuplicates()
    
    print("Writing users data into " + output_data)
    # write users table to partitioned parquet files to optimise and save space
    users_table.write.format('parquet').mode('overwrite').save(output_data+'users')
    print("Writing complete!")

    # create timestamp column from original unix timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts/1000.0), TimestampType())
    log_df = log_df.withColumn('timestamp', get_timestamp('ts'))
    
    # create datetime column from original unix timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts/1000.0), DateType())
    log_df = log_df.withColumn('datetime', get_datetime('ts'))
    
    # extract several time measurements from timestamp to create several columns
    time_table = log_df.select(['ts', 'timestamp']).withColumn('year', year('timestamp')).withColumn('month', month('timestamp')).withColumn('week', weekofyear('timestamp')).withColumn('weekday', dayofweek('timestamp')).withColumn('day', dayofmonth('timestamp')).withColumn('hour', hour('timestamp')).dropDuplicates()
    
    print("Writing time data into " + output_data)
    # write time table to parquet files partitioned by year and month
    time_table.write.format('parquet').partitionBy(['year', 'month']).mode('overwrite').save(output_data + 'time')
    print("Writing complete!")

    # read songs data from S3 once again
    song_data = input_data + "song-data/*/*/*/*.json"
    print("Loading songs data from " + input_data + " ...")
    songs_df = spark.read.json(song_data)
    
    # extract columns from both song and log datasets via a SQL query to create the songplays table 
    songs_df.createOrReplaceTempView('songs')
    log_df.createOrReplaceTempView('log')
    songplays_table = spark.sql(
        """
        SELECT
            l.timestamp as start_time,
            year(l.timestamp) as year,
            month(l.timestamp) as month,
            l.userId as user_id,
            l.level as level,
            s.song_id,
            s.artist_id,
            l.sessionId as session_id,
            s.artist_location as location,
            l.userAgent as user_agent
        FROM log l
        JOIN songs s
            ON s.title = l.song
            AND l.artist = s.artist_name
        """).dropDuplicates()
            
    print("Writing songplay data into " + output_data)
    # write songplays table to partitioned parquet files to optimise and save space
    songplays_table.write.partitionBy(['year', 'month']).format('parquet').mode('overwrite').save(output_data + 'songplays')
    print("Writing complete!")


def main():
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lake-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
