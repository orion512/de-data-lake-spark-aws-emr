import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song-data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select([
        "song_id", "title", "artist_id", "year", "duration"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy(
        "year", "artist_id"
        ).parquet(os.path.join(output_data, "songs.parquet"))

    # extract columns to create artists table
    artists_table = df.select([
        "artist_id", "artist_name", "artist_location", 
        "artist_latitude", "artist_longitude"])
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(
        os.path.join(output_data, "artists.parquet"))


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log-data/*/*')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table    
    users_table = df.select([
        'userId', 'firstName', 'lastName', 'gender', 'level'])
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(
        os.path.join(output_data, "users.parquet"))

    # create timestamp column from original timestamp column
    convert_timestamp = udf(lambda ts_int: datetime.fromtimestamp(ts_int/1000), TimestampType())
    df = df.withColumn('timestamp', convert_timestamp(col("ts")))
    
    # create datetime column from original timestamp column
    df = df.withColumn('hour', hour(col("timestamp"))) \
        .withColumn('day', date_format(col("timestamp"), 'd')) \
        .withColumn('week', weekofyear(col("timestamp"))) \
        .withColumn('month', month(col("timestamp"))) \
        .withColumn('year', year(col("timestamp"))) \
        .withColumn('weekday', dayofweek(col("timestamp")))
    
    # extract columns to create time table
    time_table = df.select('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy(
        'year', 'month').parquet(os.path.join(output_data, "time.parquet"))

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song-data/*/*/*/*.json')
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(
        song_df,
        [df.song==song_df.title, df.artist==song_df.artist_name],
        "left"
    )

    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id())

    songplays_table = songplays_table.select([
    'songplay_id', 'timestamp', 'userId', 'level', 'song_id', 'artist_id',
    'sessionId', 'location', 'userAgent',
    df['year'], df['month']]) \
        .withColumnRenamed('timestamp', 'start_time') \
        .withColumnRenamed('userId', 'user_id') \
        .withColumnRenamed('sessionId', 'session_id') \
        .withColumnRenamed('userAgent', 'user_agent')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(os.path.join(output_data, "songplays.parquet"))


def main():
    print("Starting Script !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    spark = create_spark_session()
    print("Spark Session Created !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    input_data ="s3a://udacity-dend/" #s3
    output_data = "s3a://output-udacity/" #s3
#     input_data = "data" # localhost
#     output_data = "output" # locahost
    
    process_song_data(spark, input_data, output_data)  
    print("Song Data Processed !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    process_log_data(spark, input_data, output_data)
    print("Spark Processing Finished !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")


if __name__ == "__main__":
    main()
