import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import hour, dayofmonth, weekofyear, month, year, dayofweek, date_format
from pyspark.sql.functions import expr
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Setup and launch a spark session."""
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Extract song data from S3;
    create songs and artists dataframes based on song data;
    write them to S3 as partitioned parquet files.
    """
    
    # get filepath to song data file
    # smaller file for test: song_data = os.path.join(input_data, "song_data/A/A/A/*.json")
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)
    
    # change year and num_songs columns datatype to integar
    df = df.withColumn("num_songs", expr("cast(num_songs as int)"))
    df = df.withColumn("year", expr("cast(year as int)"))
    
    # extract columns to create songs table
    songs_table = df.select("song_id", \
                            "title", \
                            "artist_id", \
                            "year", \
                            "duration") \
                    .dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table \
    .write \
    .partitionBy("year", "artist_id") \
    .parquet(os.path.join(output_data, "songs"), "overwrite")

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id as artist_id", \
                                  "artist_name as name", \
                                  "artist_location as location", \
                                  "artist_latitude as latitude", \
                                  "artist_longitude as longitude") \
                      .dropDuplicates()
    
    # write artists table to parquet files
    artists_table \
    .write \
    .parquet(os.path.join(output_data, "artists"), "overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Extract log and song data from S3;
    create users, time and songplays dataframes based on log and song data;
    write them to S3 as partitioned parquet files.
    """
    
    # get filepath to log data file
    # smaller file for test: log_data = os.path.join(input_data, "log_data/2018/11/2018-11-3*.json")
    log_data = os.path.join(input_data, "log_data/*/*/*.json")
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == "NextSong")

    # change itemInSession, sessionId and status columns datatype to integar
    df = df.withColumn("itemInSession", expr("cast(itemInSession as int)"))
    df = df.withColumn("sessionId", expr("cast(sessionId as int)"))
    df = df.withColumn("status", expr("cast(status as int)"))
    
    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", \
                                "firstName as first_name", \
                                "lastName as last_name", \
                                "gender as gender", \
                                "level as level") \
                    .dropDuplicates()
    
    # write users table to parquet files
    users_table \
    .write \
    .parquet(os.path.join(output_data, "users"), "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0).strftime('%Y-%m-%d %H:%M:%S.%f'))
    df = df.withColumn("new_ts", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("start_time", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.selectExpr("start_time as start_time", \
                               "hour(start_time) as hour", \
                               "dayofmonth(start_time) as day", \
                               "weekofyear(start_time) as week", \
                               "month(start_time) as month", \
                               "year(start_time) as year", \
                               "dayofweek(start_time) as weekday") \
                   .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table \
    .write \
    .partitionBy("year", "month") \
    .parquet(os.path.join(output_data, "time"), "overwrite")

    # read in song data to use for songplays table
    # smaller file for test: song_data = os.path.join(input_data, "song_data/A/A/A/*.json")
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    df_song = spark.read.json(song_data).dropDuplicates()
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df \
                .join(df_song, [df.artist == df_song.artist_name, df.song == df_song.title], 'left') \
                .select(df.start_time, \
                        df.userId, \
                        df.level, \
                        df_song.song_id, \
                        df_song.artist_id, \
                        df.sessionId, \
                        df.location, \
                        df.userAgent) \
                .dropDuplicates()

    # add auto-incremental column and rename column names in songplays_table
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())
    songplays_table = songplays_table.selectExpr("songplay_id as songplay_id", \
                                                 "start_time as start_time", \
                                                 "userId as user_id", \
                                                 "level as level", \
                                                 "song_id as song_id", \
                                                 "artist_id as artist_id", \
                                                 "sessionId as session_id", \
                                                 "location as location", \
                                                 "userAgent as user_agent", \
                                                 "year(start_time) as year", \
                                                 "month(start_time) as month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table \
    .write \
    .partitionBy("year", "month") \
    .parquet(os.path.join(output_data, "songplays"), "overwrite")


def main():
    """
    Specify input and output data S3 path.
    Execute create_spark_session, process_song_data and process_log_data functions.
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = config['AWS']['AWS_S3']
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
