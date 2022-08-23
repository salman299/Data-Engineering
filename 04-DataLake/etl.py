import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Configure a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process a song_data. 
    Reads a song_data using a spark session and create songs and artist tables from it.
    Parameters:
        spark (pyspark.sql.session.SparkSession): SparkSession
        input_data (str): path of a input data
        output_data (str): path of a output data
    """
    # get filepath to song data file
    song_data =  input_data+'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.selectExpr('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    songs_table.createOrReplaceTempView('songs')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, 'songs'), 'overwrite', partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    columns = ['artist_name as artist', 'artist_location as location', 'artist_latitude as latitude', 'artist_longitude as longitude']
    artists_table = df.selectExpr('artist_id', *columns).dropDuplicates()
    artists_table.createOrReplaceTempView('artists')
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Process a log_data. 
    Reads a log_data using a spark session and create users, time, and songplays tables from it.
    Parameters:
        spark (pyspark.sql.session.SparkSession): SparkSession
        input_data (str): path of a input data
        output_data (str): path of a output data
    """
    # get filepath to log data file
    log_data = input_data+'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    df.withColumn('user_id', df.userId.cast(IntegerType()))
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    columns = ['userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level']
    users_table = df.selectExpr(*columns).dropDuplicates()
    
    users_table.createOrReplaceTempView('users')
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts/1000).isoformat())
    df = df.withColumn('start_time', get_timestamp('ts').cast(TimestampType()))
    
    # extract columns to create time table
    time_table = df.select('start_time')
    time_table = time_table.withColumn('hour', hour('start_time'))
    time_table = time_table.withColumn('day', dayofmonth('start_time'))
    time_table = time_table.withColumn('week', weekofyear('start_time'))
    time_table = time_table.withColumn('month', month('start_time'))
    time_table = time_table.withColumn('year', year('start_time'))
    time_table = time_table.withColumn('weekday', dayofweek('start_time'))
    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, 'time'), 'overwrite', partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song_data/*/*/*/*.json'))

    # extract columns from joined song and log datasets to create songplays table
    df = df.join(song_df, song_df.title == df.song)
    columns = ['ts', 'userId as user_id', 'level', 'song_id', 'artist_id', 'sessionId as session_id', 
               'location', 'userAgent as user_agent', 'year', "month('start_time') as month"]
    songplays_table = df.selectExpr(*columns)
    

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'), 'overwrite', partitionBy=['year', 'month'])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
