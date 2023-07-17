import os
import configparser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType as StructType, DoubleType as DoubleType, StructField as StructField
from pyspark.sql.types import StringType as StringType, IntegerType as IntegerType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a new Spark session with the given configuration or
    updates the config of exisiting session with the same
    
    :return spark: Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: Loads song_data from S3,extracts the songs and artist tables
    and loads back to S3
        
    :spark       : Spark Session object
    :input_data  : Location of song_data (songs metadata) json files 
    :output_data : Location where dimensional tables are stored in parquet format
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    songSchema = StructType([
        StructField("song_id",StringType()),
        StructField("artist_id",StringType()),
        StructField("artist_latitude",DoubleType()),
        StructField("artist_location",StringType()),
        StructField("artist_longitude",DoubleType()),
        StructField("artist_name",StringType()),
        StructField("duration",DoubleType()),
        StructField("num_songs",IntegerType()),
        StructField("title",StringType()),
        StructField("year",IntegerType()),
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema=songSchema)
    df.createOrReplaceTempView("song_data_view")
    # extract columns to create songs table 
    song_fields = ["title", "artist_id","year", "duration"]
    songs_table = df.select(song_fields).dropDuplicates() \
                    .withColumn("song_id", monotonically_increasing_id())
    # Write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy('year', 'artist_id')\
    .parquet(output_data + 'songs/')

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude") \
                        .dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists/')


def process_log_data(spark, input_data, output_data):
    """
    Loads the log_data from S3 (input_data), extracts the songs and artist tables
    and loads the processed data back to S3
    
    :param spark: Spark Session object
    :param input_data: Location of songs metadata json files
    :param output_data: Location where dimension tables will be stored in parquet format             
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    df.createOrReplaceTempView("log_data_view")

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users/')
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x) / 1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))
    
    # extract columns to create time table 
    df = df.withColumn("hour", hour("start_time")) 
    df = df.withColumn("day", dayofmonth("start_time")) 
    df = df.withColumn("week", weekofyear("start_time"))
    df = df.withColumn("month", month("start_time"))
    df = df.withColumn("year", year("start_time"))
    df = df.withColumn("weekday", date_format(col("start_time"), 'E'))
    
    time_table = df.select("start_time", "hour", "day", "week", "month",\
                           "year", "weekday").drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month")\
    .parquet(output_data+'time/')
    

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                to_timestamp(logTable.ts/1000) as start_time,
                                logTable.userId as user_id,
                                logTable.level as level,
                                songTable.song_id as song_id,
                                songTable.artist_id as artist_id,
                                logTable.sessionId as session_id,
                                logTable.location as location,
                                logTable.userAgent as user_agent,
                                month(to_timestamp(logTable.ts/1000)) as month,
                                year(to_timestamp(logTable.ts/1000)) as year
                                FROM log_data_view logTable
                                JOIN song_data_view songTable on logTable.artist = songTable.artist_name and 
                                logTable.song = songTable.title
                             """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-mybucket/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
