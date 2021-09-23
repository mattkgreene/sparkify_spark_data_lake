import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import dayofweek
from pyspark.sql.functions import monotonically_increasing_id


def create_spark_session():
    """
    Function to instantiate a spark_session
    
    Parameters:
        None
    Outputs:
        None
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.4") \
        .getOrCreate()
    spark._jsc.hadoopConfiguration().set(
        "spark.sql.parquet.output.committ‌​er.class",
        "org.apache.spark.sql.parquet.DirectParquetOutputCommitter"
    )

    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function is about processing the song log data
    It initially reads all the song data from the Udacity bucket
    Next the function selects the columns from the new song_df
    and creates a song_dim_df, which is then written to my S3 Bucket
    Finally, an artist_dim is also created out of the song_df and
    written to my S3 bucket.
    
    Parameters:
        - SparkSession
        - input data path
        - output data path
    Outputs:
        None
    """
    # get filepath to song data file
    song_data = "s3a://udacity-dend/song_data/*/*/*/*.json"
    
    # read song data file
    print("Starting song_df read from Udacity S3")
    song_df = spark.read.json(song_data).dropDuplicates().cache()
    print("Finished song_df read from Udacity S3")
    

    # extract columns to create songs table
    songs_cols = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_dim_df = song_df.select(songs_cols).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    # Buffer in memory instead of disk, faster but more memory intensive
    print("Song Dim df starting s3 write")
    
    songs_dim_df.write.option("header",True)\
    .mode("overwrite")\
    .partitionBy("year","artist_id")\
    .option('fs.s3a.committer.name', 'partitioned') \
    .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
    .option("fs.s3a.fast.upload.buffer", "bytebuffer") \
    .parquet('s3a://mkgpublic/udacity/songs_dim/songs_dim.parquet')
    
    print("Song Dim df finished write")
    # extract columns to create artists table
    
    print("starting artists dim select")
    artists_dim_df = song_df.selectExpr("artist_id", 
                                        "artist_name as name",
                                        "artist_location as location",
                                        "artist_latitude as latitude",
                                        "artist_longitude as longitude"
                                       )
    print("finished artists dim select")
    # write artists table to parquet files
    print("Artists Dim df starting s3 write")
    artists_dim_df.write.option("header",True) \
    .mode("overwrite")\
    .option('fs.s3a.committer.name', 'partitioned') \
    .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
    .option("fs.s3a.fast.upload.buffer", "bytebuffer") \
    .parquet('s3a://mkgpublic/udacity/artists_dim/artists_dim.parquet')
    
    print("Artists Dim df finished write")


def process_log_data(spark, input_data, output_data):
    """
    This function is about processing the user log data
    It initially reads all the user log data from the Udacity bucket.
    Next the function selects the columns from the new log_df
    and creates a users_df, which is then written to my S3 Bucket
    Next the function processes date_dim from the timestamp column in
    the log_df and parses out multiple date columns.
    Finally, the songs_dim_df is read from the S3 bucket 
    and joined together with
    date dim + log_df to create the songplays fact table.
    Then the songplays fact table along with date_dim 
    are written to a s3 bucket.
    written to my S3 bucket.
    
    Parameters:
        - SparkSession
        - input data path
        - output data path
    Outputs:
        None
    """
    # get filepath to log data file
    
    log_data = "s3a://udacity-dend/log_data/*/*/*.json"

    # read log data file
    print("Starting log_df read from Udacity S3")
    log_df = spark.read.json(log_data)
    print("Finished log_df read from Udacity S3")
    # filter by actions for song plays
    song_plays_log = log_df.filter(log_df.page=='NextSong')

    # extract columns for users table    
    users_df = song_plays_log.selectExpr("CAST(userId as int) as user_id",
                                         "firstName as first_name", 
                                         "lastName as last_name", "gender",
                                         "level").distinct()
    
    # write users table to parquet files
    # Buffer in memory instead of disk, faster but more memory intensive
    print("Starting users_df write to S3")
    users_df.write.option("header",True) \
    .mode("overwrite")\
    .option('fs.s3a.committer.name', 'partitioned') \
    .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
    .option("fs.s3a.fast.upload.buffer", "bytebuffer") \
    .parquet('s3a://mkgpublic/udacity/users_dim/users_dim.parquet')
    
    print("Finished users_df write to S3")

    # create timestamp column from original timestamp column
    print("Starting date_dim parse")
    time_log_df = song_plays_log.withColumn(
        "ts",from_unixtime(song_plays_log.ts/1000)
    )
    # create datetime column from original timestamp column
    date_dim_df = time_log_df.selectExpr("ts as start_time")
    
    # extract columns to create time table
    date_dim_df = date_dim_df\
    .withColumn("hour",hour(date_dim_df.start_time))\
    .withColumn("day",dayofmonth(date_dim_df.start_time))\
    .withColumn("week",weekofyear(date_dim_df.start_time))\
    .withColumn("month", month(date_dim_df.start_time))\
    .withColumn("year", year(date_dim_df.start_time))\
    .withColumn("weekday", dayofweek(date_dim_df.start_time)
               )
    
    print("Finished date_dim parse")
    
    # write time table to parquet files partitioned by year and month
    # Buffer in memory instead of disk, faster but more memory intensive
    print("Starting date_dim write to S3")
    date_dim_df.write.option("header",True) \
    .mode("overwrite")\
    .partitionBy("year","month")\
    .option('fs.s3a.committer.name', 'partitioned') \
    .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
    .option("fs.s3a.fast.upload.buffer", "bytebuffer") \
    .parquet('s3a://mkgpublic/udacity/date_dim/date_dim.parquet')
    
    print("Finished date_dim write to S3")
    # read in song data to use for songplays table
    print("Starting songs_dim read from S3")
    songs_dim_parquet = 's3a://mkgpublic/udacity/songs_dim/'
    songs_dim_df = spark.read.parquet(songs_dim_parquet)
    print("Finished songs_dim read from S3")

    # extract columns from joined song and log datasets to create songplays table
    date_dim_df = date_dim_df.alias("date_dim_df")
    log_song_join = [time_log_df.song == songs_dim_df.title]
    date_timelog_join = [date_dim_df.start_time == time_log_df.ts]
    
    print("Starting songplays fact table join")
    songplays_table = time_log_df.join(songs_dim_df, 
                                       on=log_song_join, how='inner')\
    .join(date_dim_df,on=date_timelog_join, how='inner')\
    .withColumn('songplay_id', monotonically_increasing_id())\
    .selectExpr("songplay_id", "ts as start_time", 
                "CAST(userId as int) as user_id", 
                "level","song_id", "artist_id",
                "sessionId as session_id","location", 
                "userAgent as user_agent",
                "date_dim_df.year", "date_dim_df.month"
           )
    
    print("Finished songplays fact table join")

    # write songplays table to parquet files partitioned by year and month
    # Buffer in memory instead of disk, faster but more memory intensive
    print("Starting songplays write to s3")
    songplays_table.write.option("header",True) \
    .mode("overwrite")\
    .partitionBy("year","month")\
    .option('fs.s3a.committer.name', 'partitioned') \
    .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
    .option("fs.s3a.fast.upload.buffer", "bytebuffer") \
    .parquet('s3a://mkgpublic/udacity/songplays/songplays.parquet')
    print("Finished songplays write to s3")


def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
    
    S3_BUCKET=config['S3']['S3_BUCKET']
    S3_KEY=config['S3']['S3_NON_EMR_KEY']
    
    S3_URI = 's3://{bucket}/{key}'.format(bucket=S3_BUCKET, key=S3_KEY)
    
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = S3_URI
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    print("All Done!")


if __name__ == "__main__":
    main()
