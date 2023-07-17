import os
import configparser
from datetime import datetime
import logging
import pandas as pd
from pyspark.sql import SparkSession, HiveContext, Row
from pyspark.sql.functions import udf, col, lit, substring, split, round, when, expr, concat, count, isnan
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, upper, to_date
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType as StructType, DoubleType as DoubleType, StructField as StructField
from pyspark.sql.types import StringType as StringType, IntegerType as IntegerType, TimestampType, DateType
from pathlib import Path
from os import listdir
from os.path import isfile, join, isdir
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
    spark = SparkSession.builder.\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0").\
    enableHiveSupport().getOrCreate()
        
    return spark

def convert_to_datetime(date):
    """
    Convert to yyyy-mm-dd format
    
    :return: date in yyyy-mm-dd format
    """   
    if date is not None:
        return pd.Timestamp('1960-1-1')+pd.to_timedelta(date, unit='D')

convert_to_datetime_udf = udf(convert_to_datetime, DateType())

def convert_to_datetime_from_format(format_str):
    """
    Convert string to yyyy-mm-dd format
    
    :return: date in yyyy-mm-dd format
    """
    def func(date):
        """
        Convert string to yyyy-mm-dd format 
        
        :return: date in yyyy-mm-dd format
        """
        if date is not None:
            date=pd.to_datetime(date,format=format_str,errors='coerce')
            return date      
    return udf(func,DateType())


def update_column_name(original_table, new_column_names):
    """
    Update column names
    
    :return: original table with updated column names
    """
    for original_column_name, new_column_name in zip(original_table.columns, new_column_names):
        original_table = original_table.withColumnRenamed(original_column_name, new_column_name)
    return original_table

def customUnion(df1, df2):
    """
    Append data frames
    
    :return: combined single data frame
    """
    cols1 = df1.columns
    cols2 = df2.columns
    total_cols = sorted(cols1 + list(set(cols2) - set(cols1)))
    def expr(mycols, allcols):
        def processCols(colname):
            if colname in mycols:
                return colname
            else:
                return lit(None).alias(colname)
        cols = map(processCols, allcols)
        return list(cols)
    appended = df1.select(expr(cols1, total_cols)).union(df2.select(expr(cols2, total_cols)))
    return appended

def process_immigration_data(spark, output_data):
    """
    Process immigration data to get processed tables
    
    Arguments:
            spark {object}: SparkSession object
            output_data {object}: Target S3 endpoint
            
    :return: None
    """
    
    global fact_immigration
    global dim_immigration_entdepa
    global dim_immigration_entdepd
    global dim_immigration_visa
    global dim_immigration_airline
    global dim_immigration_personal
    
    logging.info("Start processing df_immigration")
    
    # read immigration data file
    
    i=0
    for f in os.listdir('../../data/18-83510-I94-Data-2016/'):
        immigration_data = os.path.join('../../data/18-83510-I94-Data-2016/' + f)
        dfimmigration = spark.read.format('com.github.saurfang.sas.spark').load(immigration_data)
        if i==0:
            df_immigration = dfimmigration
        else:
            df_immigration = customUnion(df_immigration, dfimmigration)
        i+=1
    
    df_immigration = df_immigration.withColumn('country', lit('United States'))
    
    logging.info("Start processing fact_immigration")
    # extract columns to create fact_immigration table
    fact_immigration = df_immigration.select('cicid', 'i94yr', 'i94mon', 'i94port', 'i94addr',\
                                             'arrdate', 'depdate', 'i94mode', 'country', 'matflag',\
                                             'entdepa','entdepd','dtaddto','dtadfile','occup',\
                                             'count').distinct()\
    .withColumn("immigration_id", monotonically_increasing_id())
    
    new_columns = ['cic_id', 'year', 'month', 'city_code', 'state_code', 'arrival_date',\
                   'departure_date','mode','country','matflag','entdepa','entdepd',\
                   'dtaddto','dtadfile','occup','count','immigration_id']
                  
    fact_immigration = update_column_name(fact_immigration, new_columns)
    
    fact_immigration = fact_immigration.withColumn('arrival_date', \
                                                   convert_to_datetime_udf(col('arrival_date')))
    fact_immigration = fact_immigration.withColumn('departure_date', \
                                                   convert_to_datetime_udf(col('departure_date')))
    fact_immigration = fact_immigration.withColumn('dtadfile', \
                                                   convert_to_datetime_from_format('%Y%m%d')(col('dtadfile')))
    fact_immigration = fact_immigration.withColumn('dtaddto', regexp_replace('dtaddto', 'D/S', '01011900'))
    fact_immigration = fact_immigration.withColumn('dtaddto', \
                                                   convert_to_datetime_from_format('%m%d%Y')(col('dtaddto')))
    
    maps_entdepd = {'O':'1','R':'2','K':'3','N':'4','D':'5','I':'6','Q':'7','J':'8','W':'9',\
                    'L':'10','M':'11','V':'12','T':'13','Y':'14','Z':'15','G':'16','null':'17'}
    fact_immigration = fact_immigration.replace(to_replace=maps_entdepd, subset=['entdepd'])
    fact_immigration=fact_immigration.na.fill({'entdepd': '17'})
    fact_immigration.withColumn('entdepd',col('entdepd').cast('int'))
    
    maps_entdepa ={'G':'1','Z':'2','T':'3','O':'4','P':'5','A':'6','K':'7','U':'8','H':'9',\
                   'F':'10','B':'11','M':'12','N':'13','Q':'14','J':'15','R':'16','I':'17',\
                   'null':'18'}
    fact_immigration = fact_immigration.replace(to_replace=maps_entdepa, subset=['entdepa'])
    fact_immigration=fact_immigration.na.fill({'entdepa': '18'})
    fact_immigration.withColumn('entdepa',col('entdepa').cast('int'))
    
    new_columns = ['cic_id', 'year', 'month', 'city_code', 'state_code', 'arrival_date',\
                   'departure_date','mode','country','matflag','entdepa_code','entdepd_code',\
                   'dtaddto','dtadfile','occup','count','immigration_id']
    fact_immigration = update_column_name(fact_immigration, new_columns)
    
    # write fact_immigration table to parquet files partitioned by state_code
    
    fact_immigration = fact_immigration.filter("cic_id is not NULL AND year is not NULL and month is not NULL AND city_code is not NULL and state_code is not NULL and arrival_date is not NULL and departure_date is not NULL and mode is not NULL and country is not NULL and matflag is not NULL and entdepa_code is not NULL and entdepd_code is not NULL and dtaddto is not NULL and dtadfile is not NULL and occup is not NULL and count is not NULL and immigration_id is not NULL")
    
    fact_immigration.write.mode("overwrite").partitionBy('state_code')\
    .parquet(path=output_data+'immigration_data/fact_immigration') 

    #creating dim_immigration_entdepa and writing it to parquet files
    logging.info("Start processing dim_immigration_entdepa")    
        
    data = [{'code' : 1, 'entdepa' : 'G'}, {'code' : 2, 'entdepa' : 'Z'},\
            {'code' : 3, 'entdepa' : 'T'}, {'code' : 4, 'entdepa' : 'O'},\
            {'code' : 5, 'entdepa' : 'P'}, {'code' : 6, 'entdepa' : 'A'},\
            {'code' : 7, 'entdepa' : 'K'}, {'code' : 8, 'entdepa' : 'U'},\
            {'code' : 9, 'entdepa' : 'H'}, {'code' : 10, 'entdepa' : 'F'},\
            {'code' : 11, 'entdepa' : 'B'}, {'code' : 12, 'entdepa' : 'M'},\
            {'code' : 13, 'entdepa' : 'N'}, {'code' : 14, 'entdepa' : 'Q'},\
            {'code' : 15, 'entdepa' : 'J'}, {'code' : 16, 'entdepa' : 'R'},\
            {'code' : 17, 'entdepa' : 'I'}, {'code' : 18, 'entdepa' : 'null'}]
    
    dim_immigration_entdepa = spark.createDataFrame(data)
    
    dim_immigration_entdepa.write.mode("overwrite")\
    .parquet(output_data + 'immigration_data/dim_immigration_entdepa/') 
    
    #creating dim_immigration_entdepd and writing it to parquet files
    logging.info("Start processing dim_immigration_entdepd") 
    
    data = [{'code' : 1, 'entdepd' : 'O'}, {'code' : 2, 'entdepd' : 'R'},\
            {'code' : 3, 'entdepd' : 'K'}, {'code' : 4, 'entdepd' : 'N'},\
            {'code' : 5, 'entdepd' : 'D'}, {'code' : 6, 'entdepd' : 'I'},\
            {'code' : 7, 'entdepd' : 'Q'}, {'code' : 8, 'entdepd' : 'J'},\
            {'code' : 9, 'entdepd' : 'W'}, {'code' : 10, 'entdepd' : 'L'},\
            {'code' : 11, 'entdepd' : 'M'}, {'code' : 12, 'entdepd' : 'V'},\
            {'code' : 13, 'entdepd' : 'T'}, {'code' : 14, 'entdepd' : 'Y'},\
            {'code' : 15, 'entdepd' : 'Z'},{'code' : 16, 'entdepd' : 'G'},\
            {'code' : 17, 'entdepd' : 'null'}]

    dim_immigration_entdepd = spark.createDataFrame(data)
    
    dim_immigration_entdepd.write.mode("overwrite")\
    .parquet(path=output_data+'immigration_data/dim_immigration_entdepd')
    
    #creating dim_immigration_visa and writing it to parquet files
    logging.info("Start processing dim_immigration_visa") 
    
    data = [{'visa_type' : 'B1', 'visa' : 1.0}, {'visa_type' : 'WT', 'visa' : 2.0},\
            {'visa_type' : 'E2', 'visa' : 1.0}, {'visa_type' : 'I1', 'visa' : 1.0},\
            {'visa_type' : 'M2', 'visa' : 3.0}, {'visa_type' : 'CPL', 'visa' : 2.0},\
            {'visa_type' : 'E1', 'visa' : 1.0}, {'visa_type' : 'M1', 'visa' : 3.0},\
            {'visa_type' : 'F1', 'visa' : 3.0}, {'visa_type' : 'GMB', 'visa' : 1.0},\
            {'visa_type' : 'GMT', 'visa' : 2.0}, {'visa_type' : 'CP', 'visa' : 2.0},\
            {'visa_type' : 'F2', 'visa' : 3.0}, {'visa_type' : 'WB', 'visa' : 1.0},\
            {'visa_type' : 'I', 'visa' : 1.0}, {'visa_type' : 'SBP', 'visa' : 2.0},\
            {'visa_type' : 'B2', 'visa' : 2.0}]
    
    dim_immigration_visa = spark.createDataFrame(data)
    dim_immigration_visa = dim_immigration_visa.select(sorted(dim_immigration_visa.columns,\
                                                              reverse=True))
    
    dim_immigration_visa.write.mode("overwrite")\
    .parquet(path=output_data + 'immigration_data/dim_immigration_visa')
    
    #creating dim_immigration_airline and writing it to parquet files
    logging.info("Start processing dim_immigration_airline") 
    
    dim_immigration_airline = df_immigration.select('cicid', 'airline', 'admnum',\
                                                    'fltno','visatype',\
                                                    'visapost').distinct()\
    .withColumn("immigration_airline_id", monotonically_increasing_id())
    new_columns = ['cic_id', 'airline', 'admin_num', 'flight_number', 'visa_type',\
                   'visapost', 'immigration_airline_id']
    dim_immigration_airline = update_column_name(dim_immigration_airline, new_columns)
    
    dim_immigration_airline.write.mode("overwrite")\
    .parquet(path=output_data + 'immigration_data/dim_immigration_airline')
    
    #creating dim_immigration_personal and writing it to parquet files
    logging.info("Start processing dim_immigration_personal") 
    
    dim_immigration_personal = df_immigration.select('cicid', 'i94cit', 'i94res',\
                                                     'biryear','gender',\
                                                     'insnum').distinct()\
    .withColumn("immi_personal_id", monotonically_increasing_id())
    new_columns = ['cic_id', 'country_residence','country_citizenship',\
                   'birth_year', 'gender', 'ins_num', 'immi_personal_id']
    dim_immigration_personal = update_column_name(dim_immigration_personal, new_columns)
    
    dim_immigration_personal.write.mode("overwrite")\
    .parquet(path=output_data + 'immigration_data/dim_immigration_personal')
    
def process_airport_data(spark, output_data):
    """
    Process airport data to processed tables
    
    Arguments:
            spark {object}: SparkSession object
            output_data {object}: Target S3 endpoint
            
    return: None
    """
    
    global dim_airport_type
    global dim_airport_city_state_code
    global dim_airport
    
    logging.info("Start processing df_airport")
    
    # read airport data file
    airport_data = 'airport-codes_csv.csv'
    df_airport  = spark.read.csv(airport_data, header=True)
    df_airport = df_airport.withColumn('iso_region', substring('iso_region', 4,5))
    
    logging.info("Start processing dim_airport_type")
    
    data = [{'code' : 1, 'airport_type' : 'closed'}, {'code' : 2, 'airport_type' : 'small_airport'},\
            {'code' : 3, 'airport_type' : 'medium_airport'}, {'code' : 4, 'airport_type' : 'large_airport'},\
            {'code' : 5, 'airport_type' : 'heliport'}, {'code' : 6, 'airport_type' : 'seaplane_base'},\
            {'code' : 7, 'airport_type' : 'balloonport'}]
    dim_airport_type = spark.createDataFrame(data)
    dim_airport_type = dim_airport_type.select(sorted(dim_airport_type.columns,reverse=True))
    dim_airport_type = dim_airport_type.withColumn('code',col('code').cast("int"))
    
    dim_airport_type.write.mode("overwrite").parquet(path=output_data + 'airport_data/dim_airport_type')
    
    logging.info("Start processing dim_airport_city_state_code")
    
    dim_airport_city_state_code = df_airport.select(['municipality','iso_region'])
    dim_airport_city_state_code = dim_airport_city_state_code.withColumn('country', lit('United States'))
    new_columns = ['city', 'state_code', 'country']
    dim_airport_city_state_code = update_column_name(dim_airport_city_state_code, new_columns)
    dim_airport_city_state_code = dim_airport_city_state_code.withColumn('city', upper(col('city')))
    
    dim_airport_city_state_code.write.mode("overwrite").parquet(path=output_data  +\
                                                                'airport_data/dim_airport_city_state_code')
    
    logging.info("Start processing dim_airport")
    
    dim_airport = df_airport.select(["ident","type","name","elevation_ft","municipality","iata_code","coordinates"])
    split_col = split(dim_airport['coordinates'], ',')
    dim_airport = dim_airport.withColumn('airport_longitude', split_col.getItem(0))
    dim_airport = dim_airport.withColumn('airport_latitude', split_col.getItem(1))
    dim_airport = dim_airport.drop("coordinates")
    dim_airport.withColumn('airport_longitude',col('airport_longitude').cast("float"))
    dim_airport.withColumn('airport_latitude',col('airport_latitude').cast("float"))
    dim_airport = dim_airport.withColumn('airport_longitude', round(dim_airport['airport_longitude'], 2))
    dim_airport = dim_airport.withColumn('airport_latitude', round(dim_airport['airport_latitude'], 2))
    code=[1,2,3,4,5,6,7]
    maps_airport_type = {'closed':'1','small_airport':'2','medium_airport':'3','large_airport':'4',\
                         'heliport':'5', 'seaplane_base':'6','balloonport':'7'}
    dim_airport = dim_airport.replace(to_replace=maps_airport_type, subset=['type'])
    new_columns = ['ident','airport_type_code','airport_name','elevation_ft','city',\
                   'iata_code','airport_longitude','airport_latitude']
    dim_airport = update_column_name(dim_airport, new_columns)
    dim_airport = dim_airport.withColumn('airport_type_code',col('airport_type_code').cast("int"))
    dim_airport = dim_airport.withColumn('city', upper(col('city')))
    
    dim_airport.write.mode("overwrite").parquet(path = output_data +'airport_data/dim_airport')
    
def process_temperature_data(spark, output_data):
    """
    Process temperature data to processed tables
    
    Arguments:
            spark {object}: SparkSession object
            output_data {object}: Target S3 endpoint
            
    return: None
    """
    global dim_temperature_city
    global dim_temperature_usa
    
    logging.info("Start processing df_temperature")
    
    # read temperature data file
    temperature_data = '../../data2/GlobalLandTemperaturesByCity.csv'
    df_temperature = spark.read.csv(temperature_data, header=True)
    df_temperature = df_temperature.where(df_temperature['Country'] == 'United States')
    
    logging.info("Start processing dim_temperature_city")
    dim_temperature_city = df_temperature.select(['City','Latitude','Longitude']).distinct()
    dim_temperature_city = dim_temperature_city.withColumn('Latitude',\
                                                           when(substring('Latitude', -1,1)=='N',\
                                                                expr("substring(Latitude, 1, length(Latitude)-1)")).otherwise\
                                                           (-expr("substring(Latitude, 1, length(Latitude)-1)")))

    dim_temperature_city = dim_temperature_city.withColumn('Longitude',\
                                                           when(substring('Longitude', -1,1)=='E',\
                                                                expr("substring(Longitude,1,length(Longitude)-1)")).otherwise\
                                                           (-expr("substring(Longitude, 1, length(Longitude)-1)")))

    new_columns=['city','latitude','longitude']
    dim_temperature_city = update_column_name(dim_temperature_city, new_columns)
    dim_temperature_city = dim_temperature_city.withColumn('city', upper(col('city')))
    
    dim_temperature_city.write.mode("overwrite").parquet(path = output_data +\
                                                         'temperature_data/dim_temperature_city')
    
    logging.info("Start processing dim_temperature_usa")
    dim_temperature_usa=df_temperature.select(['dt','AverageTemperature','AverageTemperatureUncertainty',\
                                               'City','Country']).distinct()
    dim_temperature_usa = dim_temperature_usa.withColumn('year', substring('dt',1,4))
    dim_temperature_usa = dim_temperature_usa.withColumn('month', substring(substring('dt',6,6),1,2))
    new_columns = ['date','avg_temp','avg_temp_var','city','country','year','month']
    dim_temperature_usa = dim_temperature_usa.withColumn('city', upper(col('city')))
    dim_temperature_usa = update_column_name(dim_temperature_usa, new_columns)
    dim_temperature_usa = dim_temperature_usa.withColumn('year',col('year').cast("int"))
    dim_temperature_usa = dim_temperature_usa.withColumn('month',col('month').cast("int"))
    
    dim_temperature_usa.write.mode("overwrite").parquet(path=output_data +\
                                                        'temperature_data/dim_temperature_usa')
    
def process_demographics_data(spark, output_data):
    """
    Process demographics data to processed tables
    
    Arguments:
            spark {object}: SparkSession object
            output_data {object}: Target S3 endpoint
            
    return: None
    """
    global dim_demographics_race
    global dim_demographics_city_race
    global dim_demographics_city_population
    
    logging.info("Start processing df_demographics")
    
    # read demographics data file
    demographics_data = 'us-cities-demographics.csv'
    df_demographics = spark.read.format('csv').options(header=True, delimiter=';').load(demographics_data)
    
    logging.info("Start processing dim_demographics_race")
    
    data = [{'race_code' : 1, 'race_type' : 'Hispanic or Latino'}, {'race_code' : 2, 'race_type' : 'White'},\
            {'race_code' : 3, 'race_type' : 'Asian'}, {'race_code' : 4, 'race_type' : 'Black or African-American'},\
            {'race_code' : 5, 'race_type' : 'American Indian and Alaska Native'}]

    dim_demographics_race = spark.createDataFrame(data)
    
    dim_demographics_race.write.mode("overwrite").parquet(path = output_data +\
                                                          'demographics_data/dim_demographics_race')
    
    logging.info("Start processing dim_demographics_city_race")
    
    maps_race_type = {'Hispanic or Latino':'1','White':'2','Asian':'3','Black or African-American':'4',\
                      'American Indian and Alaska Native':'5'}
    dim_demographics_city_race = df_demographics.select(['City','State Code','Race'])
    new_columns = ['city','state','race']
    dim_demographics_city_race = update_column_name(dim_demographics_city_race, new_columns)
    dim_demographics_city_race = dim_demographics_city_race.select(concat(dim_demographics_city_race.city,\
                                                                          dim_demographics_city_race.state)\
                                                                   .alias('idx'),'race')
    dim_demographics_city_race = dim_demographics_city_race.replace(to_replace=maps_race_type, subset=['race'])
    new_columns = ['idx','race_code']
    dim_demographics_city_race = update_column_name(dim_demographics_city_race, new_columns)
    dim_demographics_city_race = dim_demographics_city_race\
    .withColumn('idx', upper(col('idx')))
    
    dim_demographics_city_race.write.mode("overwrite")\
    .parquet(path=output_data + 'demographics_data/dim_demographics_city_race')

    logging.info("Start processing dim_demographics_city_population")
    
    dim_demographics_city_population = df_demographics.select(['City','State Code','Male Population',\
                                                               'Female Population','Number of Veterans',\
                                                               'Foreign-born','Median Age',\
                                                               'Average Household Size']).distinct()
    new_columns = ['city','state_code','male_pop','female_pop','num_vetarans','foreign_born',\
                   'median_age','avg_household_size']
    dim_demographics_city_population = update_column_name(dim_demographics_city_population, new_columns)
    dim_demographics_city_population = dim_demographics_city_population.\
    select(concat(dim_demographics_city_population.city,\
                  dim_demographics_city_population.state_code)\
           .alias('idx'),'city','state_code','male_pop','female_pop',\
           'num_vetarans','foreign_born','median_age',\
           'avg_household_size')
    dim_demographics_city_population = dim_demographics_city_population\
    .withColumn('city', upper(col('city')))
    dim_demographics_city_population = dim_demographics_city_population\
    .withColumn('idx', upper(col('idx')))
    
    dim_demographics_city_population.write.mode("overwrite").parquet\
    (path = output_data + 'demographics_data/dim_demographics_city_population')

def process_label_descriptions(spark, output_data):
    """ 
    Parsing label desctiption file to get codes of country, city, state
        
    Arguments:
    spark {object}: SparkSession object
    output_data {object}: Target S3 endpoint
        
    return: None
    """
    
    global country_code
    global state_code
    global city_code
    
    logging.info("Start processing label descriptions")
    label_file = "I94_SAS_Labels_Descriptions.SAS"
    with open(label_file) as f:
        contents = f.readlines()
    
    logging.info("Start processing country_code")
    country_code = {}
    for countries in contents[10:298]:
        pair = countries.split('=')
        code, country = pair[0].strip(), pair[1].strip().strip("'")
        country_code[code] = country
 
    country_code = spark.createDataFrame(country_code.items(), ['code', 'country'])
    
    country_code.write.mode("overwrite").parquet(path = output_data + 'label_data/country_code')

    logging.info("Start processing city_code")
    city_code = {}
    for cities in contents[303:962]:
        pair = cities.split('=')
        code, city = pair[0].strip("\t").strip().strip("'"),\
        pair[1].strip('\t').strip().strip("''")
        city_code[code] = city

    city_code = spark.createDataFrame(city_code.items(), ['abbr_code', 'city'])
    city_code = city_code.withColumn("city", split(col("city"), ",").getItem(0))
    
    city_code.write.mode("overwrite").parquet(path = output_data + 'label_data/city_code')

    logging.info("Start processing state_code")
    state_code = {}
    for states in contents[982:1036]:
        pair = states.split('=')
        code, state = pair[0].strip('\t').strip("'"), pair[1].strip().strip("'")
        state_code[code] = state
    
    state_code = spark.createDataFrame(state_code.items(), ['abbr_code', 'state']) 
    
    state_code.write.mode("overwrite").parquet(path = output_data + 'label_data/state_code')
    
def process_data_check(spark, output_data, bucket_name):
    """
    To check if tables were created correctly
    
    Arguments:
            spark {object}: SparkSession object
            output_data {object}: Target S3 endpoint
            
    :return: None
    """

    session = boto3.Session(
        aws_access_key_id=config['KEYS']['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['KEYS']['AWS_SECRET_ACCESS_KEY'])

    #Then use the session to get the resource
    s3 = session.resource('s3')

    my_bucket = s3.Bucket(bucket_name)   
    
    logging.info("process_data_check in progress")
    
    # checking if fact_immigration is ok
  
    noZeroCount = True
    for my_bucket_object in my_bucket.objects.filter(Prefix="immigration_data/fact_immigration/"):
        if my_bucket_object.key.endswith('parquet'):
            df = spark.read.parquet(output_data + my_bucket_object.key)
            if df.count()<1:
                print('0 records ....problem in fact_immigration')
                noZeroCount = False
                break
    if noZeroCount:
        print('fact_immigration is Ok')
   
    # checking if dim_immigration_visa is ok
    
    df = spark.read.parquet(output_data + "immigration_data/dim_immigration_visa/")
    if df.count()<1:
        print('0 records ....problem in dim_immigration_visa')
    else:
        print('dim_immigration_visa is Ok')
        
    # checking if dim_immigration_personal is ok
    
    noZeroCount = True
    for my_bucket_object in my_bucket.objects.filter(Prefix="immigration_data/dim_immigration_personal/"):
        if my_bucket_object.key.endswith('parquet'):
            df = spark.read.parquet(output_data + my_bucket_object.key)
            if df.count()<1:
                print('0 records ....problem in dim_immigration_personal')
                noZeroCount = False
                break
    if noZeroCount:
        print('dim_immigration_personal is Ok')

    # checking if dim_immigration_airline is ok
    
    noZeroCount = True
    for my_bucket_object in my_bucket.objects.filter(Prefix="immigration_data/dim_immigration_airline/"):
        if my_bucket_object.key.endswith('parquet'):
            df = spark.read.parquet(output_data + my_bucket_object.key)
            if df.count()<1:
                print('0 records ....problem in dim_immigration_airline')
                noZeroCount = False
                break
    if noZeroCount:
        print('dim_immigration_airline is Ok')
        
    # checking if dim_immigration_entdepa is ok
    
    df = spark.read.parquet(output_data + "immigration_data/dim_immigration_entdepa/")
    if df.count()<1:
        print('0 records ....problem in dim_immigration_entdepa')
    else:
        print('dim_immigration_entdepa is Ok')
        
    # checking if dim_immigration_entdepd is ok
    
    df = spark.read.parquet(output_data + "immigration_data/dim_immigration_entdepd/")
    if df.count()<1:
        print('0 records ....problem in dim_immigration_entdepd')
    else:
        print('dim_immigration_entdepd is Ok')    
    
    # checking if dim_airport_city_state_code is ok
    
    df = spark.read.parquet(output_data + "airport_data/dim_airport_city_state_code/")
    if df.count()<1:
        print('0 records ....problem in dim_airport_city_state_code')
    else:
        print('dim_airport_city_state_code is Ok')
        
    # checking if dim_airport_type is ok
    
    df = spark.read.parquet(output_data + "airport_data/dim_airport_type/")
    if df.count()<1:
        print('0 records ....problem in dim_airport_type')
    else:
        print('dim_airport_type is Ok')
        
    # checking if dim_airport is ok
    
    df = spark.read.parquet(output_data + "airport_data/dim_airport/")
    if df.count()<1:
        print('0 records ....problem in dim_airport')
    else:
        print('dim_airport is Ok')    
        
    # checking if dim_demographics_race is ok
    
    df = spark.read.parquet(output_data + "demographics_data/dim_demographics_race/")
    if df.count()<1:
        print('0 records ....problem in dim_demographics_race')
    else:
        print('dim_demographics_race is Ok')
        
   # checking if dim_demographics_city_race is ok
    
    df = spark.read.parquet(output_data + "demographics_data/dim_demographics_city_race/")
    if df.count()<1:
        print('0 records ....problem in dim_demographics_city_race')
    else:
        print('dim_demographics_city_race is Ok')
        
    # checking if dim_demographics_city_population is ok
    
    noZeroCount = True
    for my_bucket_object in my_bucket.objects.filter(Prefix="demographics_data/dim_demographics_city_population/"):
        if my_bucket_object.key.endswith('parquet'):
            df = spark.read.parquet(output_data + my_bucket_object.key)
            if df.count()<1:
                print('0 records ....problem in dim_demographics_city_population')
                noZeroCount = False
                break
    if noZeroCount:
        print('dim_demographics_city_population is Ok')  
        
    # checking if dim_temperature_city is ok
    
    noZeroCount = True
    for my_bucket_object in my_bucket.objects.filter(Prefix="temperature_data/dim_temperature_city/"):
        if my_bucket_object.key.endswith('parquet'):
            df = spark.read.parquet(output_data + my_bucket_object.key)
            if df.count()<1:
                print('0 records ....problem in dim_temperature_city')
                noZeroCount = False
                break
    if noZeroCount:
        print('dim_temperature_city is Ok')
        
    # checking if dim_temperature_usa is ok
    
    noZeroCount = True
    for my_bucket_object in my_bucket.objects.filter(Prefix="temperature_data/dim_temperature_usa/"):
        if my_bucket_object.key.endswith('parquet'):
            df = spark.read.parquet(output_data + my_bucket_object.key)
            if df.count()<1:
                print('0 records ....problem in dim_temperature_usa')
                noZeroCount = False
                break
    if noZeroCount:
        print('dim_temperature_usa is Ok')
        
    # checking if city_code is ok
    
    df = spark.read.parquet(output_data + "label_data/city_code/")
    if df.count()<1:
        print('0 records ....problem in city_code')
    else:
        print('city_code is Ok')
        
    # checking if country_code is ok
    
    df = spark.read.parquet(output_data + "label_data/country_code/")
    if df.count()<1:
        print('0 records ....problem in country_code')
    else:
        print('country_code is Ok')
        
    # checking if state_code is ok
    
    df = spark.read.parquet(output_data + "label_data/state_code/")
    if df.count()<1:
        print('0 records ....problem in state_code')
    else:
        print('state_code is Ok')     

def sample_joins():
    """
    Sample joins to check that tables were created as intended
    
    Arguments: None
            
    :return: None
    """
    
    logging.info("Joining dim_temperature_usa and dim_airport on 'city' column")
    dim_temperature_usa.join(dim_airport,dim_temperature_usa["city"] == dim_airport["city"]).show(5)
    logging.info("Joining dim_airport_city_state_code and city_code on 'city' column")
    dim_airport_city_state_code.join(city_code,["city"]).distinct().show(5)
    logging.info("Joining dim_airport and city_code on 'city' column")
    dim_airport.join(city_code,["city"]).distinct().show(5)
    logging.info("Joining fact_immigration and city_code on 'city_code' column")
    fact_immigration.join(city_code,fact_immigration["city_code"]==city_code["abbr_code"]).distinct().drop(col("abbr_code")).show(5)
    logging.info("Joining fact_immigration & city_code on 'city_code' column & joining result to dim_temperature_city on 'city' column")
    df1=fact_immigration.join(city_code,fact_immigration["city_code"]==city_code["abbr_code"]).distinct().drop(col("abbr_code"))
    df1.join(dim_temperature_city,["city"]).distinct().show(5)
    logging.info("Joining dim_temperature_city and dim_demographics_city_population on 'city' column")
    dim_temperature_city.join(dim_demographics_city_population,["city"]).distinct().show(5)
 
def check_schema():
    """
    Print schema to check that created tables have schema as intended
    
    Arguments: None
            
    :return: None
    """
        
    logging.info("fact_immigration.printSchema")
    fact_immigration.printSchema()
    logging.info("dim_immigration_entdepa.printSchema")
    dim_immigration_entdepa.printSchema()
    logging.info("dim_immigration_entdepd.printSchema")
    dim_immigration_entdepd.printSchema()
    logging.info("dim_immigration_visa.printSchema")
    dim_immigration_visa.printSchema()
    logging.info("dim_immigration_airline.printSchema")
    dim_immigration_airline.printSchema()
    logging.info("dim_immigration_personal.printSchema")
    dim_immigration_personal.printSchema()
    logging.info("dim_airport_type.printSchema")
    dim_airport_type.printSchema()
    logging.info("dim_airport_city_state_code.printSchema")
    dim_airport_city_state_code.printSchema()
    logging.info("dim_airport.printSchema")
    dim_airport.printSchema()
    logging.info("dim_temperature_city.printSchema")
    dim_temperature_city.printSchema()
    logging.info("dim_temperature_usa.printSchema")
    dim_temperature_usa.printSchema()
    logging.info("dim_demographics_race.printSchema")
    dim_demographics_race.printSchema()
    logging.info("dim_demographics_city_race.printSchema")
    dim_demographics_city_race.printSchema()
    logging.info("dim_demographics_city_population.printSchema")
    dim_demographics_city_population.printSchema()
    logging.info("country_code.printSchema")
    country_code.printSchema()
    logging.info("city_code.printSchema")
    city_code.printSchema()
    logging.info("state_code.printSchema")
    state_code.printSchema()
    
def checkRowAllNull():
    """
    Check if there is a row in a table with all entries NULL
    
    Arguments: None
            
    :return: None
    """
    
    logging.info("checkRowAllNull in progress")
    result = fact_immigration.filter("cic_id is NULL AND year is NULL and month is NULL AND city_code is NULL and state_code is NULL and arrival_date is NULL and departure_date is NULL and mode is NULL and country is NULL and matflag is NULL and entdepa_code is NULL and entdepd_code is NULL and dtaddto is NULL and dtadfile is NULL and occup is NULL and count is NULL").count()
    if result==0:
        print('fact_immigration is OK')
    else:
        print('fact_immigration has a row with all nulls') 
    
    result = dim_immigration_entdepa.filter("code is NULL and entdepa is NULL").count()
    if result==0:
        print('dim_immigration_entdepa is OK')
    else:
        print('dim_immigration_entdepa has a row with all nulls')
        
    result = dim_immigration_entdepd.filter("code is NULL and entdepd is NULL").count()
    if result==0:
        print('dim_immigration_entdepd is OK')
    else:
        print('dim_immigration_entdepd has a row with all nulls') 
        
    result = dim_immigration_visa.filter("visa is NULL and visa_type is NULL").count()
    if result==0:
        print('dim_immigration_visa is OK')
    else:
        print('dim_immigration_visa has a row with all nulls')
        
    result = dim_immigration_airline.filter("cic_id is NULL and airline is NULL and admin_num is NULL and flight_number is NULL and visa_type is NULL and visapost is NULL").count()   
    if result==0:
        print('dim_immigration_airline is OK')
    else:
        print('dim_immigration_airline has a row with all nulls')
     
    result = dim_immigration_personal.filter("cic_id is NULL and country_residence is NULL and country_citizenship is NULL and birth_year is NULL and gender is NULL and ins_num is NULL").count()    
    if result==0:
        print('dim_immigration_personal is OK')
    else:
        print('dim_immigration_personal has a row with all nulls')
    
    result = dim_airport_type.filter("code is NULL and airport_type is NULL").count()
    if result==0:
        print('dim_airport_type is OK')
    else:
        print('dim_airport_type has a row with all nulls') 
 
    result = dim_airport_city_state_code.filter("city is NULL and state_code is NULL and country is NULL").count()  
    if result==0:
        print('dim_airport_city_state_code is OK')
    else:
        print('dim_airport_city_state_code has a row with all nulls')
    
    result = dim_airport.filter("ident is NULL and airport_type_code is NULL and airport_name is NULL and elevation_ft is NULL and city is NULL and iata_code is NULL and airport_longitude is NULL and airport_latitude is NULL").count()  
    if result==0:
        print('dim_airport is OK')
    else:
        print('dim_airport has a row with all nulls')
    
    result = dim_temperature_city.filter("city is NULL and latitude is NULL and longitude is NULL").count()  
    if result==0:
        print('dim_temperature_city is OK')
    else:
        print('dim_temperature_city has a row with all nulls')
    
    result = dim_temperature_usa.filter("date is NULL and avg_temp is NULL and avg_temp_var is NULL and city is NULL and country is NULL and year is NULL and month is NULL").count()  
    if result==0:
        print('dim_temperature_usa is OK')
    else:
        print('dim_temperature_usa has a row with all nulls')
    
    result = dim_demographics_race.filter("race_code is NULL and race_type is NULL").count()  
    if result==0:
        print('dim_demographics_race is OK')
    else:
        print('dim_demographics_race has a row with all nulls')
    
    result = dim_demographics_city_race.filter("idx is NULL and race_code is NULL").count()  
    if result==0:
        print('dim_demographics_city_race is OK')
    else:
        print('dim_demographics_city_race has a row with all nulls')
    
    result = dim_demographics_city_population.filter("idx is NULL and city is NULL and state_code is NULL and male_pop is NULL and female_pop is NULL and num_vetarans is NULL and foreign_born is NULL and median_age is NULL and avg_household_size is NULL").count()  
    if result==0:
        print('dim_demographics_city_population is OK')
    else:
        print('dim_demographics_city_population has a row with all nulls')
        
    result = country_code.filter("code is NULL and country is NULL").count()
    if result==0:
        print('country_code is OK')
    else:
        print('country_code has a row with all nulls')
        
    result = city_code.filter("abbr_code is NULL and city is NULL").count()
    if result==0:
        print('city_code is OK')
    else:
        print('city_code has a row with all nulls')
    
    result = state_code.filter("abbr_code is NULL and state is NULL").count()
    if result==0:
        print('state_code is OK')
    else:
        print('state_code has a row with all nulls')
        
def main():
    """
    Main function to call other procedures
    
    :return None
    """
    spark = create_spark_session()
    output_data = "s3a://koolice/"
    bucket_name="koolice" 
    
    logging.info("Data processing started")
    
    process_immigration_data(spark, output_data)
    process_airport_data(spark, output_data)
    process_temperature_data(spark, output_data)
    process_demographics_data(spark, output_data)
    process_label_descriptions(spark, output_data)
    process_data_check(spark, output_data, bucket_name)
    checkRowAllNull()
    check_schema()
    sample_joins()
  
    logging.info("Data processing completed")
    
    
    
if __name__ == "__main__":
    main()    

