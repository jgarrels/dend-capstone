# imports
import os, time, json, sh, configparser
from sh import aws
import numpy as np
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

# read configuration file with AWS credentials
config = configparser.ConfigParser()
config.read('aws.cfg')

# make AWS credentials accessible to the AWS CLI
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

# make bucket and folder names accessible to the AWS CLI in the shell
# not strictly necessary for this script, but helpful if you want to 
#    execute some commands directly in the command line
os.environ['DEST_BUCKET']=config['S3']['DEST_BUCKET']
os.environ['IMM_KEY']=config['S3']['IMM_KEY']
os.environ['DESC_KEY']=config['S3']['DESC_KEY']
os.environ['TEMPERATURE_KEY']=config['S3']['TEMPERATURE_KEY']
os.environ['DEMOGRAPHICS_KEY']=config['S3']['DEMOGRAPHICS_KEY']
os.environ['AIRPORT_KEY']=config['S3']['AIRPORT_KEY']

# define the paths where the raw data is
# imm_folder_loc = "../../data/18-83510-I94-Data-2016"
# airport_file_loc = "./airport-codes_csv.csv"
# demographics_file_loc = "./us-cities-demographics.csv"
# temperature_file_loc = "../../data2/GlobalLandTemperaturesByCity.csv"
imm_folder_loc = config['LOCAL']['IMM_FOLDER_LOC']
airport_file_loc = config['LOCAL']['AIRPORT_FILE_LOC']
demographics_file_loc = config['LOCAL']['DEMOGRAPHICS_FILE_LOC']
temperature_file_loc = config['LOCAL']['TEMPERATURE_FILE_LOC']

# base folder for local data writing
local = config['LOCAL']['LOCAL_KEY']

# define the destination paths where the data should go, e.g. on S3
# the destination S3 bucket
bucket = config['S3']['DEST_BUCKET']
# names of folders where data is written to
imm_key = config['S3']['IMM_KEY']
airport_key = config['S3']['AIRPORT_KEY']
demographics_key = config['S3']['DEMOGRAPHICS_KEY']
temperature_key = config['S3']['TEMPERATURE_KEY']
desc_key = config['S3']['DESC_KEY']

# define function for regular S3 upload where nothing needs to be excluded,
# and variables that simplify it
onlyerrors = "--only-show-errors"
s3 = "s3"
sync = "sync"


def s3_folder_upload(key, bucket):
    """Uploads files from a local folder with name key to a folder of the same name in an S3 Bucket named bucket."""
    aws(s3, sync, os.path.join(local, key), os.path.join(bucket, key), onlyerrors, _fg=True)

def create_spark_session():
    spark = SparkSession.builder \
        .config("spark.jars.packages",\
                "org.apache.hadoop:hadoop-aws:2.7.1,com.amazonaws:aws-java-sdk:1.7.4,saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def data_quality_check(pd_dataframe, condition_wanted, message):
    """Check if condition_wanted is fulfilled in each of the dataframe's rows, 
    otherwise counts it as an error and then prints out the message with the total number of errors.
    Arguments:
    - pd_dataframe: a pandas dataframe
    - condition_wanted: a condition every row should fulfill, as a string
    - message: a message printed before the error count"""
    errors = 0
    for index, row in pd_dataframe.iterrows():
        if not eval(condition_wanted):
            errors += 1
    print(message, errors)

def immigration_data_processing(spark):
    """Takes a SparkSession as input and then
    processes the immigration data with it."""

    # create dataframe by reading in january data
    df_imm =spark.read.format('com.github.saurfang.sas.spark')\
                        .load(os.path.join(imm_folder_loc, 'i94_jan16_sub.sas7bdat'))
    print("Immigration for jan loaded")
    # list of months
    months = ["feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
    # columns the dataframe has in the end = columns of january file
    imm_columns = df_imm.columns

    # For some reason, the file for June has more columns, all starting with 'delete', probably deprecated.
    # I will thus select only the columns present in all of the dataframes.
    # load and append the remaining 11 months:
    for month in months:
        df_imm_next = spark.read.format('com.github.saurfang.sas.spark')\
                            .load(os.path.join(imm_folder_loc, f'i94_{month}16_sub.sas7bdat'))
        print(f"Immigration for {month} loaded")
        df_imm = df_imm.union(df_imm_next[imm_columns])
        print(f"Immigration for {month} appended")

    print("Immigration: All months loaded")

    # drop duplicates
    df_imm = df_imm.dropDuplicates()

    # select only people who came by plane -> i94mode==1
    df_imm = df_imm[df_imm['i94mode']==1]

    # drop NaN values in important columns
    df_imm = df_imm.dropna(subset=['i94yr', 'i94mon', 'i94port', 'arrdate', 'i94bir'])

    # select the most relevant columns for Travely
    df_imm = df_imm['i94yr', 'i94mon', 'i94port', 'i94addr', 'i94visa', 'arrdate', 'depdate', 'biryear', 'gender', 'visatype']

    # rename columns, e.g. i94yr -> year, i94port -> airport_code
    df_imm = df_imm.withColumnRenamed("i94yr", "year") \
                    .withColumnRenamed("i94mon", "month") \
                    .withColumnRenamed("i94port", "airport_code") \
                    .withColumnRenamed("i94addr", "address") \
                    .withColumnRenamed("i94visa", "visacode")

    # replace date columns with duration of stay column
    date_converter = F.udf(lambda x: datetime.fromordinal(x), T.DateType())
    df_imm = df_imm.withColumn("stay_duration", (F.col("depdate") - F.col("arrdate")))
    df_imm = df_imm.drop("depdate").drop("arrdate")

    # write data to parquet files

    # Here I chose to write the parquet files locally and then use the aws cli to upload them to the S3 bucket.
    # Time comparison: 15-20 min writing + 1-2 min uploading 
    # vs. writing directly to S3: not finished even after 6 hours

    df_imm.write \
        .partitionBy('month').mode('overwrite') \
        .parquet(os.path.join(local, imm_key))

    # Here, I only partitioned by month, since all the data is from the year 2016 
    # and it would thus not make sense to include the year in the partitioning process.
    # However, should Travely decide to include more immigration from other years, 
    # year would definitely be the first partitioning key, followed by month.

    print("Immigration data: processing finished")

def temperature_data_processing(spark):
    """Takes a SparkSession as input and then
    processes the temperature data with it."""
    # read temperature into pandas DataFrame
    pd_temperature = pd.read_csv(temperature_file_loc)

    # drop NaN values
    pd_temperature = pd_temperature.dropna()

    # select only US
    pd_temperature = pd_temperature[pd_temperature['Country']=='United States']
    
    # select only timestamps after 2000-01-01 (including)
    pd_temperature = pd_temperature[pd_temperature['dt']>='2000-01-01']

    # rename columns
    pd_temperature.rename(columns = {"AverageTemperature": "avg_temperature",
                                    "AverageTemperatureUncertainty": "avg_temperature_uncert",
                                    "City": "city",
                                    "Country": "country",
                                    "Latitude": "latitude",
                                    "Longitude": "longitude"},
                            inplace = True)

    # read data from pandas DataFrame into Spark DataFrame
    df_temperature = spark.createDataFrame(pd_temperature)
    
    # check that dataframe is not empty
    if df_temperature.head(1) != 0:
        print("Data Quality Check: data frame not empty, passed")
    else:
        print("DATAFRAME EMPTY")

    # check that there are multiple cities and dates
    if df_temperature.groupby("city").count().head(1) != 0 :
        print("Data Quality Check: multiple cities, passed")
    else:
        print("Data Quality Check FAILED: missing cities")
    if df_temperature.groupby("dt").count().head(1) != 0:
        print("Data Quality Check: multiple and dates, passed")
    else:
        print("Data Quality Check FAILED: missing or dates")
    
    df_temperature.write \
        .partitionBy('city') \
        .mode('overwrite') \
        .parquet(os.path.join(local, temperature_key))
    
    print("Temperature data: PROCESSING FINISHED")

def airport_data_processing(spark):
    """Takes a SparkSession as input and then
    processes the airport data with it."""

    # read data into pandas DataFrame
    pd_airport = pd.read_csv(airport_file_loc, delimiter=",")

    # drop all rows without an IATA code
    pd_airport = pd_airport.dropna(subset=["iata_code"])

    # drop less relevant columns and verify
    pd_airport.drop(columns = ["ident", "local_code", "continent", "gps_code"],
     inplace=True)

     # rename columns
    pd_airport.rename(columns = {"type": "airport_type",
                                "name": "airport_name",
                                "iata_code": "airport_code"},
                    inplace = True)

    # data quality check: relevant columns do not contain null/NaN
    if (pd_airport.isna().sum()["airport_name"] == 0) and (pd_airport.isna().sum()["airport_code"] == 0):
        print("Data Quality Check - passed: No missing airport names or airport codes")
    else:
        print("Data Quality Check - FAILED: Missing airport names or airport codes")

    # create schema for data
    schema = T.StructType([T.StructField("airport_type", T.StringType()),
                        T.StructField("airport_name", T.StringType()),
                        T.StructField("elevation_ft", T.DoubleType()),
                        T.StructField("iso_country", T.StringType()),
                        T.StructField("iso_region", T.StringType()),
                        T.StructField("municipality", T.StringType()),
                        T.StructField("airport_code", T.StringType()),
                        T.StructField("coordinates", T.StringType())])

    # read data from pandas DataFrame into Spark DataFrame
    df_airport = spark.createDataFrame(pd_airport, schema)

    # write data to parquet
    df_airport.write \
                    .partitionBy('iso_country') \
                    .mode('overwrite') \
                    .parquet(os.path.join(local, airport_key))
    
    print("Airport data: PROCESSING FINISHED")

def demographics_data_processing(spark):
    """Takes a SparkSession as input and then
    processes the demographics data with it."""

    # read data into pandas DataFrame
    pd_demographics = pd.read_csv(demographics_file_loc, delimiter=";")

    # drop NaN values
    pd_demographics.dropna(inplace = True)

    # drop irrelevant columns
    pd_demographics.drop(columns = ["Race", "Count"], inplace = True)

    # since now the rest of the rows is each a duplicate (or even a "quadruplicate"),
    # drop the repeated rows
    pd_demographics.drop_duplicates(inplace = True)

    # rename columns for easier joining
    # and to match snake_case
    pd_demographics.rename(columns = {"City": "city",
                                    "State": "state",
                                    "Median Age": "median_age",
                                    "Male Population": "male_population",
                                    "Female Population": "female_population",
                                    "Total Population": "population",
                                    "Number of Veterans": "veterans",
                                    "Foreign-born": "foreign_born",
                                    "Average Household Size": "avg_household_size",
                                    "State Code": "state_code"},
                        inplace = True)

    # people occur only in integers -> change type to int64 for columns male_population, female_population, veterans, foreign_born
    pd_demographics = pd_demographics.astype({"male_population": "int64",
                                            "female_population": "int64",
                                            "veterans": "int64",
                                            "foreign_born": "int64"})

    # data quality checks
    data_quality_check(pd_demographics,
                    "row.population >= (row.male_population + row.female_population)",
                    "Data quality issues in population count: ")
    data_quality_check(pd_demographics,
                    "row.population > row.foreign_born",
                    "Data quality issues in foreign_born: ")
    data_quality_check(pd_demographics,
                    "row.population > row.avg_household_size",
                    "Data quality issues in avg_household_size: ")
    data_quality_check(pd_demographics,
                    "row.male_population > row.veterans",
                    "Data quality issues in veterans: ")

    # read data from pandas DataFrame into Spark DataFrame
    df_demographics = spark.createDataFrame(pd_demographics)

    # write data to parquet locally
    df_demographics.write \
                    .partitionBy('state_code') \
                    .mode('overwrite') \
                    .parquet(os.path.join(local, demographics_key))

    print("Demographics data: PROCESSING FINISHED")

def upload_to_s3(bucket):
    """Uploads all the files containing the fully processed data
    to the S3 bucket specified."""
    # upload immigration description data
    aws(s3, sync, os.path.join(local, desc_key), os.path.join(bucket, desc_key), '--exclude', '*ipynb*', onlyerrors, _fg=True)
    print("Immigration description data: json files uploaded to S3")

    # upload immigration data
    s3_folder_upload(imm_key, bucket)
    print("Immigration data: parquet files uploaded to S3") 

    # upload temperature data
    s3_folder_upload(temperature_key, bucket)
    print("Temperature data: uploaded to S3")

    # upload airport data
    s3_folder_upload(airport_key, bucket)
    print("Airport data: uploaded to S3")

    # upload demographics data
    s3_folder_upload(demographics_key, bucket)
    print("Demographics data: uploaded to S3")

def main():
    """
    Executes the entire program.
    
    - create a Spark session
    - process the four data sources into tables
    - write tables locally into parquet files
    - upload the fully processed data files to S3
    """
    # create Spark session
    spark = create_spark_session()

    # process data and write to files
    temperature_data_processing(spark)
    airport_data_processing(spark)
    demographics_data_processing(spark)
    immigration_data_processing(spark)

    # upload to S3
    upload_to_s3(bucket)
    
    print("Program run completed")


if __name__ == "__main__":
    main()
