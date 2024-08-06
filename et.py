import pandas as pd
import numpy as np
import configparser
import os
import boto3 # aws Python API
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, date_add, expr, regexp_replace


def et_staging_airports(sourceFile, s3Bucket):
    """
    ET for staging table airports and save as a csv file in s3.
    INPUT: 
        sourceFile: source file path for "airports"
        s3Bucket: AWS S3 bucket for storing staging file
    OUTPUT:
        None
    """
    # read data as "string" format to ensure all the information is correct
    airport = pd.read_csv(sourceFile, dtype="str")
    # US airport 
    airportUS = (airport.loc[(airport.iso_country == "US") & ~(airport.type == "closed") & airport.type.str.contains("airport")])     
    
    # change "elevation_ft" to numeric
    airportUS["elevation_ft"] = airportUS["elevation_ft"].astype(float)    
    # change "elevation_ft" to numeric
    airportUS["elevation_ft"] = airportUS["elevation_ft"].astype(float)
    # separate "iso_region" to "country" and "region"
    airportUS[["country", "region"]] = airportUS["iso_region"].str.split("-", 1, expand = True)     
    # separate "iso_region" to "country" and "region" and round 4
    airportUS[["longitude", "latitude"]] = airportUS["coordinates"].str.split(",", 1, expand = True).astype(float).round(4)  
    
    # drop unnecessary columns 
    airportUSNew = (airportUS.drop(["iso_country", "iso_region", "coordinates", "country", "continent","iata_code", "gps_code", "local_code", "municipality"], axis=1))
    
    # drop NaN
    airportUSNewNoNa = airportUSNew.dropna()    
    
    # save it as a csv and remove header and index and move it to aws s3 
    airportUSNewNoNa.to_csv("airportUS.csv", sep=",", header=False, index=False, encoding="utf-8")
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('airportUS.csv', s3Bucket, 'airportUS.csv')
    
def et_staging_cities(sourceFile, s3Bucket):
    """
    ET for staging table cities and save as a csv file in s3.
    INPUT: 
        sourceFile: source file path for "cities"
        s3Bucket: AWS S3 bucket for storing staging file
    OUTPUT:
        return pandas data frame           
    """
    # read data as "string" format to ensure all the information is correct and assign delimiter ";"
    cities = pd.read_csv(sourceFile, delimiter=";", dtype="str")
            
    # drop NaN
    citiesNoNa = cities.dropna()    
    
    # transform from long to wide
    citiesPop = citiesNoNa[['City', 'State', 'Median Age', 'Male Population',
           'Female Population', 'Total Population', 'Number of Veterans',
           'Foreign-born', 'Average Household Size', 'State Code']]
    citiesNoNa["Count"] = citiesNoNa.Count.astype(float)
    citiesRace = (pd.pivot_table(citiesNoNa[["City", "Race", "Count"]], 
                                 index="City", columns = "Race", values= "Count")
                              .reset_index().reset_index(drop=True).rename_axis(None, axis=1))
    citiesNew = citiesPop.merge(citiesRace, on="City", how="left")    
    
    # to integer 
    citiesNew[["Male Population", "Female Population", "Total Population", "Number of Veterans", "Foreign-born"]] =(citiesNew[["Male Population", "Female Population", "Total Population", "Number of Veterans", "Foreign-born"]].astype(int))
    
    # to float since some rows contain NaN
    # show errors if converting to int
    citiesNew[["American Indian and Alaska Native", "Asian", "Black or African-American", "Hispanic or Latino", "White"]] = (citiesNew[["American Indian and Alaska Native", "Asian", "Black or African-American", "Hispanic or Latino", "White"]].astype(float))    

    # to float
    citiesNew[["Median Age", "Average Household Size"]] = (citiesNew[["Median Age", "Average Household Size"]].astype(float))
    
    # drop duplicates
    citiesNewNoDup = citiesNew.drop_duplicates()
      
    # save it as a csv and remove header and index and move it to aws s3 
    citiesNewNoDup.to_csv("citiesUS.csv", sep=",", header=False, index=False, encoding="utf-8")
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('citiesUS.csv', s3Bucket, 'citiesUS.csv')
    
    return citiesNewNoDup
    
def et_staging_immigration(sourceFile, cities, s3Bucket):
    """
        ET for staging table immigration and save as a parquet file in s3.
    INPUT: 
        sourceFile: source file path for "immigration"
        cities: the above ET cities table
        s3Bucket: AWS S3 bucket for storing staging file        
    OUTPUT:
        None
    """
    
    # configure PySpark
    spark = (SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate())    
    
    # read the data 
    immigration=spark.read.parquet(sourceFile)
    
    # drop unnecessary columns
    immigrationNew = immigration.drop("visapost", "occup", "entdepu", "insnum")
    
    # drop "dtaddto" equal to "D/S" 
    # then can change "dtaddto" to date format since it contain "NaN"
    immigrationPlus = immigrationNew.filter(immigrationNew.dtaddto != "D/S")
    
    # change to date
    immigrationPlus = (immigrationPlus.withColumn("arrdate", expr("date_add('1960-01-01', arrdate)")).
                                        withColumn("depdate", expr("date_add('1960-01-01', depdate)")).
                                        withColumn("dtadfile", expr("to_date(dtadfile,'yyyyMMdd')")).
                                        withColumn("dtaddto", expr("to_date(dtaddto,'MMddyyyy')"))
                      )

    # change to int
    immigrationPlus = (immigrationPlus.withColumn("i94yr", col("i94yr").cast("float").cast("integer")).
                                         withColumn("i94mon", col("i94mon").cast("float").cast("integer")).
                                         withColumn("i94mode", col("i94mode").cast("float").cast("integer")).
                                         withColumn("i94bir", col("i94bir").cast("float").cast("integer")).
                                         withColumn("i94visa", col("i94visa").cast("float").cast("integer")).
                                         withColumn("count", col("count").cast("float").cast("integer")).
                                         withColumn("biryear", col("biryear").cast("float").cast("integer"))
                        )

    # change to text
    immigrationPlus = (immigrationPlus.withColumn("cicid", regexp_replace(col('cicid').cast("string"), '\.0', '')).
                        withColumn("i94cit", regexp_replace(col('i94cit').cast("string"), '\.0', '')).
                        withColumn("i94res", regexp_replace(col('i94res').cast("string"), '\.0', '')).
                        withColumn("admnum", col('admnum').cast("bigint").cast("string")))
    
    # only select these records with "i94addr" in the cities
    usStates = sorted(cities["State Code"].drop_duplicates())
    
    # filter the "i94addr" in cities "State Code" 
    immigrationPlusNew = immigrationPlus.filter(immigrationPlus.i94addr.isin(usStates))

    ## save the data to a parquet file in s3
    (immigrationPlusNew.write
        .mode("overwrite")
        .parquet("s3a://{}/immigration.parquet".format(s3Bucket)))    

def et_staging_temperature(sourceFile, s3Bucket):
    """
    ET for staging table temperature and save as a csv in s3.
    INPUT: 
        sourceFile: source file path for "temperature"
        s3Bucket: AWS S3 bucket for storing staging file        
    OUTPUT:
        return pandas data frame
    """
    # read data as "string" format to ensure all the information is correct
    temperature = pd.read_csv(sourceFile, dtype="str") 

    # temperature in 2003 and onwards and in US
    temperatureUS10Yr = temperature[(temperature.Country == "United States") 
                & (temperature.dt >= "2003-01-01")].drop("Country", axis=1)
    
    # drop NaN
    temperatureUS10YrNoNa = temperatureUS10Yr.dropna()     

    # change to date
    temperatureUS10YrNoNa["dt"] = pd.to_datetime(temperatureUS10YrNoNa["dt"], format="%Y-%m-%d")
    
     # to float and round to 1
    temperatureUS10YrNoNa[["AverageTemperature", "AverageTemperatureUncertainty"]] = temperatureUS10YrNoNa[["AverageTemperature", "AverageTemperatureUncertainty"]].astype(float).round(1)
    
    # save it as a csv and remove header and index and move it to aws s3 
    temperatureUS10YrNoNa.to_csv("temperatureUS.csv", sep=",", header=False, index=False, encoding="utf-8")
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('temperatureUS.csv', 'udacity-capstone-project-bucket-test', 'temperatureUS.csv')

def et_staging_tables(tables, s3Bucket):
    """
    Combine all the above defined ET functions together based on the table dictionary input and save the staging tables data in s3.
    INPUT:
        tables: dictionary containing the table names and source files
        s3Bucket: AWS S3 Bucket for storing staging files
    OUTPUT:
        None
    """
    for table in tables:
        # staging "airports"
        if table == "airports":
            sourceFile = tables[table]
            et_staging_airports(sourceFile, s3Bucket)
            
        # staging "cities"
        elif table == "cities":
            sourceFile = tables[table]
            cities = et_staging_cities(sourceFile, s3Bucket)            
            
        # staging "immigration"    
        elif (table == "immigration"):
            sourceFile = tables[table]
            et_staging_immigration(sourceFile, cities, s3Bucket)

        # staging temperature
        else:
            sourceFile = tables[table]
            et_staging_temperature(sourceFile, s3Bucket) 
                        

def main():
    """
    Assign the parameters for the functions and load the functions.
    """
    
    # get the parameter from aws configure file
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    S3BUCKET = config.get('S3', 'S3BUCKET')

    # confire AWS credentials
    os.environ['AWS_ACCESS_KEY_ID']=KEY
    os.environ['AWS_SECRET_ACCESS_KEY']=SECRET   
    
    # create a dictionary to store the table names and their source files
    tables = {"airports":"../airport-codes_csv.csv", "cities":"../us-cities-demographics.csv", "immigration":"../sas_data/*.parquet", "temperature":"../GlobalLandTemperaturesByState.csv"}
    
    s3_bucket = S3BUCKET
    
    # etl function for loading staging tables
    et_staging_tables(tables, s3_bucket)


if __name__ == "__main__":
    main()