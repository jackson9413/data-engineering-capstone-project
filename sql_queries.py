import configparser
import os

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY =config.get('AWS', 'KEY')
SECRET =config.get('AWS', 'SECRET')

S3BUCKET = config.get('S3', 'S3BUCKET')

DWH_ENDPOINT = config.get("CLUSTER", "HOST")
DWH_DB = config.get("CLUSTER", "DB_NAME") 
DWH_DB_USER = config.get("CLUSTER", "DB_USER") 
DWH_DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
DWH_PORT = config.get("CLUSTER", "DB_PORT") 

# aws environment configure
os.environ['AWS_ACCESS_KEY_ID']=KEY
os.environ['AWS_SECRET_ACCESS_KEY']=SECRET



# DROP TABLES

# staging
staging_cities_table_drop = "DROP TABLE IF EXISTS cities;"
staging_airports_table_drop = "DROP TABLE IF EXISTS airports;"
staging_immigration_table_drop = "DROP TABLE IF EXISTS immigration;"
staging_temperature_table_drop = "DROP TABLE IF EXISTS temperature;"

# normalized 
dim_airports_table_drop = "DROP TABLE IF EXISTS dim_airports;"
dim_cities_table_drop = "DROP TABLE IF EXISTS dim_cities;"
dim_temperature_table_drop = "DROP TABLE IF EXISTS dim_temperature;"
fact_immigration_table_drop = "DROP TABLE IF EXISTS fact_immigration;"


# CREATE TABLES

# staging
staging_airports_table_create= ("""CREATE TABLE IF NOT EXISTS airports
                                   (airport_id INT IDENTITY(0, 1) PRIMARY KEY,
                                       ident VARCHAR NOT NULL, 
                                       type VARCHAR NOT NULL, 
                                       name VARCHAR NOT NULL,
                                       elevation_ft FLOAT4 NOT NULL, 
                                       region VARCHAR NOT NULL,
                                       longitude FLOAT8 NOT NULL,
                                       latitude FLOAT8 NOT NULL);
               """)

staging_cities_table_create = ("""CREATE TABLE IF NOT EXISTS cities 
                                   (city_id INT IDENTITY(0, 1) PRIMARY KEY,
                                       City VARCHAR NOT NULL, 
                                       State VARCHAR NOT NULL, 
                                       "Median Age" FLOAT4 NOT NULL,
                                       "Male Population" INT NOT NULL, 
                                       "Female Population" INT NOT NULL,
                                       "Total Population" INT NOT NULL,
                                       "Number of Veterans" INT NOT NULL,
                                       "Foreign-born" INT NOT NULL,
                                       "Average Household Size" FLOAT4 NOT NULL,
                                       "State Code" VARCHAR NOT NULL,
                                       "American Indian and Alaska Native" FLOAT4,
                                       "Asian" FLOAT4,
                                       "Black or African-American" FLOAT4,
                                       "Hispanic or Latino" FLOAT4 NOT NULL,
                                       "White" FLOAT4 NOT NULL
                                   );
""")


staging_immigration_table_create= ("""CREATE TABLE IF NOT EXISTS immigration
                                       (cicid VARCHAR PRIMARY KEY NOT NULL,
                                           i94yr INT NOT NULL, 
                                           i94mon INT NOT NULL, 
                                           i94cit VARCHAR NOT NULL,
                                           i94res VARCHAR NOT NULL,
                                           i94port VARCHAR NOT NULL,
                                           arrdate DATE NOT NULL,
                                           i94mode INT NOT NULL,
                                           i94addr VARCHAR,
                                           depdate DATE,
                                           i94bir INT,
                                           i94visa INT NOT NULL,
                                           count INT NOT NULL,
                                           dtadfile DATE NOT NULL,
                                           entdepa VARCHAR NOT NULL,
                                           entdepd VARCHAR,
                                           matflag VARCHAR,
                                           biryear INT,
                                           dtaddto DATE,
                                           gender VARCHAR,
                                           airline VARCHAR,
                                           admnum VARCHAR NOT NULL,
                                           fltno VARCHAR,
                                           visatype VARCHAR NOT NULL);
               """)

staging_temperature_table_create = ("""CREATE TABLE IF NOT EXISTS temperature
                                       (temp_id INT IDENTITY(0, 1) PRIMARY KEY,
                                           dt DATE NOT NULL,
                                           AverageTemperature FLOAT4 NOT NULL,
                                           AverageTemperatureUncertainty FLOAT4 NOT NULL,
                                           State VARCHAR NOT NULL
                                       );
""")

# STAGING TABLES
staging_airports_copy = ("""COPY airports (ident, type, name, elevation_ft, region, longitude, latitude)
FROM 's3://{}/airportUS.csv'
credentials 'aws_access_key_id={};aws_secret_access_key={}' CSV;""".format(S3BUCKET, KEY, SECRET))

staging_cities_copy = ("""COPY cities 
FROM 's3://{}/citiesUS.csv'
credentials 'aws_access_key_id={};aws_secret_access_key={}'
CSV;""".format(S3BUCKET, KEY, SECRET))

staging_immigration_copy = ("""COPY immigration
FROM 's3://{}/immigration.parquet/'
credentials 'aws_access_key_id={};aws_secret_access_key={}'
PARQUET;""".format(S3BUCKET, KEY, SECRET))

staging_temperature_copy = ("""COPY temperature 
FROM 's3://{}/temperatureUS.csv'
credentials 'aws_access_key_id={};aws_secret_access_key={}'
CSV;""".format(S3BUCKET, KEY, SECRET))


# normalized tables

dim_airports_table_create = (""" CREATE TABLE dim_airports AS
                                SELECT DISTINCT tb.region,  totalAirport FROM
                                    (SELECT region, COUNT(ident) AS totalAirport
                                    FROM airports
                                    GROUP BY 1) tb
                                    WHERE region IN (SELECT DISTINCT "State Code" FROM cities)
                                    AND region IN (SELECT DISTINCT i94addr FROM immigration);
""")

dim_cities_table_create = (""" CREATE TABLE dim_cities AS
                                SELECT DISTINCT tb.* FROM
                                    (SELECT "State Code", 
                                    ROUND(AVG("Median Age"), 1) AS "Median Age",
                                    SUM("Male Population") AS "Male Population",
                                    SUM("Female Population") AS "Female Population",
                                    SUM("Total Population") AS "Total Population",
                                    SUM("Number of Veterans") AS "Number of Veterans",
                                    SUM("Foreign-born") AS "Foreign-born",
                                    ROUND(AVG("Average Household Size"), 1) AS "Average Household Size",
                                    ROUND(SUM("American Indian and Alaska Native"), 1) AS "American Indian and Alaska Native",
                                    ROUND(SUM([Asian]), 1) AS [Asian],
                                    ROUND(SUM("Black or African-American"), 1) AS "Black or African-American",
                                    ROUND(SUM("Hispanic or Latino"), 1) AS "Hispanic or Latino",
                                    ROUND(SUM([White]), 1) AS [White] FROM cities
                                    GROUP BY "State Code") tb
                                    WHERE "State Code" IN (SELECT DISTINCT region FROM airports)
                                    AND "State Code" IN (SELECT DISTINCT i94addr FROM immigration);
""")

dim_temperature_table_create = (""" CREATE TABLE dim_temperature AS
                                    WITH season AS (
                                    SELECT 
                                        CASE 
                                        WHEN EXTRACT(MONTH FROM dt) IN (03, 04, 05) THEN 'Spring'
                                        WHEN EXTRACT(MONTH FROM dt) IN (06, 07, 08) THEN 'Summer'
                                        WHEN EXTRACT(MONTH FROM dt) IN (09, 10, 11) THEN 'Fall'
                                        WHEN EXTRACT(MONTH FROM dt) IN (12, 01, 02) THEN 'Winter'
                                        END Season, 
                                        CASE 
                                        WHEN State = 'Georgia (State)' THEN 'Georgia'
                                        WHEN State = 'District Of Columbia' THEN 'District of Columbia'
                                        ELSE State
                                        END AS State
                                        , 
                                        ROUND(AVG(AverageTemperature), 1) AS AverageTemperature 
                                        FROM temperature
                                        GROUP BY 1, 2
                                    )
                                    SELECT DISTINCT cities."State Code", FallAvgTemp, SummerAvgTemp, SpringAvgTemp, WinterAvgTemp FROM
                                        (SELECT State, 
                                        CASE WHEN Season = 'Fall' THEN AverageTemperature END AS FallAvgTemp FROM season) fall 
                                        JOIN (
                                        SELECT State, 
                                        CASE WHEN Season = 'Summer' THEN AverageTemperature END AS SummerAvgTemp FROM season 
                                        ) summer
                                        ON fall.State = Summer.State
                                        JOIN (
                                        SELECT State, 
                                        CASE WHEN Season = 'Spring' THEN AverageTemperature END AS SpringAvgTemp FROM season     
                                        ) spring
                                        ON fall.State = spring.State
                                        JOIN (
                                        SELECT State, 
                                        CASE WHEN Season = 'Winter' THEN AverageTemperature END AS WinterAvgTemp FROM season        
                                        ) winter
                                        ON fall.State = winter.State
                                        JOIN cities 
                                        ON fall.State = cities.State
                                        WHERE cities."State Code" IN (SELECT DISTINCT region FROM airports)
                                        AND cities."State Code" IN (SELECT DISTINCT i94addr FROM immigration)
                                        AND FallAvgTemp IS NOT NULL AND SummerAvgTemp IS NOT NULL AND SpringAvgTemp IS NOT NULL AND WinterAvgTemp IS NOT NULL
                                ;
""")


fact_immigration_table_create = ("""CREATE TABLE fact_immigration AS
                                    SELECT * FROM 
                                    immigration  
                                    WHERE i94addr IN
                                    (SELECT DISTINCT "State Code" FROM cities)
                                    AND i94addr IN (SELECT DISTINCT region FROM airports);
                """)


# QUERY LISTS

drop_table_queries = [staging_cities_table_drop, staging_airports_table_drop, staging_immigration_table_drop, staging_temperature_table_drop, dim_airports_table_drop, dim_cities_table_drop, dim_temperature_table_drop, fact_immigration_table_drop]

create_staging_table_queries = [staging_airports_table_create, staging_cities_table_create, staging_immigration_table_create, staging_temperature_table_create]

copy_table_queries = [staging_airports_copy, staging_cities_copy, staging_immigration_copy, staging_temperature_copy]

create_and_insert_normalized_table_queries = [dim_airports_table_create, dim_cities_table_create, dim_temperature_table_create, fact_immigration_table_create]