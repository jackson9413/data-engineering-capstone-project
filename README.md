# 1 About the project #
* This project studies the relationship between the immigrants to US states and the population demographics, the temperature, and the airports in the relevant state.
* It uses STAR schema to create different dimension and fact tables based on the data sourced publicily.
* The processes utlize on-premise tools. It develops ETL pipelines with Python, PySpark, and SQL.
* It uses AWS S3 to store all the staging data and builds a data warehouse by using AWS Redshift to store staging tables and normalized tables. 
<br >  

# 2 Getting started #
* "Capstone Project AWS.ipynb" contains all the codes for the project.  
* "sql_queries.py" contains all the sql queries to create the staging and normalized tables.
    1. Create S3 bucket and Redshift in the same region and create IAM role for Redshift to copy data from S3. And fill in all the parameters in "dwh.cfg" file.
    2. Launch a new terminal, and "cd" to the current working directory.   
    3. Type "python etl.py" to run ET processes and save the staging files in S3 bucket.  
    4. Then type "python create_insert_tables.py" to create the staging tables in Redshift and insert normalized tables based on the staging tables created. 
    5. Open "test.ipynb" and run all the cells to check whether the tables have been created successfully.
<br >

# 3 About the data #
* [I94 Immigration Data](https://www.trade.gov/national-travel-and-tourism-office): This data comes from the US National Tourism and Trade Office.
* [World Temperature Data](https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data?resource=download): This dataset came from Kaggle (The file is too big. Thus, don't upload in the repo. Can be downloaded from Kaggle directly).
* [U.S. City Demographic Data](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/): This data comes from OpenSoft.
* [Airport Code Table](https://datahub.io/core/airport-codes#pandas): This is a simple table of airport codes and corresponding cities. 
<br >

# 4 Schema and ETL #
* STAR Schema
    * dimension tables
        * dim_airports ("State" PK; "num_airports")
        * dim_temperature ("State", PK; "min_tm"; "max_tm")
        * dim_population ("State", PK; age, gender, race population, household, etc)
    * fact tables
        * fact_tourists ("cicid", PK; "State", FK)  
<br>

* ETL
    * airports
        1. to numeric "elevation_ft"
        2. Separate "iso_region" to two columns  
        3. Separate "coordinates" to two columns and round
        4. Drop other airport type only keeping "airport" & not closed
        5. Only include US
    * cities
        1. Transform columns "Race" and "Count" from long to wide
        2. To numeric: "Male Population", "Female Population", "Total Population", "Number of Veterans", "Foreign-born", "Count"
        3. To float: "Median Age", "Average Household Size"    
    * immigration
        1. to date "arrdate", "depdate", "dtadfile"
        2. to text "cicid", "i94cit", "i94res", "admnum"
        3. to integer "i94yr", "i94mon", "i94mode", "i94bir", "	i94visa", "count" 
        4. only keep the records with "i94addr" the same as the state codes in "cities" table
    * temperature
        1. Only include 2003 onwards of temperature for data usage
        2. Only include US
<br >

# 5 Data Dictionary #
* **dim_cities**:
    * "State Code" (PK): Code for states.
    * "Median Age": Median Age for population for US 2015 Census.
    * "Male Population": Male population for US 2015 Census.
    * "Female Population": Female Population for US 2015 Census.
    * "Total Population": Total Population for US 2015 Census.
    * "Number of Veterans": Number of Veterans for US 2015 Census.
    * "Foreign-born": Foreign-born population for US 2015 Census.
    * "Average Household Size": Average Household Size for US 2015 Census.
    * "American Indian and Alaska Native": American Indian and Alaska Native population for US 2015 Census.
    * "Asian": Asian population for US 2015 Census.
    * "Hispanic or Latino": Hispanic or Latino population for US 2015 Census.
    * "White": White population for US 2015 Census.
* **dim_airports**:
    * "region" (PK): Code for states.
    * "totalAirport": Number of airports in the state (including small, medium, and large airports).
* **dim_temperature**:
    * "State Code" (PK): Code for states.
    * "FallAvgTemp": Average temperature in Fall (Sep, Oct, Nov) from 2003 onwards.
    * "SummerAvgTemp": Average temperature in Summer (Jun, Jul, Oct) from 2003 onwards.
    * "	SpringAvgTemp": Average temperature in Spring (Mar, Apr, May) from 2003 onwards.
    * "WinterAvgTemp": Average temperature in Winter (Dec, Jan, Feb) from 2003 onwards.
* **fact_tourists**:
    * "cicid" (PK): cic id for tourists.
    * "i94yr": i94 filling year.
    * "i94mon": i94 filling month.
    * "i94cit": country codes.
    * "i94res": country codes.
    * "i94port": port initials.
    * "arrdate": Arrival Date in the USA.
    * "i94mode": 1 = 'Air, '2 = 'Sea', 3 = 'Land', 9 = 'Not reported' ;.
    * "i94addr" (FK): US state codes.
    * "depdate": Departure Date from the USA.
    * "i94bir": Respondant birth year.
    * "i94visa": visa codes, 1 = Business, 2 = Pleasure, 3 = Student.
    * "count": Used for summary statistics.
    * "dtadfile": Date added to I-94 Files.
    * "visapost":  Department of State where where Visa was issued.
    * "entdepa": Arrival Flag - admitted or paroled into the U.S..
    * "entdepd": Departure Flag - Departed, lost I-94 or is deceased.
    * "matflag": Match flag - Match of arrival and departure records.    
    * "biryear": 4 digit year of birth.
    * "dtaddto": Date to which admitted to U.S. (allowed to stay until).
    * "gender": Non-immigrant sex.
    * "airline": Airline used to arrive in U.S..
    * "admnum": Admission Number.
    * "fltno": Flight number of Airline used to arrive in U.S..
    * "visatype": Class of admission legally admitting the non-immigrant to temporarily stay in U.S..           
