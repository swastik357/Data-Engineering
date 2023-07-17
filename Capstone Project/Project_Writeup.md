# UDACITY DATA ENGINEERING CAPSTONE PROJECT

## PROJECT DESCRIPTION:

In this project, I prepared source of truth data-warehouse by collecting, processing and transforming 
data from multiple data sources and storing the processed tables on AWS S3 destination bucket (details of original data sources and their processing will be discussed later). Therafter, I ran some data quality checks (discussed in detail later) and some sample joins (discussed in detail later) to confirm that the process went on correctly and that the original plan of extracting value out combination of related data from multiple data sources was accomplished successfully. Exploratory data analysis is done in Exploratory_Data_Analysis.ipynb and ETL on full data is there in etl.py 

### SOURCES OF RAW DATA AND THEIR PROCESSING:

1) Airport data: airport-codes_csv.csv:
This dataset (in csv format) contains details of airports (like name, type, country, co-ordinates, etc)
I processed this data into three tables: dim_airport_type, dim_airport_city_state_code, dim_airport (details discussed later)

2) Immigration data: ../../data/18-83510-I94-Data-2016/:
This dataset (in SAS format) contains details of international visitor arrivals to US (like cic_id, arrival date, departure date, city code, state code, etc)
I processed this data into six tables: fact_immigration, dim_immigration_entdepa, dim_immigration_entdepd, dim_immigration_visa, dim_immigration_airline, dim_immigration_personal (details discussed later)

3) Temperature data: ../../data2/GlobalLandTemperaturesByCity.csv:
This dataset (in csv format) contains details of temperature of cities (like average temperature and average temperature uncertainty) along with details of cities (like name, latitude, longitude and country)
I processed this data into two tables: dim_temperature_city and dim_temperature_usa (details discussed later)

4) Demographics data: us-cities-demographics.csv:
This dataset(in csv format) contains demographic details of cities in US (like median age, male population, female population, etc)
I processed this data into three tables: dim_demographics_race, dim_demographics_city_race, dim_demographics_city_population (details discussed later)

5) Label description data: I94_SAS_Labels_Descriptions.SAS:
This dataset(in SAS format) contains details like names of cities, states and countries and their respective codes.
I processed this data into three tables: city_code, state_code, country_code (details discussed later)

### TRANSFORMED TABLES:
#### 1) Transformed tables from Airport data

dim_airport_type
 |-- code: integer Code of airport type (maps to airport_type_code in dim_airport)
 |-- airport_type: string Airport type

dim_airport_city_state_code
 |-- city: string Name of city (maps to city in dim_airport)
 |-- state_code: string State code
 |-- country: string Country

dim_airport
 |-- ident: string Ident code
 |-- airport_type_code: integer Airport type code 
 |-- airport_name: string Name of airport
 |-- elevation_ft: string Elevation in feet
 |-- city: string City
 |-- iata_code: string Iata code
 |-- airport_longitude: double Longitude of airport
 |-- airport_latitude: double Latitude of airport
 
#### 2) Transformed tables from Immigration data

fact_immigration
 |-- cic_id: double Cid_id
 |-- year: double Year
 |-- month: double Month
 |-- city_code: string City
 |-- state_code: string State_code
 |-- arrival_date: date Arrival date
 |-- departure_date: date Departure date
 |-- mode: double Mode
 |-- country: string Country
 |-- matflag: string Matflag
 |-- entdepa_code: string Entdepa_code
 |-- entdepd_code: string Entdepd_code
 |-- dtaddto: date Date addto
 |-- dtadfile: date Date adfile
 |-- occup: string Occupation
 |-- count: double Count
 |-- immigration_id: long Immigration_id

dim_immigration_entdepa
 |-- code: long Entdepa_code (maps to entdepa_code in fact_immigration)
 |-- entdepa: string Entdepa
 
dim_immigration_entdepd
 |-- code: long Entdepd_code (maps to entdepd_code in fact_immigration)
 |-- entdepa: string Entdepd
 
 dim_immigration_visa
 |-- visa_type: string Visa type (maps to visa_type in dim_immigration_airline)
 |-- visa: double Visa
 
dim_immigration_airline
 |-- cic_id: double Cic_id (maps to cic_id in fact_immigration)
 |-- airline: string Airline
 |-- admin_num: double Admission number
 |-- flight_number: string Flight number
 |-- visa_type: string Visa_type
 |-- visapost: string  Visa post
 |-- immigration_airline_id: Immigration airline id
 
dim_immigration_personal
 |-- cic_id: double Cic_id (maps to cic_id in fact_immigration)
 |-- country_residence: double Country of residence
 |-- country_citizenship: double  Country of citizenship
 |-- birth_year: double Birth year
 |-- gender: string Gender
 |-- ins_num: string Ins number
 |-- immi_personal_id: long Immigration personal id
 
#### 3) Transformed tables from Temperature data

dim_temperature_city
 |-- city: string City (maps to city in dim_temperature_usa)
 |-- latitude: string Latitude
 |-- longitude: string Longitude

dim_temperature_usa
 |-- date: string Date
 |-- avg_temp: string Average Temperature
 |-- avg_temp_var: string Average Temperature Variation
 |-- city: string City
 |-- country: string Country
 |-- year: integer Year
 |-- month: integer Month 
 
#### 4) Transformed tables from Demographics data

dim_demographics_race
 |-- race_code: long Race_code (maps to race_code in dim_demographics_city_race)
 |-- race_type: string Race_type

dim_demographics_city_race
 |-- idx: string Idx combining city and state code (maps to idx in dim_demographics_city_population)
 |-- race_code: string Race_code

dim_demographics_city_population
 |-- idx: string Idx combining city and state code 
 |-- city: string City
 |-- state_code: string State code
 |-- male_pop: string Male population
 |-- female_pop: string Female population
 |-- num_vetarans: string Number of veterans
 |-- foreign_born: string Foreign born
 |-- median_age: string Median age
 |-- avg_household_size: string Average Household size 
 
#### 5) Transformed tables from Label description data

country_code
 |-- code: string Country code 
 |-- country: string Country

city_code
 |-- abbr_code: string City Code
 |-- city: string City

state_code
 |-- abbr_code: string State code 
 |-- state: string State
 
### CHOICE OF TOOLS, TECHNOLOGIES AND DATA MODEL:

1) AWS S3: For storing processed tables because of advantages of scalability, accessibility. cost efficiency and security

2) Pandas: For exploratory data analysis because it is suitable for small data. Pandas data frame is stored in RAM and access is faster (because it local and primary memory access is fast) but limited to available memory, it is not scalable

3) PySpark: For data processing of entire data set and storing processed tables on AWS S3 because Pyspark is horizontally scalable

4) Data model has been constructed so that:
a. Table is less prone to data entry error. For instance, it is better to have a code 1 for New York City in separate table and use that code 1 in other table everytime New York City appears, than enter New York City every time and risk spelling error.
b. Characteristics unique to an entity are stored in a separate table along with the entity.
For instance, latitude and longitude of a city are unique to a city and it is better to have a separate table which contains cities and their latitude and longitude than repeat all three information in other table everytime the city occurs.

### DATA QUALITY CHECKS:

There are four data quality checks run through four different procedures:

1) process_data_check: This checks that all tables are non-empty
2) checkRowAllNull(): This checks that no table contains any row with all nulls
3) check_schema(): This prints scheme of final tables to confirm that they are 
as intended
4) sample_joins(): This runs a few sample joins accross processed tables from 
different data sources to cofirm that they can be combined together correctly 
to display more enriched combination of related data across different data sources
(the details of sample joins will be discussed later)

### Sample joins on processed tables to confirm value addition:
In the sample_joins() procedure , I run following joins accross processed tables from multiple data sources to confirm that they can be joined correctly and that there was value addition by combining related data from multiple data sources. The results of these joins will be displayed on terminal upon running etl.py:

1) Joining dim_temperature_usa and dim_airport on 'city' column:
dim_temperature_usa.join(dim_airport,dim_temperature_usa["city"] == dim_airport["city"]).show(5)

2) Joining dim_airport_city_state_code and city_code on 'city' column:
    dim_airport_city_state_code.join(city_code,["city"]).distinct().show(5)
    logging.info("Joining dim_airport and city_code on 'city' column")
    dim_airport.join(city_code,["city"]).distinct().show(5)
    
3) Joining fact_immigration and city_code on 'city_code' column:
    fact_immigration.join(city_code,fact_immigration["city_code"]==city_code["abbr_code"]).distinct().drop(col("abbr_code")).show(5)
    
4) Joining fact_immigration & city_code on 'city_code' column & joining result to dim_temperature_city on 'city' column:
    df1=fact_immigration.join(city_code,fact_immigration["city_code"]==city_code["abbr_code"]).distinct().drop(col("abbr_code"))
    df1.join(dim_temperature_city,["city"]).distinct().show(5)
    
5) Joining dim_temperature_city and dim_demographics_city_population on 'city' column:
    dim_temperature_city.join(dim_demographics_city_population,["city"]).distinct().show(5)
    
### ADDRESSING OTHER SCENARIOS:

1) The data was increased by 100x: We would consider putting data on AWS EMR, a distributed data cluster for processing large data sets on cloud.
2) The pipelines would be run on a daily basis by 7 am every day: We would consider using Apache Airflow for building up ETL data pipeline and automate.
3) The database needed to be accessed by 100+ people: We would consider moving the database to Amazon Redshift (budget permitting).

### Data Update Frequency

Tables created from Immigration data and Temperature data should be updated monthly since they are updated monthly. Tables created from Demographics data could be updated annually because census collection is infrequent and expensive. Tables created from Airport data and Label description data can be updated annually as the data changes very infrequently.

### HOW TO RUN THE PROJECT:

1) Enter AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in dl.cfg
2) Set output_data = <s3_bucket> (eg. output_data = "s3a://koolice/") in etl.py
3) Set bucket_name (eg. if output_data = "s3a://koolice/", then bucket_name="koolice") in etl.py 
4) Run python3 etl.py in terminal

