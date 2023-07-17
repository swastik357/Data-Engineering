# Project Description

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis. In this project, I created a database schema and ETL pipeline for this analysis. 

# Description of files:

test.ipynb displays the first few rows of each table to let you check your database.
create_tables.py drops and creates your tables.
etl.ipynb reads and processes a single file from song_data and log_data and loads the data into your tables. 
etl.py reads and processes files from song_data and log_data and loads them into your tables. 
sql_queries.py contains all your sql queries, and is imported into the last three files above.

# Schema for Song Play Analysis:

Using the song and log datasets, I created a star schema optimized for queries on song play analysis. This includes the following tables.

## Fact Table

songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## Dimension Tables

users - users in the app
user_id, first_name, last_name, gender, level

songs - songs in music database
song_id, title, artist_id, year, duration

artists - artists in music database
artist_id, name, location, latitude, longitude

time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday