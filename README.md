# Sparkify's Songs Cloud Database
  In this project, the song database is moved to Cloud. The data is firstly stored in AWS S3. For analysis purposes, an OLAP schema needs to be created, For this purpose, data from S3 is copied to AWS Redshift. AWS Redshift provides the parallel processing power that's needed for OLAP queries. Data stored in S3 -> Data staged in Redshift -> Data copied to Dimension tables in Redshift

# An explanation of the files in the repository
  sql_queries.py -> This script contains script for creating tables, inserting data tp tables in Redshift create_tables.py -> Executes the sql code in sql_queries.py to create tables in Redshift etl.py -> Executes the sql code in sql_queries.py to copy S3 data to staging tables and then inserting staged data to dimension tables in Redshift

# Steps to run the Python scripts:
  SQL statements to CREATE, COPY and INSERT data are written in sql_queries.py
  Run create_tables.py to create staging and dimension tables
  Run etl.py to 
    1. copy data from S3 to staging tables in Redshift
    2. insert data from staging tables to OLAP tables
