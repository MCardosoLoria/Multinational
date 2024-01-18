# Multinational-Retail-Data-Centralisation

## Table of Contents
- Project Brief <br>
- Project Dependencies <br>
- Description of Project <br>
  - Extracting and Cleaning Data from Data Sources <br>
  - Creating the Database Schema <br>
  - Querying the Data <br>
- Usage Instructions <br>
  - Extracting and Cleaning Data from Data Sources <br>
  - Creating the Database Schema <br>
  - Querying the Data <br>
- File Structure <br>

## Project Brief 
Technologies used: Python, APIs, PostgreSQL, Pgadmin4, Pandas, AWS RDS.  <br>
Developed a system that extracts retail sales data from different data sources; PDF documents; an AWS RDS database; RESTful API, JSON and CSV files to fix 100% of irregularities in the data.  <br>
Thoroughly processed and cleansed a substantial volume of 100k+ records, preparing the data for modelling within a star-based database schema which involved problem solving and organization to carry out.  <br>
Conducted in-depth analysis of the processed data, unveiling valuable insights relevant to the retail industry for enhancing business operations and decision-making processes; increasing readability of the data by 45%.  <br>

## Project Dependencies
In order to run this project, the following modules need to be installed:

- re
- pandas
- tabula
- json
- boto3
- requests

## Description of the Project
### Extracting and Cleaning Data from Data Sources  <br>

Here, in the DatabaseConnector.py, the read_db_creds method is used to load the credentials to be used when connecting to the engine that will provide the names of the tables. <br>
The init_db_engine method is used to input the database credentials into the engine and connect to it securely.  <br>
The list_db_tables method is used to gain acces to the names of the tables which will be extracted, cleaned and uploaded to PGAdmin4.  <br>
The upload_to_db method is used to upload the selected table onto the database by providing the .csv file where the table was cleaned and written onto.  <br>

Next, in the DataExtractor.py, the read_rds_table method is used to extract and return the dataframes for legacy_users and orders_table.  <br>
The retrieve_pdf_data method is used to extract and return the dataframes for card_details. <br>
The list_number_of_stores is used to extract and return the number of stores in the given link so that the next method can used that to compile and extract the correct number of stores data. <br>
The retrieve_stores_data is used to extract and return the dataframes for stores_data. <br>
The extract_from_s3_products is used to extract and return the dataframes for products_list. <br>
The extract_from_json_dates is used to extract and return the dataframes for date_times. <br>

Finally, in the DataCleaning.py, the clean_user_data method is used to remove NULL and incorrect values from users table and to return a Pandas Dataframe.  <br>
The clean_card_data method is used to remove NULL and incorrect values from card_details table and to return a Pandas Dataframe. <br>
The clean_store_data method is used to remove NULL and incorrect values from stores_data table and to return a Pandas Dataframe.  <br>
The clean_orders_data method is used to remove NULL and incorrect values from orders_table and to return a Pandas Dataframe.  <br>
The clean_products_data method is used to remove NULL and incorrect values from products_data and to return a Pandas Dataframe. Also, the mthod deals with seperating the weights column into their respective size and unit which is then converted to kilograms for all the weights.  <br>
The clean_dates_data method is used to remove NULL and incorrect values from dates_table and to return a Pandas Dataframe.  <br>

### Creating the Database Schema  <br>

Here, in the Database Schema.sql, the queries convert data types to their correct data types for each table. For the products table, some of the queries remove the '£' sign from the price and adds a weight_class column and a weight_range column. Also, for the stores table, some of the queries corrects incorrect staff numbers to NULL since they were not clear for adaptation. <br>

### Querying the Data <br>

Here, in the Database Queries.sql, the queries return specific results required for the client. These are listed in the docstrings above each query within the file. <br>

## Usage Instructions
### Extracting and Cleaning Data from Data Sources <br>

Run the methods within each file, in order, to connect to the database, extract and clean the tables and finalise by uploading each table using the upload_to_db method. <br>

### Creating the Database Schema <br>

Connect to the SQL server by adding a connection in VSCode and filling in your PGAdmin4 credentials. <br>
Run the individual queries in the SQL server by turning the others into comments whilst doing so; using Ctrl+/ to do so. <br>

### Querying the Data <br>

Run the individual queries in the SQL server by turning the others into comments whilst doing so; using Ctrl+/ to do so to return the required results for the tables in question. <br>

## File Structure
DatabaseConnector.py <br>
DataExtractor.py <br>
DataCleaning.py <br>
Database Schema.sql <br>
Database Queries.sql <br>
README.md
