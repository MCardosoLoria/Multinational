import fastapi
import uvicorn
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import numpy as np
import pandas as pd
import psycopg2
import tabula
import jpype
import boto3
import requests
import json
import ast
import csv
import codecs
import awscli
import re

DATABASE_TYPE = 'postgresql'
DBAPI = 'psycopg2'
HOST = 'localhost'
USER = 'postgres'
PASSWORD = 'Santosfc1234'
DATABASE = 'sales_data'
PORT = 5432

class DatabaseConnector:

    # Add init with self.

    # Reads the credentials for the database from the YAML file.

    def read_db_creds():
        with open(r'Multinational Retail Data\sales_data\db_creds.yaml', 'r') as file:
            db_creds = yaml.safe_load(file)
            return db_creds
        
    # Creates and initiates engine connection to AWS.

    def init_db_engine():
        db_creds = DatabaseConnector.read_db_creds()
        engine = create_engine(f"{db_creds['DATABASE_TYPE']}+{db_creds['DBAPI']}://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        engine = engine.connect()
        return engine
    
    # Lists the tables extracted from the connection to the AWS server.

    def list_db_tables():
        engine = DatabaseConnector.init_db_engine()
        inspector = inspect(engine)
        return inspector.get_table_names()
    
    # Uploads the tables extracted to PGAdmin4.

    def upload_to_db():
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        engine = engine.connect()
        df = DataCleaning.clean_store_data()
        df.to_sql(name = "dim_store_details", con = engine, if_exists = 'replace', index = False)

    def upload_store_to_db():
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        engine = engine.connect()
        with open (r"Multinational Retail Data\sales_data\Multination_stores_data.csv", "r"):
            df = pd.read_csv(r"Multinational Retail Data\sales_data\Multination_stores_data.csv", sep='|', encoding='latin-1')
        df.to_sql(name = "dim_store_details", con = engine, if_exists = 'replace', index = False)

class DataExtractor:

    # Reads SQL table from AWS connection and returns a Pandas DataFrame.

    def read_rds_table(table_name):
        engine = DatabaseConnector.init_db_engine()
        df = pd.read_sql_table(table_name, engine)
        return df
    
    # Reads PDF from link and returns a Pandas DataFrame.
        
    def retrieve_pdf_data(table_name):
        df = tabula.read_pdf(table_name, pages = "all")
        return df
    
    # Lists number of stores in the AWS server

    def list_number_of_stores():
        url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
        headers = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
        response = requests.get(url, headers = headers)
        number_of_stores = response.json()
        return number_of_stores
    
    # Extracts stores data from AWS server

    def retrieve_stores_data():
        stores_list = []
        store_number = 1
        headers = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
        while store_number < 451:
            url = f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
            response = requests.get(url, headers = headers)
            data = response.text
            df = json.loads(data)
            stores_list.append(df)
            store_number += 1
        return stores_list

    # Extracts table from AWS and returns a Pandas DataFrame.

    def extract_from_s3_products():
        s3 = boto3.client("s3")
        products_list = s3.download_file("data-handling-public", "products.csv", r"Multinational Retail Data\sales_data\Multinational_products.csv")
        products_list = pd.DataFrame(products_list)
        return products_list
    
    # Extracts date events data and returns a Pandas Dataframe.

    def extract_from_json_dates():
        s3 = boto3.client("s3")
        dates_list = s3.download_file("data-handling-public", "date_details.json", r"Multinational Retail Data\sales_data\Multination_date_events.csv")
        dates_list = pd.DataFrame(dates_list)
        return dates_list

class DataCleaning:

    # Removes NULL values from users table and returns a Pandas Dataframe.

    def clean_user_data(table_name):
        df = DataExtractor.read_rds_table(table_name)
        df.to_csv(r"Multinational Retail Data\sales_data\Multination_legacy_users.csv")
        read_df = pd.read_csv(r"Multinational Retail Data\sales_data\Multination_legacy_users.csv")
        read_df = read_df.dropna()
        return read_df
    
    # Removes NULL values from PDF and returns a Pandas Dataframe.
        
    def clean_card_data(table_name):
        df = DataExtractor.retrieve_pdf_data(table_name)
        for card_data in df:
            card_data.to_csv(r"Multinational Retail Data\sales_data\card_details.csv")
        read_df = pd.read_csv(r"Multinational Retail Data\sales_data\card_details.csv")
        read_df = read_df.dropna()
        return read_df
    
    # Removes NULL values from pdf and returns a Pandas Dataframe.
    
    def clean_store_data():
        df = DataExtractor.retrieve_stores_data()
        df = ast.literal_eval(df)
        df = pd.DataFrame(df)
        df.to_csv(r"Multinational Retail Data\sales_data\Multination_stores_data.csv", index=False, header=True)
        read_df = pd.read_csv(r"Multinational Retail Data\sales_data\Multination_stores_data.csv")
        read_df = read_df.dropna()
        read_df = pd.concat(df, ignore_index = True)
        return read_df
    
    # Removes NULL values from orders table and returns a Pandas Dataframe.
    
    def clean_orders_data(table_name):
        df = DataExtractor.read_rds_table(table_name)
        df.to_csv(r"Multinational Retail Data\sales_data\Multination_orders_table.csv")
        read_df = pd.read_csv(r"Multinational Retail Data\sales_data\Multination_orders_table.csv")
        read_df = read_df.dropna()
        read_df = read_df.drop(columns = ['first_name', 'last_name', '1', 'level_0'])
        return read_df
    
    def clean_products_data():
        read_df = pd.read_csv(r"Multinational Retail Data\sales_data\Multinational_products.csv")
        read_df = read_df.dropna()
        read_df['weight'] = read_df['weight'].astype(str)
        def find_num():
            [int(num) for num in re.findall(r'\d+', read_df['weight']) if num.isdigit()]
        read_df['weight_split'] = read_df['weight'].apply(find_num)
        read_df.to_csv(r"Multinational Retail Data\product_data_Cleaned.csv", index = False, header = True)
        return read_df
    
    def clean_dates_data():
        read_df = pd.read_csv(r"Multinational Retail Data\sales_data\Multination_date_events.csv")
        read_df = read_df.dropna()
        return read_df

print(DataCleaning.clean_products_data())

