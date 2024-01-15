import pandas as pd
import tabula
import boto3
import requests
import json
import DatabaseConnector


class DataExtractor:

    # Reads SQL table from AWS connection and returns a Pandas DataFrame

    def read_rds_table(table_name):
        engine = DatabaseConnector.init_db_engine()
        df = pd.read_sql_table(table_name, engine)
        return df
    
    # Reads PDF from link and returns a Pandas DataFrame
        
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

    # Extracts table from AWS and returns a Pandas DataFrame

    def extract_from_s3_products():
        s3 = boto3.client("s3")
        products_list = s3.download_file("data-handling-public", "products.csv", r"Multinational Retail Data\sales_data\Multinational_products.csv")
        products_list = pd.DataFrame(products_list)
        return products_list
    
    # Extracts date events data and returns a Pandas Dataframe

    def extract_from_json_dates():
        s3 = boto3.client("s3")
        dates_list = s3.download_file("data-handling-public", "date_details.json", r"Multinational Retail Data\sales_data\Multination_date_events.json")
        dates_list = pd.DataFrame(dates_list)
        return dates_list
