import fastapi
import uvicorn
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import psycopg2
import tabula
import jpype
import boto3

class DatabaseConnector:

    def _init_ (self, engine, db_creds, inspector):
        self.engine = engine
        self.db_creds = db_creds
        self.inspector = inspector

    # Reads the credentials for the database from the YAML file.

    def read_db_creds(self):
        with open(r'Multinational Retail Data\sales_data\db_creds.yaml', 'r') as file:
            self.db_creds = yaml.safe_load(file)
            return self.db_creds
        
    # Creates and initiates engine connection to AWS.

    def init_db_engine(self):
        self.db_creds = DatabaseConnector.read_db_creds()
        self.engine = create_engine(f"{self.db_creds['DATABASE_TYPE']}+{self.db_creds['DBAPI']}://{self.db_creds['RDS_USER']}:{self.db_creds['RDS_PASSWORD']}@{self.db_creds['RDS_HOST']}:{self.db_creds['RDS_PORT']}/{self.db_creds['RDS_DATABASE']}")
        self.engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        self.engine = self.engine.connect()
        return self.engine
    
    # Lists the tables extracted from the connection to the AWS server.

    def list_db_tables(self):
        self.engine = DatabaseConnector.init_db_engine()
        self.inspector = inspect(self.engine)
        return self.inspector.get_table_names()
    
    # Uploads the tables extracted to PGAdmin4.

    def upload_to_db(self, table_name):
        self.engine = DatabaseConnector.init_db_engine()
        df = DataCleaning.clean_user_data(table_name)
        df.to_sql(name = "dim_users", con = self.engine)

class DataExtractor:

    # Reads SQL table from AWS connection and returns a Pandas DataFrame.

    def read_rds_table(table_name):
        engine = DatabaseConnector.init_db_engine()
        df = pd.read_sql_table(table_name, engine)
        return df
    
    # Reads PDF from link and returns a Pandas DataFrame.
        
    def retrieve_pdf_data(path):
        df = tabula.read_pdf(path, pages = "all")
        return df
    
    # Extracts table from AWS and returns a Pandas DataFrame.
    
class DataCleaning:

    # Removes NULL values from SQL table and returns a Pandas Dataframe.

    def clean_user_data(table_name):
        df = DataExtractor.read_rds_table(table_name)
        df.to_csv(r"Multinational Retail Data\sales_data\Multination_legacy_users.csv")
        read_df = pd.read_csv(r"Multinational Retail Data\sales_data\Multination_legacy_users.csv")
        read_df = read_df.dropna()
        return read_df
    
    # Removes NULL values from PDF and returns a Pandas Dataframe.
        
    def clean_card_data(path):
        df = DataExtractor.retrieve_pdf_data(path)
        for i in df:
            i.to_csv(r"Multinational Retail Data\sales_data\card_details.csv")
        read_df = pd.read_csv(r"Multinational Retail Data\sales_data\card_details.csv")
        read_df = read_df.dropna()
        return read_df
    

DatabaseConnector.upload_to_db("legacy_users")


