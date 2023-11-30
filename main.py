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

class DatabaseConnectorCleaningExtraction:

    def __init__(self, engine):
        self.engine = engine

    # Reads the credentials for the database from the YAML file.

    def read_db_creds(self):
        with open(r'Multinational Retail Data\sales_data\db_creds.yaml', 'r') as file:
            db_creds = yaml.safe_load(file)
            return db_creds
        
    # Creates and initiates engine connection to AWS.

    def init_db_engine(self):
        db_creds = self.read_db_creds()
        engine = create_engine(f"{db_creds['DATABASE_TYPE']}+{db_creds['DBAPI']}://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        engine = engine.connect()
        return engine
    
    # Lists the tables extracted from the connection to the AWS server.

    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        return inspector.get_table_names()
    
    # Uploads the tables extracted to PGAdmin4.

    def upload_to_db(self, table_name):
        engine = self.init_db_engine()
        df = self.clean_user_data(table_name)
        df.to_sql(name = "dim_users", con = engine)

    # Reads SQL table from AWS connection and returns a Pandas DataFrame.

    def read_rds_table(self, table_name):
        engine = self.init_db_engine()
        df = pd.read_sql_table(table_name, engine)
        return df
    
    # Reads PDF from link and returns a Pandas DataFrame.
        
    def retrieve_pdf_data(self, path):
        df = tabula.read_pdf(path, pages = "all")
        return df
    
    # Extracts table from AWS and returns a Pandas DataFrame.

    # Removes NULL values from SQL table and returns a Pandas Dataframe.

    def clean_user_data(self, table_name):
        df = self.read_rds_table(table_name)
        df.to_csv(r"Multinational Retail Data\sales_data\Multination_legacy_users.csv")
        read_df = pd.read_csv(r"Multinational Retail Data\sales_data\Multination_legacy_users.csv")
        read_df = read_df.dropna()
        return read_df
    
    # Removes NULL values from PDF and returns a Pandas Dataframe.
        
    def clean_card_data(self, path):
        df = self.retrieve_pdf_data(path)
        for i in df:
            i.to_csv(r"Multinational Retail Data\sales_data\card_details.csv")
        read_df = pd.read_csv(r"Multinational Retail Data\sales_data\card_details.csv")
        read_df = read_df.dropna()
        return read_df
    

DatabaseConnectorCleaningExtraction.clean_user_data("legacy_users")


