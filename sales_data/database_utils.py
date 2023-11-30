import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import psycopg2
from data_cleaning import DataCleaning

class DatabaseConnector:

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

    def upload_to_db(table_name):
        engine = DatabaseConnector.init_db_engine()
        df = DataCleaning.clean_user_data(table_name)
        df.to_sql(name = "dim_users", con = engine)






