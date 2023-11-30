import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import tabula
import jpype
from database_utils import DatabaseConnector

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
    
DataExtractor.read_rds_table("legacy_users")

