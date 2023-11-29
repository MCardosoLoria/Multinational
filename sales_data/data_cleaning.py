import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
from pandasgui import show
import pandas as pd
from data_extraction import DataExtractor

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
    






