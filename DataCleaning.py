import pandas as pd
import re
import DataExtractor

class DataCleaning:

    # Removes NULL and incorrect values from users table and returns a Pandas Dataframe.

    def clean_user_data(table_name):
        df = DataExtractor.read_rds_table(table_name)
        df = df.loc[df['country'].isin(['Germany', 'United Kingdom', 'United States'])]
        df.to_csv(r"Multinational Retail Data\sales_data\Multination_legacy_users.csv")
        return df
    
    # Removes NULL and incorrect values from PDF and returns a Pandas Dataframe
        
    def clean_card_data(table_name):
        df = DataExtractor.retrieve_pdf_data(table_name)
        for card_data in df:
            card_data.to_csv(r"Multinational Retail Data\sales_data\card_details.csv")
        read_df = pd.read_csv(r"Multinational Retail Data\sales_data\card_details.csv")
        read_df = read_df.dropna()
        read_df.to_csv(r"Multinational Retail Data\sales_data\card_details.csv")
        return read_df
    
    # Removes NULL and incorrect values from stores pdf and returns a Pandas Dataframe
    
    def clean_store_data():
        df = DataExtractor.retrieve_stores_data()
        df = pd.DataFrame(df)
        df = df.loc[df['store_type'].isin(['Local', 'Super Store', 'Mall Kiosk', 'Outlet'])]
        df.to_csv(r"Multinational Retail Data\sales_data\Multination_stores_data.csv")
        return df
    
    # Removes NULL and incorrect values from orders table and returns a Pandas Dataframe
    
    def clean_orders_data(table_name):
        df = DataExtractor.read_rds_table(table_name)
        df = df.drop(columns = ['first_name', 'last_name', '1', 'level_0'])
        df.to_csv(r"Multinational Retail Data\sales_data\Multination_orders_table.csv")
        return df
    
    # Function used in cleaning products data for multipack weights
    
    def multipack_conversion(x):
        if 'x' in x:
            a, b, c = x.split(' ')
            c = c.replace('g', '')
            x = int(a) * int(c)
            return x
        else:
            return x

    # Function used to convert weight to kg from g and ml
       
        # ^ = starts with
        # ? = 0 or one occurence
        # \d = where the string contains digits
        # * = zero or more occurences
        # \w = contains word characters

    def divide_by_1000(x):
        regex = re.compile(r"^(?P<numbers>\d*\.\d*|\d+)(?P<letters>\w*)$")
        x = str(x)
        match = regex.search(x)
        if match:
            a, b = match.groups()
            a = float(a)
            if b == 'kg':
                return a
            if b == 'g' or 'ml':
                return a / 1000
        else:
            return None
        
    # Removes NULL and incorrect values from products table and returns a Pandas Dataframe

    def clean_products_data():
        read_df = pd.read_csv(r"Multinational Retail Data\sales_data\Multinational_products.csv")
        read_df = read_df.dropna()

        # Drops incorrect rows
        read_df = read_df.loc[read_df['removed'].isin(['Still_avaliable', 'Removed'])]
        
        # Creates Unit column
        read_df['weight_unit'] = read_df['weight'].apply(lambda x: re.findall('[a-z]{0,2}$', x))
        read_df['weight_unit'] = read_df['weight_unit'].apply(lambda x: x[0])

        # Converts multipacks to one integer value
        read_df['weight'] = read_df['weight'].apply(lambda x: DataCleaning.multipack_conversion(x))

        # Creates Weight column
        read_df['weight'] = read_df['weight'].apply(lambda x: DataCleaning.divide_by_1000(x))

        # Changes unit values to kg
        read_df['weight_unit'] = read_df['weight_unit'].apply(lambda x: x.replace('g', 'kg'))
        read_df['weight_unit'] = read_df['weight_unit'].apply(lambda x: x.replace('ml', 'kg'))
        read_df['weight_unit'] = read_df['weight_unit'].apply(lambda x: x.replace('kk', 'k'))

        # Writes to .csv
        read_df.to_csv(r"Multinational Retail Data\product_data_Cleaned.csv", index = False, header = True)
        return read_df
    
    # Removes NULL and incorrect values from date times table and returns a Pandas Dataframe

    def clean_dates_data():
        read_df = pd.read_json(r"Multinational Retail Data\sales_data\Multination_date_events.json")
        read_df = read_df.dropna()
        read_df.to_csv(r"Multinational Retail Data\date_time_cleaned.csv", index = False, header = True)
        read_df = pd.read_csv(r"Multinational Retail Data\date_time_cleaned.csv")
        read_df = read_df.loc[read_df['month'].isin(['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12'])]
        read_df.to_csv(r"Multinational Retail Data\date_time_cleaned.csv", index = False, header = True)
        return read_df

