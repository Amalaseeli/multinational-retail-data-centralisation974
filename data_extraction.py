import pandas as pd
import tabula
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning

class DataExtractor:
    """ This class will work as a utility class, in it you will be creating methods that help extract data from different data sources.
The methods contained will be fit to extract data from a particular data source, these sources will include CSV files, an API and an S3 bucket."""
    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine()
        df = pd.read_sql_table(table_name, con=engine)
        
        #print(df.info())
        return df
    
    def retrieve_pdf_data(self):
        pdf_path= "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        list_df=tabula.read_pdf(pdf_path, stream=True, pages="all")
        dfs = pd.concat(list_df, ignore_index=True)
        print(len(dfs))
       
        return dfs

    
db_connector = DatabaseConnector()
data_extractor = DataExtractor()
#tables = db_connector.list_db_tables()
#['legacy_store_details', 'dim_card_details', 'legacy_users', 'orders_table']
#print(tables)
user_data_table = "legacy_users"  # Replace with the actual table name from `tables`
#user_data_df = data_extractor.read_rds_table(db_connector, user_data_table)
# print(user_data_df.shape)
#dfs = data_extractor. retrieve_pdf_data()
data_cleaner = DataCleaning()
#cleaned_user_data_df = data_cleaner.clean_user_data(user_data_df)
#cleaned_user_data_df.info()
#db_connector.upload_to_db(cleaned_user_data_df, "dim_users")
#data_extractor.retrieve_pdf_data()
#cleaned_card_data_df = data_cleaner.clean_card_data(dfs)
#db_connector.upload_to_db(cleaned_card_data_df, "dim_card_details")


   
    