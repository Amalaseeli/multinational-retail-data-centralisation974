import pandas as pd
import tabula
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import requests
import boto3

header={
    "x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
}
s3_address="s3://data-handling-public/products.csv"


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

    def list_num_of_sores(self, header:dict):
        url="https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
        response=requests.get(url, headers=header)
        data=response.json()
        print(data) #451
        return data['number_stores']
    
    def retrieve_stores_data(self,num_of_stores,header):
        store_data=[]
        for store_number in range(num_of_stores):
            try:

                url=f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
                response=requests.get(url, headers=header)

                if response.status_code !=200:
                    print(f"Failed retrive store {store_number}: status code :{response.status_code}")
                    continue
                
                else:
                    store_details=response.json()
                    store_data.append(store_details)
                    df=pd.DataFrame(store_data)

            except requests.RequestException as e:
                print(f"Request failed for store {store_number}: {e}")
        df=pd.DataFrame(store_data)
        print(df.head())
        print(df.info())
        print(len(df))
        df.to_csv('strore_details.csv')
        return df
    
    def extract_from_s3(self, address):
        df=pd.read_csv(address)
        print(df.head)
        df.to_csv('product_data.csv')
        return df
    
    def extract_date_events(self):
        url="https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
        response = requests.get(url)
        data=response.json()
        df=pd.DataFrame(data)
        return df
    
if __name__ == "__main__":
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()
    data_cleaner = DataCleaning()
    tables = db_connector.list_db_tables()
    #['legacy_store_details', 'dim_card_details', 'legacy_users', 'orders_table']
    #print(tables)
    user_data_table = "legacy_users"  
    orders_data="orders_table"

    user_data_df = data_extractor.read_rds_table(db_connector, user_data_table)
    order_df=data_extractor.read_rds_table(db_connector, orders_data)
    #order_df.to_csv('order.csv')
    # print(order_df.head())
    # print(user_data_df.shape)
    dfs = data_extractor. retrieve_pdf_data()
   
    cleaned_user_data_df = data_cleaner.clean_user_data(user_data_df)
    cleaned_user_data_df.info()
    db_connector.upload_to_db(cleaned_user_data_df, "dim_users")
    data_extractor.retrieve_pdf_data()
    cleaned_card_data_df = data_cleaner.clean_card_data(dfs)
    db_connector.upload_to_db(cleaned_card_data_df, "dim_card_details")
    num_of_stores=data_extractor.list_num_of_sores(header)
    store_df=data_extractor.retrieve_stores_data(num_of_stores, header)
    cleaned_store_data_df=data_cleaner.called_clean_store_data(store_df)
    db_connector.upload_to_db(cleaned_store_data_df,"dim_store_details")
    product_df=data_extractor.extract_from_s3(s3_address)
    cleaned_product_df=data_cleaner.convert_product_weights(product_df)
    products_df = data_cleaner.clean_products_data(cleaned_product_df)
    db_connector.upload_to_db(products_df, "dim_products")

    orders_df=data_cleaner.clean_orders_data(order_df)
    db_connector.upload_to_db(orders_df, "orders_table")

    date_event_df=data_extractor.extract_date_events()
    date_event_df=data_cleaner.clean_date_event(date_event_df)
    db_connector.upload_to_db(date_event_df, "dim_date_times")






   
    