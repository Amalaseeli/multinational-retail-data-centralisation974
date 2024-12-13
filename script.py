from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor

s3_address="s3://data-handling-public/products.csv"
header={
    "x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
}

db_connector = DatabaseConnector()
data_extractor = DataExtractor()
data_cleaner = DataCleaning()
tables = db_connector.list_db_tables()
#['legacy_store_details', 'dim_card_details', 'legacy_users', 'orders_table']
user_data_table = "legacy_users"  
orders_data="orders_table"
user_data_df = data_extractor.read_rds_table(db_connector, user_data_table)
   
order_df=data_extractor.read_rds_table(db_connector, orders_data)
order_df.to_csv('Dataset/raw_dataset/order_data.csv')

dfs = data_extractor. retrieve_pdf_data()
dfs.to_csv('Dataset/raw_dataset/card_data.csv')
cleaned_user_data_df = data_cleaner.clean_user_data(user_data_df)
# cleaned_user_data_df.info()
db_connector.upload_to_db(cleaned_user_data_df, "dim_users")
cleaned_card_data_df = data_cleaner.clean_card_data(dfs)
db_connector.upload_to_db(cleaned_card_data_df, "dim_card_details")

num_of_stores=data_extractor.list_num_of_sores(header)
store_df=data_extractor.retrieve_stores_data(num_of_stores, header)
store_df.to_csv('Dataset/raw_dataset/store_data.csv')
cleaned_store_data_df=data_cleaner.called_clean_store_data(store_df)
db_connector.upload_to_db(cleaned_store_data_df,"dim_store_details")

product_df=data_extractor.extract_from_s3(s3_address)
product_df.to_csv('Dataset/raw_dataset/product_data.csv')
cleaned_product_df=data_cleaner.convert_product_weights(product_df)
products_df = data_cleaner.clean_products_data(cleaned_product_df)
db_connector.upload_to_db(products_df, "dim_products")

order_data_df=data_cleaner.clean_orders_data(order_df)
db_connector.upload_to_db(order_data_df, "orders_table")

date_event_df=data_extractor.extract_date_events()
date_event_df.to_csv('Dataset/raw_dataset/date_event.csv')
date_event_df=data_cleaner.clean_date_event(date_event_df)
db_connector.upload_to_db(date_event_df, "dim_date_times")


    






   
    