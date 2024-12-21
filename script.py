from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor

def extract_and_upload_dim_users():
    '''
    In this function let us extract user details from the AWS RDS database, clean it and upload it to the local database
    
    '''
    tables = db_connector.list_db_tables()
    #['legacy_store_details', 'dim_card_details', 'legacy_users', 'orders_table']
    user_data_table = "legacy_users"  
    user_data_df = data_extractor.read_rds_table(db_connector, user_data_table)
    cleaned_user_data_df = data_cleaner.clean_user_data(user_data_df)
    # cleaned_user_data_df.info()
    db_connector.upload_to_db(cleaned_user_data_df, "dim_users")

def extract_order_data_and_upload():

    '''This function let us extract the source of truth for all orders the company has made 
    in the past stored in AWS RDS. Then clean the data and upload as orders_tabe'''

    orders_data="orders_table"   
    order_df=data_extractor.read_rds_table(db_connector, orders_data)
    order_data_df=data_cleaner.clean_orders_data(order_df)
    db_connector.upload_to_db(order_data_df, "orders_table")
    
def extract_card_data_and_upload():
    '''Here let us extract card details stored in the PDF format and clean and upload it to the local database as dim_card_details'''
    dfs = data_extractor. retrieve_pdf_data()
    cleaned_card_data_df = data_cleaner.clean_card_data(dfs)
    db_connector.upload_to_db(cleaned_card_data_df, "dim_card_details")

def extract_stores_data_and_upload():
    ''' The stored data retrived through API'''
    header={"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    num_of_stores=data_extractor.list_num_of_sores(header)
    store_df=data_extractor.retrieve_stores_data(num_of_stores, header)
    cleaned_store_data_df=data_cleaner.called_clean_store_data(store_df)
    db_connector.upload_to_db(cleaned_store_data_df,"dim_store_details")

def extract_product_data_and_upload():
    ''' Extract product details stored in csv format in an s3 bucket'''
    s3_address="s3://data-handling-public/products.csv"
    product_df=data_extractor.extract_from_s3(s3_address)
    cleaned_product_df=data_cleaner.convert_product_weights(product_df)
    products_df = data_cleaner.clean_products_data(cleaned_product_df)
    db_connector.upload_to_db(products_df, "dim_products")

def extract_date_event_data_and_upload():
    '''In this function we extract data stored in JSON file containing the details of each sale happend. After cleaning upload as dim_date_times table.'''
    date_event_df=data_extractor.extract_date_events()
    date_event_df=data_cleaner.clean_date_event(date_event_df)
    db_connector.upload_to_db(date_event_df, "dim_date_times")

if __name__ == '__main__':
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()
    data_cleaner = DataCleaning()
    extract_and_upload_dim_users()
    extract_order_data_and_upload()
    extract_card_data_and_upload()
    extract_stores_data_and_upload()
    extract_product_data_and_upload()
    extract_date_event_data_and_upload()




    






   
    