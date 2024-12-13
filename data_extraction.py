import pandas as pd
import tabula

import requests
import boto3

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
        list_df=tabula.read_pdf(pdf_path, pages="all")
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
                    

            except requests.RequestException as e:
                print(f"Request failed for store {store_number}: {e}")
        df=pd.DataFrame(store_data)
        return df
    
    def extract_from_s3(self, address):
        df=pd.read_csv(address)
        return df
    
    def extract_date_events(self):
        url="https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
        response = requests.get(url)
        data=response.json()
        df=pd.DataFrame(data)
        return df
    
