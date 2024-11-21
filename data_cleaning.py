import pandas as pd
from database_utils import DatabaseConnector
import numpy as np
import re
class DataCleaning:
    """ methods to clean data from each of the data sources"""
    
    def clean_user_data(self, df):
        df=df.drop('index', axis=1)
        df = df.replace("NULL", np.nan)
        df=df.convert_dtypes()
        df.dropna(axis=0, how='all', inplace=True)
        df['join_date_converted'] = pd.to_datetime(df['join_date'],format='mixed' , errors='coerce')
        #non_date_entries = df[df['join_date_converted'].isna()]['join_date']
        df = df.dropna(subset=['join_date_converted'])
     
    # Print the non-date entries
        # if not non_date_entries.empty:
        #     print("Non-date entries in 'join_date' column:")
        #     print(non_date_entries)
        # else:
        #     print("No non-date entries found in 'join_date' column.")
        print(df.shape)
        return df
    
    def clean_card_data(self, dfs):
        dfs.to_csv('card_data.csv')
        dfs = dfs.loc[:, ~dfs.columns.str.contains('^Unnamed')]
        dfs = dfs.replace("NULL", np.nan)
        print(dfs.isnull().sum())
        #print(dfs[dfs.isnull().all(axis=1)])
        dfs.dropna(axis=0, how="all", inplace=True)
        print(len(dfs))
        #dfs.drop_duplicates(subset=['card_number'], inplace=True, ignore_index=True,keep='first'
        #print(dfs[dfs.isnull()])
        dfs['card_number'] = dfs['card_number'].astype('str').replace(r'\s*\?\s*', '', regex=True)
        dfs['card_number'].replace('nan', np.nan, inplace=True)
        # dfs['card_number'] = dfs['card_number'].astype('str').replace('?','')
       
        dfs['card_number_numeric'] =pd.to_numeric(dfs['card_number'], errors="coerce")
        non_numeric_rows = dfs[dfs['card_number_numeric'].isna() & dfs['card_number'].notna()]
        print(non_numeric_rows['card_number'].to_list())
        non_numeric_df=pd.DataFrame(non_numeric_rows)
        non_numeric_df.to_csv('non.csv')
        dfs.drop(non_numeric_rows.index, axis=0, inplace=True)
        len(non_numeric_rows)
        dfs['date_payment_confirmed'] = pd.to_datetime(dfs['date_payment_confirmed'], format="mixed", errors="coerce")
        # # #non_date_entries =dfs[dfs['date_payment_confirmed'].isna()]['date_payment_confirmed']
        dfs.dropna(subset=['date_payment_confirmed'], inplace=True)
        dfs.drop('card_number_numeric', axis=1, inplace=True)
        print(dfs.info())
        print(len(dfs))
        print(dfs.head())
        return dfs
    
    def  called_clean_store_data(self,df):
        df=df.replace('NULL', np.nan)
        df.dropna(how="all", axis=0, inplace=True)
        df['opening_date']= pd.to_datetime(df['opening_date'], format="mixed", errors="coerce")
        df.dropna(subset=['opening_date'], inplace=True)
        df['staff_numbers'] = df['staff_numbers'].str.replace(r'\D', '', regex=True)
        print(len(df))
        return df
    
    def convert_product_weights(self,df):
        df=df.drop('Unnamed: 0', axis=1)
        df=df.replace('Null', np.nan)
        df.dropna(axis=0, how="all", inplace=True)
        print(df.info)
        print(len(df))
        
        conversion_factors={
            'kg':1,
            'g':0.001,
            'ml':0.001,
            'oz':0.0283495,
            'lb':0.453592
        }

        def convert_to_kg(weight):

            match=re.match(r"([\d\.]+)\s*(kg|g|ml|oz|lb)?", str(weight).lower().strip())
            if match:
                value, unit = match.groups()
                value = float(value)

                if unit in conversion_factors:
                    return value * conversion_factors[unit]
                else:
                    print(f"Unknown unit '{unit}' encountered.")
                    return None
            
        df['weight']=df['weight'].apply(convert_to_kg)
        print(len(df))
       
        return df
        
    def clean_products_data(self,df):
        price_pattern=r"^£\d+(\.\d{2})?$"
        df['product_price']=df['product_price'].astype(str)
        df= df[df['product_price'].str.match(price_pattern)]
        print(df.head())
        print(len(df))
        return df
    
    def clean_orders_data(self,df):
        print(df.columns)
        df=df.drop(['first_name', 'last_name', '1'], axis=1)
        print(df.columns)
        print(len(df))
        return df
    
    def clean_date_event(self,df):
        df = df.replace('NULL', np.nan)
        df.dropna(axis=0, how="all", inplace=True)
        df.to_csv('date_event.csv')
       
        df[['month', 'year', 'day']]=df[['month', 'year', 'day']].apply(pd.to_numeric, errors="coerce")
        df.dropna(subset=['month', 'year', 'day'], inplace=True)
        print(df.info())
        print(len(df))
        return df


data_cleaner = DataCleaning()

