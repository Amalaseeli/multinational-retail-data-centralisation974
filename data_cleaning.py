import pandas as pd
from database_utils import DatabaseConnector
import numpy as np
class DataCleaning:
    """ methods to clean data from each of the data sources"""
    
    def clean_user_data(self, df):
        df=df.drop('index', axis=1)
        df = df.replace("NULL", np.nan)
       
        df=df.convert_dtypes()
        df.dropna(axis=0, how='all', inplace=True)
        df['join_date_converted'] = pd.to_datetime(df['join_date'],format='mixed' , errors='coerce')
        non_date_entries = df[df['join_date_converted'].isna()]['join_date']
        df = df.dropna(subset=['join_date_converted'])
     
    # Print the non-date entries
        if not non_date_entries.empty:
            print("Non-date entries in 'join_date' column:")
            print(non_date_entries)
        else:
            print("No non-date entries found in 'join_date' column.")

        
        print(df.shape)
        return df
    
    def clean_card_data(self, dfs):
        dfs.to_csv('card_data.csv')
        dfs=dfs.drop('card_number expiry_date', axis=1)
        dfs = dfs.loc[:, ~dfs.columns.str.contains('^Unnamed')]
        
        dfs = dfs.replace("NULL", np.nan)
        print(dfs.isnull().sum())
        #print(dfs[dfs.isnull().all(axis=1)])
        dfs.dropna(axis=0, how="all", inplace=True)
        print(len(dfs))

        #df2=pd.DataFrame(dfs["card_number"].unique())
        #df2.to_csv('card.csv')
        #print(dfs["card_number"].unique())
        #print(dfs[dfs.duplicated(subset=['card_number'])==True])
        print(len(dfs))
        #dfs.drop_duplicates(subset=['card_number'], inplace=True, ignore_index=True,keep='first')
      
        print(dfs.info())
        print(len(dfs))
       
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
        # dfs = dfs[dfs['card_number'].notna()]
        dfs['date_payment_confirmed'] = pd.to_datetime(dfs['date_payment_confirmed'], format="mixed", errors="coerce")
        # # #non_date_entries =dfs[dfs['date_payment_confirmed'].isna()]['date_payment_confirmed']
        dfs.dropna(subset=['date_payment_confirmed'], inplace=True)
        dfs.drop('card_number_numeric', axis=1, inplace=True)
        # dfs.head()
        print(dfs.info())
        print(len(dfs))
        print(dfs.head())
        return dfs


    
data_cleaner = DataCleaning()

