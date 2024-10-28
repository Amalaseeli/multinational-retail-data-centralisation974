import pandas as pd
from database_utils import DatabaseConnector
import numpy as np
class DataCleaning:
    """ methods to clean data from each of the data sources"""
    
    def clean_user_data(self, df):
        # Example cleaning steps:
        df.replace("NULL", np.nan, inplace=True)
        df.dropna(inplace=True)  # Drop rows with NULL values
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')  # Parse dates
        
        return df
    
data_cleaner = DataCleaning()

