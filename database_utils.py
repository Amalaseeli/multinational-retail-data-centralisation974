import yaml
from sqlalchemy import create_engine
from sqlalchemy import text
import psycopg2
import pandas as pd

    
class DatabaseConnector:
    """" contains methods use to connect with and upload data to the database."""
    
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as f:
            credentials=yaml.safe_load(f)
        return credentials

    def init_db_engine(self):
        credentials=self.read_db_creds()
        RDS_HOST=credentials.get("RDS_HOST")
        RDS_PASSWORD=credentials.get("RDS_PASSWORD")
        RDS_USER=credentials.get("RDS_USER")
        RDS_DATABASE=credentials.get("RDS_DATABASE")
        RDS_PORT=credentials.get("RDS_PORT")

        engine = create_engine(f"postgresql://{RDS_USER}:{ RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
    
        return engine
    
    def list_db_tables(self):
        engine = self.init_db_engine()
        with engine.connect() as conn:
            tables = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'"))
            print(tables)
            return [table[0] for table in tables]
        
    def init_local_db_engine(self):
        credentials=self.read_db_creds()
        DATABASE_TYPE = credentials.get("DATABASE_TYPE")
        DBAPI = credentials.get("DBAPI")
        HOST = credentials.get("HOST")
        USER = credentials.get("USER")
        PASSWORD = credentials.get("PASSWORD")
        DATABASE = credentials.get("DATABASE")
        PORT = credentials.get("PORT")
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine


    def upload_to_db(self, df, table_name):
        engine = self.init_local_db_engine()
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data uploaded successfully to {table_name}.")
        return engine
    
    def read_postgreSQL_table(self):
        engine = self.init_local_db_engine()
        with engine.connect() as conn:
            results = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'"))
            tables= results.fetchall()
            tables=[table[0] for table in tables]
            #['dim_users', 'dim_store_details', 'dim_card_details', 'dim_products', 'orders_table', 'dim_date_times']
            print(tables)
            return tables 
