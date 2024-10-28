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
        RDS_HOST=credentials.get("RDS_HOST")#
        RDS_PASSWORD=credentials.get("RDS_PASSWORD")
        RDS_USER=credentials.get("RDS_USER")
        RDS_DATABASE=credentials.get("RDS_DATABASE")
        RDS_PORT=credentials.get("RDS_PORT")

        engine = create_engine(f"postgresql://{RDS_USER}:{ RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        # with engine.connect() as connection:
        #     connection.execute(text("SET transaction_read_only = off;"))
        #     print("Connected and ensured write mode is active.")
        with engine.connect() as connection:
            connection.execute(text("SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE;"))
        
    
        return engine
    
    def list_db_tables(self):
        engine = self.init_db_engine()
        with engine.connect() as conn:
            tables = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'"))
            print(tables)
            return [table[0] for table in tables]

    def upload_to_db(self, df, table_name):
        engine = self.init_db_engine()
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
       
databaseconn=DatabaseConnector()
databaseconn.list_db_tables()