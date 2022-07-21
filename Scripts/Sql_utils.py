import os
import datetime
import re
import pandas as pd
from sqlalchemy import create_engine


class Sql_helper():
    def __init__(self, host, user, password, database, port=5432) -> None:
        self.sql_host = host
        self.sql_user = user
        self.sql_password = password
        self.database = database 
        self.sql_url = 'postgresql://{}:{}@{}:{}/{}'.format(user,
                                                            password,
                                                            host,
                                                            port,
                                                            database)   
        pass

    def check_connection(self):
        engine = create_engine(self.sql_url)
        #try:
        engine.connect()
        return engine
        #except:
            #return None

    def execute_sql_file(self, path, sql_engine):
        try:
            sql_engine.execute(open(path, "r").read())
        except Exception as e:
            print(e)
            return 0
        return 1


    def parquet_to_sql(self, full_path, table_name, sql_engine, limit=None):
        df = pd.read_parquet(full_path)
        try:
            df[:limit].to_sql(table_name, sql_engine, if_exists='append')
        except Exception as e:
            print(e)
            return 0
        return 1

    

    