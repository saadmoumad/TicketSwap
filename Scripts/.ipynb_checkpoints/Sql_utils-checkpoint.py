import os
import datetime
from ssl import _PasswordType
from xmlrpc.client import _HostType
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
        try:
            engine.connect()
            return engine
        except:
            return None

    