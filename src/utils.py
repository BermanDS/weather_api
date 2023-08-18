##### This is the module of processing streaming data. #####################################################
# Author:        Anton Salyaev                                                                             #
# Email:         asalyaev@corp.finam.ru                                                                    #
# Description:   Necessary procedures and classes of Kafka connector and main object of data processing    #
############################################################################################################# 
import re
import gc
import os
import ast
import sys
import json
import pytz
import redis
import socket
import logging
import requests
import itertools
import traceback
import functools
import numpy as np
import pandas as pd
import psycopg2 as pg2
import psycopg2.extras as extras
from importlib import reload
from time import time, sleep
from dateutil.parser import parse
from datetime import datetime, timedelta, date, timezone


class NpEncoder(json.JSONEncoder):
    """ Custom encoder for numpy data types """
    
    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):

            return int(obj)

        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)

        elif isinstance(obj, (np.complex_, np.complex64, np.complex128)):
            return {'real': obj.real, 'imag': obj.imag}

        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()

        elif isinstance(obj, (np.bool_)):
            return bool(obj)

        elif isinstance(obj, (np.void)): 
            return None
        
        elif isinstance(obj, datetime):
            return obj.isoformat()
        
        return json.JSONEncoder.default(self, obj)

###################################################################################################
### Logging
###################################################################################################

log_levels = ["debug", "info", "warn", "error"]


def logger_init(location: str = '', log_path: str = os.getcwd(), log_level: str = 'info') -> object:

    reload(logging)
    location = location if location != '' else socket.gethostname()

    logging.basicConfig(
            filename = os.path.join(log_path, f'{location}.log'),
            level = logging.INFO if log_level == 'info' \
                    else logging.ERROR if log_level == 'error' \
                    else logging.WARN if log_level == 'warn' \
                    else logging.DEBUG,
        )
    return logging.getLogger(__name__)


def log(logger: object = None, tag: str = 'service', log_level: str = 'info', message: str = '', data: dict = {}) -> None:

    if log_levels.index(log_level) >= log_levels.index(log_level) and logger:
        log_info = {
            "level": log_level,
            "time": datetime.now(timezone.utc).isoformat(),
            "tag": tag,
            "message": message,
        }
        log_info.update(data)
            
        if log_level == 'info': logger.info(json.dumps(log_info, cls=NpEncoder))
        elif log_level == 'debug':logger.debug(json.dumps(log_info, cls=NpEncoder))
        elif log_level == 'warn': logger.warn(json.dumps(log_info, cls=NpEncoder))
        elif log_level == 'error':logger.error(json.dumps(log_info, cls=NpEncoder))


def flatten_list(xss: list = [[]]) -> list:
    """
    convert list of lists to flatten list
    """

    return [x for xs in xss for x in xs]

######################################################################################################
####### PostgreSQL connector 
######################################################################################################

class DBpostgreSQL:
    """
    PostgreSQL Database connector class.
    """

    def __init__(self, 
                db_name: str = '', 
                host: str = '', 
                username: str = '', 
                password: str = '', 
                port: int = 5432, 
                log_level: str = 'error',
                tz: str = '', 
                timeout: int = 0,
                logger: object = None):

        self.db = db_name
        self.timezone = tz
        self.df_result = pd.DataFrame()
        self.host = host
        self.timeout = timeout if timeout > 0 else 1000
        self.user = username
        self.password = password
        self.port = port
        self.query_insert_template = "INSERT INTO %s(%s) VALUES %%s"
        self.cur = None
        self.conn = None
        self.log_level = log_level
        self.log_levels = ["debug", "info", "warn", "error"]
        self.logger = logger


    def log(self, tag: str = 'postgresql-service', log_level: str = 'info', message: str = '', data: dict = {}) -> None:

        if self.log_levels.index(log_level) >= self.log_levels.index(self.log_level) and self.logger:
            #---------------------------------------------------------------------------------------
            log_info = {
                "level": log_level,
                "time": datetime.now(timezone.utc).isoformat(),
                "tag": tag,
                "message": message,
            }
            log_info.update(data)
            #------------------------------------------------------------------------------------------
            if log_level == 'info': self.logger.info(json.dumps(log_info, cls=NpEncoder))
            elif log_level == 'debug':self.logger.debug(json.dumps(log_info, cls=NpEncoder))
            elif log_level == 'warn': self.logger.warn(json.dumps(log_info, cls=NpEncoder))
            elif log_level == 'error':self.logger.error(json.dumps(log_info, cls=NpEncoder))
    

    def connect(self, first = True):
        """
        Connect to a Postgres database.
        """
        
        if self.conn is None:
            try:
                self.conn = pg2.connect(
                    dbname=self.db,
                    host=self.host, 
                    user=self.user, 
                    password=self.password, 
                    port=self.port
                )
            except (Exception, pg2.DatabaseError) as error:
                self.log('connection to DB', 'error', f"Connection to DB {self.db}: {error}")
                raise error
            finally:
                self.log('connection to DB', 'info', f"Connection to DB {self.db}: opened successfully!")
                self.cur = self.conn.cursor()
                if self.timezone:
                    self.cur.execute(f"SET timezone='{self.timezone}';")
                    self.conn.commit()
        else:
            self.cur = self.conn.cursor()
    

    def insert_values(self, df: pd.DataFrame = None, table: str = '', batch_size: int = 5000) -> bool:
        """
        Using psycopg2.extras.execute_values() to insert the dataframe
        """
    
        ## Create a list of tupples from the dataframe values ###################################################33
        tuples = list(df.itertuples(index=False, name=None))
        
        ## Comma-separated dataframe columns #######################################################################
        cols = ','.join(list(df))
        
        ## SQL query to execute #####################################################################################
        query  = self.query_insert_template % (table, cols)

        self.connect()
        len_df, down_df = df.shape[0], 0
        for i in range(np.int16(np.ceil(len_df/batch_size))):
            try:
                extras.execute_values(self.cur, query, tuples[batch_size * i : batch_size * (i + 1)])
                down_df += self.cur.rowcount
                self.conn.commit()
            except (Exception, pg2.DatabaseError) as error:
                self.log('insert to DB', 'error', f"Loading data to table {self.db}.{table} after {down_df} rows: {error}")
                self.conn.rollback()
                self.cur.close()
                self.close()
                return False
        self.cur.close()
        #-----------------------------------------------------------------------------------------------------------------
        if down_df < len_df:
            self.log('insert to DB', 'error', f"Data to table {self.db}.{table}: uploaded JUST {down_df} from {len_df}.")
        else:
            self.log('insert to DB', 'info', f"Data to table {self.db}.{table}: uploaded {down_df} out of {len_df} successfully!")
        #------------------------------------------------------------------------------------------------------------------
        self.close()
        return True


    def update_values(self, df: pd.DataFrame = None, table: str = '', feat_pk: str = None, batch_size: int = 5000) -> bool:
        """
        Using psycopg2.extras.execute_values() to insert the dataframe
        feat_pk - Primary key
        """

        if feat_pk is None:
            self.log('update table in DB', 'error', f"Need define PK for updating {self.db}.{table}")
                        
            return False
        
        # Create a list of tupples from the dataframe values ##############################################################
        tuples = list(df.itertuples(index=False, name=None))
        
        # Comma-separated dataframe columns ###############################################################################
        full_cols = list(df)
        cols_no_pk = [i for i in df.columns if i not in feat_pk]

        # SQL query to execute #############################################################################################
        query  = f"""INSERT INTO {table} ({", ".join(full_cols)}) 
                     VALUES %s ON CONFLICT ({feat_pk}) 
                     DO UPDATE SET {", ".join([f"{l} = excluded.{l}" for l in cols_no_pk])};"""
        
        self.connect()
        len_df, down_df = df.shape[0], 0
        for i in range(np.int16(np.ceil(len_df/batch_size))):
            try:
                extras.execute_values(self.cur, query, tuples[batch_size * i : batch_size * (i + 1)])#, template =f"({', '.join(['%s' for i in full_cols])})")
                down_df += self.cur.rowcount
                self.conn.commit()
            except (Exception, pg2.DatabaseError) as error:
                self.log('update table in DB', 'error', f"Updating table {self.db}.{table} after {down_df} rows: {error}")
                self.conn.rollback()
                self.cur.close()
                self.close()
                return False
            except (Exception, pg2.InvalidColumnReference) as error:
                self.log('update table in DB', 'error', f"Invalid specification for table {self.db}.{table} after {down_df} rows: {error}")
                self.conn.rollback()
                self.cur.close()
                self.close()
                return False
            except:
                self.log('update table in DB', 'error', f"Other error for table {self.db}.{table} after {down_df} rows")
                self.conn.rollback()
                self.cur.close()
                self.close()
                return False
        self.cur.close()
        #-----------------------------------------------------------------------------------------------------------------
        if down_df == 0:
            self.log('update table in DB', 'warning', f"Data in table {self.db}.{table}: updated JUST {len_df} rows.")
        else:
            self.log('update table in DB', 'info', f"Data in table {self.db}.{table}: updated {down_df} out of {len_df} successfully!")
        #------------------------------------------------------------------------------------------------------------------
        self.close()
        return True


    def update_set(self, table: str, value: dict = {}, condition: dict = {}) -> (bool, int):
        """
        procedure of simple query
        if you need to use as value of definite atribute the string or date as example : 
        ... where id = 'dfgter4DF$%^' and ...
        you should to use additional quotes as example : 
        {'id': "'dfgter4DF$%^'"}
        It's important
        """
        
        if value == {}:
            self.log('update table in DB', 'error', f"Need define value set for updating {self.db}.{table}")
            
            return False, 0
        #------------------------------------------------------------------------------------------------------
        elif condition == {}:
            query = f"""UPDATE {table} SET {' AND '.join([f"{k} = {v}" for k,v  in value.items()])};"""
        else:
            query = f"""UPDATE {table} SET {' AND '.join([f"{k} = {v}" for k,v  in value.items()])}
                        WHERE {' AND '.join([f"{k} = {v}" for k,v  in condition.items()])};"""
        #--------------------------------------------------------------------------------------------------------
        
        self.connect(False)
        try:
            self.cur.execute(query)
            affected_rows = self.cur.rowcount
            self.conn.commit()
            self.cur.close()
            self.close()
            if affected_rows > 0:
                return True, affected_rows
            else:
                return False, affected_rows
        except (Exception, pg2.ProgrammingError) as error:
            self.log('update table in DB', 'error', f"Executing query to DB {self.db}: {error}")
            self.conn.commit()
            self.cur.close()
            self.close()
            return False, 0
            

    def delete_from(self, table: str, condition: dict = {}) -> (bool, int):
        """
        procedure of simple query
        if you need to use as value of definite atribute the string or date as example : 
        ... where id = 'dfgter4DF$%^' and ...
        you should to use additional quotes as example : 
        {'id': "'dfgter4DF$%^'"}
        It's important
        """

        if condition == {}:
            self.log('delete from table in DB', 'error', f"Need define condition for deleting {self.db}.{table}")
            
            return False, 0
        
        query = f"""DELETE FROM {table} WHERE {' AND '.join([f"{k} = {v}" for k,v  in condition.items()])};"""
        #-----------------------------------------------------------------------------------------------------------

        self.connect(False)
        try:
            self.cur.execute(query)
            affected_rows = self.cur.rowcount
            self.conn.commit()
            self.cur.close()
            self.close()
            if affected_rows > 0:
                return True, affected_rows
            else:
                return False, affected_rows
        except (Exception, pg2.ProgrammingError) as error:
            self.log('delete from table in DB', 'error', f"Executing query to DB {self.db}: {error}")
            self.conn.commit()
            self.cur.close()
            self.close()
            return False, 0
    

    def select_rows(self, query: str = '', condition: dict = {}) -> (list, bool):
        """
        Run a SQL query to select rows from table.
        """
        
        if condition != {}:
            where_value = ' AND '.join([f"{x[0]} = " + json.dumps(x[1], cls= NpEncoder).replace('"',"'") for x in condition.items()])
            query += f""" WHERE {where_value};"""
            
        self.connect(False)
        with self.cur as cur:
            try:
                cur.execute(query)
                records = [row for row in cur.fetchall()]
                self.conn.commit()
                cur.close()
                self.close()
                return records, True
            except TimeoutError:
                self.log('query to DB', 'error', f"timeout occured during execution query to DB {self.db}")
                self.close()
            except Exception as error:
                self.log('query to DB', 'error', f"selecting query to DB {self.db}: {error}")
                self.close()
        
        return [], False
    

    def query_to_df(self, query: str = '') -> None:
        """
        Run a SQL query and connection for transform to pandas DF.
        """
        
        self.connect()
        self.df_result = pd.DataFrame()
        try:
            self.df_result = pd.read_sql_query(query, self.conn)
        except TimeoutError:
            self.log('query to DB', 'error', f"timeout occured during pandas query to DB {self.db}")
            self.conn.rollback()
        except Exception as error:
            self.log('query to DB', 'error', f"pandas query to DB {self.db}: {error}")
            self.conn.rollback()
        
        self.close()


    def execute_query(self, query: str = '') -> bool:
        """
        procedure of simple query
        """
        
        self.connect(False)
        with self.cur as cur:
            try:
                cur.execute(query)
                cur.fetchall()
                self.conn.commit()
                cur.close()
                self.close()
                return True
            except pg2.ProgrammingError as error:
                if 'delete' not in query.lower():
                    self.log('query to DB', 'error', f"executing query to DB {self.db}: {error}")
                self.conn.commit()
                cur.close()
                self.close()
                return True
            except:
                cur.close()
                self.close()
                return False
            
    
    def insert_if_not_exist(self, table: str = '', values: dict = {}, feat_pk: str = 'id') -> (bool, int):
        """
        procedure of simple query
        if you have autoincremental id and you need to insert values to table 
        """
        
        if values == {}:
            self.log('insert to DB', 'error', f"Need define values set for updating {self.db}.{table}")
                        
            return False, 0
        #----------------------------------------------------------------------------------------------------------------
        else:
            tuple_value = list(pd.DataFrame(data = [values.values()], columns = values.keys()).itertuples(index=False, name=None))
            where_value = ' AND '.join([f"{x[0]} = " + json.dumps(x[1], cls= NpEncoder).replace('"',"'") for x in values.items()])
            query = f"""INSERT INTO {table}
                        ({','.join(values.keys())})
                        SELECT {str(tuple_value[0])[1:-1]}
                        WHERE NOT EXISTS (
                            SELECT {feat_pk} 
                            FROM {table} 
                            WHERE {where_value});"""
        #----------------------------------------------------------------------------------------------------------
        self.connect(False)
        
        try:
            self.cur.execute(query)
            affected_rows = self.cur.rowcount
            self.conn.commit()
            self.cur.close()
            self.close()
            return True, affected_rows
        except TimeoutError:
            self.log('query to DB', 'error', f"timeout occured during pandas query to DB {self.db}")
            self.cur.close()
            self.close()
            return False, 0
        except (Exception, pg2.ProgrammingError) as error:
            self.log('query to DB', 'error', f"executing query to DB {self.db}: {error}")
            self.conn.commit()
            self.cur.close()
            self.close()
            return False, 0
        except:
            self.cur.close()
            self.close()
            return False, 0
    
    
    def close(self):

        if self.conn:
            try:
                self.cur.close()
            except:
                None
            self.conn.close()
            self.conn = None

###################################################################################################
###### REDIS connector ############################################################################
###################################################################################################


class DBredis:
    """
    Redis message Broker class.
    """

    def __init__(self, 
                topic: int = 0, 
                host: str = 'localhost', 
                tz: str = 'Europe/Moscow', 
                username: str = '',
                password: str = '', 
                retention_ms: int = 172800, 
                log_level: str = 'error',
                port: int = 6379, 
                headers: dict = {}):

        self.topic = topic
        self.timezone = tz
        self.headers = headers
        self.host = host
        self.retention_period = retention_ms
        self.user = username
        self.password = password
        self.port = port
        self.dc_result = {}
        self.df_result = pd.DataFrame()
        self.pref_msg = ''
        self.format_dt = '%Y-%m-%d %H:%M:%S'
        self.log_level = log_level
        self.log_levels = ["debug", "info", "warn", "error"]
        self.logger = None
        self.conn = None

    
    def log(self, tag: str = 'redis-service', log_level: str = 'info', message: str = '', data: dict = {}):
        """
        logging with custom format:
        tag - some tag of action
        message - message for logging
        data - some key-value pairs for additional data of logging
        """

        if self.log_levels.index(log_level) >= self.log_levels.index(self.log_level) and self.logger:
            log_info = {
                "level": log_level,
                "time": datetime.now(timezone.utc).isoformat(),
                "tag": tag,
                "message": message,
            }
            log_info.update(data)
            
            if log_level == 'info': self.logger.info(json.dumps(log_info, cls=NpEncoder))
            elif log_level == 'debug':self.logger.debug(json.dumps(log_info, cls=NpEncoder))
            elif log_level == 'warn': self.logger.warning(json.dumps(log_info, cls=NpEncoder))
            elif log_level == 'error':self.logger.error(json.dumps(log_info, cls=NpEncoder))


    def connect_broker(self):
        """
        Connect to a Kafka broker as Producer.
        """
                
        if self.conn is None and not all([self.user, self.password]):
            try:
                self.conn = redis.StrictRedis(
                                    host=self.host,
                                    port=self.port,
                                    db=self.topic,
                                    decode_responses=True,
                                    charset='utf-8',
                                    password=self.password,
                                    )
                if self.timezone:
                    self.conn.set('timezone', self.timezone)
                if self.headers != {}:
                    for k,v in self.headers.items():
                        _ = self.conn.set(k, self.change_format_to_str(v))
                    
            except Exception as error:
                self.log('connection to broker', 'error', f"{self.pref_msg}Connection to Broker {self.host}: {error}")
        
    
    def change_format_to_str(self, value: object = None) -> object:
        """
        decoding values
        """

        if isinstance(value, bool):
            value = 1 if value else 0
        elif isinstance(value, bytes):
            value = value.decode()
        elif value is None:
            value = '0'
        elif isinstance(value, (list, dict)):
            value = json.dumps(value, cls = NpEncoder)

        return value


    def publish_message(self, key, value) -> bool:
        """
        publishing data to Broker
        """

        load = False
        self.connect_broker()
        if self.conn:
            try:
                load = self.conn.setex(key, self.retention_period, self.change_format_to_str(value))
            except Exception as error:
                self.log('publishing to broker', 'error', f"Publishing message to topic {self.topic}: {error}")
        #-------------------------------------------------------------------------------------------------------
        self.close()

        return load


    def reading_message(self, key, temp_dc = {}) -> dict:
        """
        messages from que for transform to dict
        """

        self.connect_broker()
        if temp_dc == {}:
            self.dc_result = {key:None}
        elif isinstance(temp_dc, dict):
            self.dc_result = temp_dc
            self.dc_result[key] = None
        else:
            self.dc_result = {key:None}
        #------------------------------------------------------------------------------------------------------------
        if self.conn:
            try:
                self.dc_result[key] = self.change_format_to_str(self.conn.get(key))
            except Exception as error:
                self.log('consuming from broker', 'error', f"reading message ({key}) from topic {self.topic}: {error}")
        #-------------------------------------------------------------------------------------------------------------        
        self.close()

        return self.dc_result

    
    def close(self):

        if self.conn:
            self.conn.close()
            self.conn = None
