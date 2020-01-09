import pandas as pd
import psycopg2
import os

class RedshiftQueryRunner():

    def __init__(self):
        self.connection = self.get_connection()

    def __del__(self):
        self.connection.close()

    def get_connection_string(self):
        """
        Checks if REDSHIFT_PASSWORD and REDSHIFT_USERNAME are in .bash_probile,
        if not asks to manually input username and password

        Returns: connection string to our redshift instance
        """
        pw = os.environ.get('REDSHIFT_PASSWORD')
        un = os.environ.get('REDSHIFT_USERNAME')

        if not (pw and un):
            print('Could not find password in enviroment')
            pw = input('Enter Redshift password: ')
            un = input('Enter Redshift user name')

        conn_string = """dbname='dwh' port='5439' user='{}' password='{}' \
                    host='blink-dwh.c50aousolcxo.us-east-1.redshift.amazonaws.com' \
                    """.format(un, pw)

        return conn_string

    def get_connection(self):
        """
        Attampets to connect to our redshift intance

        Returns: pyscopg2 connection if successfull else None
        """
        try:
            con = psycopg2.connect(self.get_connection_string())
            print('Connected to Redshift!')
            return con
        except:
            print('Coult not connect to Redshift')
            return None

    def get_data_frame_from_query(self, query, params=None):
        """
        Generates a Pandas DataFrame from a query

        Args:
            query: a SQL query in string format
            params: a set of params to inject into the SQL

        Returns: DataFrame of query results
        """
        return pd.read_sql(query, self.connection, params=params)
    
    def get_data_frame_from_sql(self, sql_file, params=None):
        """
        Generates a Pandas DataFrame from a SQL file

        Args:
            file: a SQL file, should return results
            params: a set of params to inject into the SQL

        Returns: DataFrame of SQL results
        """
        with open(sql_file, 'r') as f:
            query = f.read()
        return pd.read_sql(query, self.connection, params=params)