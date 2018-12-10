import os
import psycopg2
import pandas as pd


class Database():
    def __init__(self, db_prefix):
        self.db_prefix = db_prefix
        self._init_connection()

    @staticmethod
    def _load_env(variable_string):
        try:
            return os.getenvb(key=str.encode(variable_string)).decode()
        except Exception:
            raise Exception('Unable to decode "{}" variable!'.format(variable_string))

    def _init_connection(self):
        database = self._load_env('DATABASE_{}_NAME'.format(self.db_prefix))
        username = self._load_env('DATABASE_{}_USERNAME'.format(self.db_prefix))
        password = self._load_env('DATABASE_{}_PASSWORD'.format(self.db_prefix))
        hostname = self._load_env('DATABASE_{}_HOST'.format(self.db_prefix))
        port = self._load_env('DATABASE_{}_PORT'.format(self.db_prefix))
        try:
            self.con = psycopg2.connect(
                "dbname = %s user = %s password = %s host = %s port = %s" %
                (database, username, password, hostname, port))
        except psycopg2.OperationalError:
            raise EnvironmentError(
                "Password authentication failed for user {}".format(username))

    def disconnect(self):
        self.con.close()

    def run_query(self, query):
        df = pd.read_sql(query, con=self.con)
        return df
