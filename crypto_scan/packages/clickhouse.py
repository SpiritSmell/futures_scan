import clickhouse_connect
import pandas as pd
from time import time
import json
from datetime import datetime

DEBUG = False
HOST = "clickhouse-dev.eurasia.kz"
PORT = 8123
USERNAME = "security"
PASSWORD = "V4qhK4jxUV@$B"
DATABASE_NAME = "raw"
TABLE_NAME = "test7"
CONNECTION_STRING = f"http://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/"
TIMESTAMP_FIELD = '__metatimestamp_timestamp'

import clickhouse_connect
import json
import logging
from time import time
from datetime import datetime

TIMESTAMP_FIELD = "timestamp"


class ClickhouseConnector:
    def __init__(self, connection_string, database_name, table_name=None, json_as_string=False):
        self.connection_string = connection_string
        self.database_name = database_name
        self.table_name = table_name
        self.json_as_string = json_as_string
        self.client = self._get_client()

    def _get_client(self):
        try:
            return clickhouse_connect.get_client(dsn=self.connection_string, database=self.database_name)
        except Exception as e:
            logging.error(f"Error connecting to Clickhouse: {e}")
            return None

    def _ensure_connection(self):
        if self.client is None or not self.client.ping():
            logging.warning("Reconnecting to Clickhouse...")
            self.client = self._get_client()

    def add_timestamp(self, records, start_time):
        for record in records:
            record[TIMESTAMP_FIELD] = start_time
        return records

    def save_data(self, data, timestamp=None, replace=False, timeseries=True):
        self._ensure_connection()
        if len(data) > 25_000:
            raise ValueError(f"Too many records: {len(data)}")
        if timestamp is None:
            logging.warning("Timestamp is None! Setting to current time.")
            timestamp = time()

        try:
            self.client.command("SET allow_experimental_object_type=1")
            if timeseries:
                data = self.add_timestamp(data, timestamp)
            if replace:
                self.client.command(f"TRUNCATE TABLE IF EXISTS {self.database_name}.{self.table_name}")

            self.client.command(
                f"CREATE TABLE IF NOT EXISTS {self.database_name}.{self.table_name} (data JSON) ENGINE = Memory")
            json_values = ", ".join(f"('{json.dumps(item)}')" for item in data)
            self.client.command(f"INSERT INTO {self.database_name}.{self.table_name} VALUES {json_values}")
        except Exception as e:
            logging.error(f"Error saving data to Clickhouse: {e}")

    def save_dataframe(self, data, timestamp=None, replace=False, timeseries=True):
        self._ensure_connection()
        if len(data) > 10_000:
            raise ValueError(f"Too many records: {len(data)}")
        if timestamp is None:
            logging.warning("Timestamp is None! Setting to current time.")
            timestamp = datetime.now()
        data[TIMESTAMP_FIELD] = timestamp

        try:
            self.client.command("SET allow_experimental_object_type=1")
            table_name = f'{self.database_name}."{self.table_name}"'
            if replace:
                self.client.command(f'TRUNCATE TABLE IF EXISTS {table_name}')
            self.client.insert_df(table_name, data)
        except Exception as e:
            logging.error(f"An error occurred: {e}")

    def add_record(self, data, table_name=None):
        self._ensure_connection()
        table_name = table_name or self.table_name
        try:
            self.client.insert_df(table=table_name, df=data)
        except Exception as e:
            logging.error(f"Error saving data to Clickhouse: {e}")
            return False
        return True

    def clean_table(self, table_name=None):
        self._ensure_connection()
        table_name = table_name or self.table_name
        try:
            self.client.command(f"TRUNCATE TABLE {self.database_name}.{table_name}")
        except Exception as e:
            logging.error(f"Error truncating table: {e}")

    def delete_record(self, condition, table_name=None):
        self._ensure_connection()
        table_name = table_name or self.table_name
        try:
            self.client.command(f"DELETE FROM {self.database_name}.{table_name} WHERE {condition}")
        except Exception as e:
            logging.error(f"Error deleting data from Clickhouse: {e}")

    def manage_role(self, action, role_name, rights=None, scope=None, source_role=None):
        self._ensure_connection()
        try:
            if action == "create":
                query = f"CREATE ROLE IF NOT EXISTS {role_name}"
            elif action == "grant":
                query = f"GRANT {rights} ON {scope} TO {role_name}"
            elif action == "assign":
                query = f"GRANT {source_role} TO {role_name}"
            elif action == "delete":
                query = f"DROP ROLE IF EXISTS {role_name}"
            else:
                raise ValueError("Invalid action for role management")
            self.client.command(query)
            return True
        except Exception as e:
            logging.error(f"Error running query in Clickhouse: {e}")
            return False

    def read_data(self, table_name=None):
        self._ensure_connection()
        table_name = table_name or self.table_name
        try:
            return self.client.query_df(f"SELECT * FROM {self.database_name}.{table_name}")
        except Exception as e:
            logging.error(f"Error reading data from Clickhouse: {e}")
            return None

    def query(self, query):
        self._ensure_connection()
        if not query:
            return None
        try:
            return self.client.query_df(query)
        except Exception as e:
            logging.error(f"Error executing query in Clickhouse: {e}")
            return None




def test_add_record():
    HOST = "clickhouse-dev.eurasia.kz"
    PORT = 8123
    USERNAME = "trino"
    PASSWORD = "Trin0P@$$w0rd"
    DATABASE_NAME = "security"
    TABLE_NAME = "clickhouse_ref_clickhouseadrolesrights__v1_1_view_latest"
    CONNECTION_STRING = f"http://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/"
    TIMESTAMP_FIELD = '__metatimestamp_timestamp'
    AD_ROLES_TABLE_NAME = "clickhouse_ref_adroles__v1_1"
    CLICKHOUSE_ROLES_TABLE_NAME = "clickhouse_ref_clickhouseroles__v1_1"
    CLICKHOUSE_RIGHTS_TABLE_NAME = "clickhouse_ref_clickhouserights__v1_1"
    clickhouse_connector = ClickhouseConnector(connection_string=CONNECTION_STRING, database_name=DATABASE_NAME,
                                               table_name=TABLE_NAME)

    df = clickhouse_connector.read_data(CLICKHOUSE_RIGHTS_TABLE_NAME)
    print(df)
    clickhouse_rights_df_subset = pd.DataFrame(df, columns=['right_name', 'description', 'command'])
    df_to_add = pd.DataFrame()
    print(clickhouse_rights_df_subset)
    #clickhouse_connector.add_record(clickhouse_rights_df_subset, CLICKHOUSE_RIGHTS_TABLE_NAME)


def main():
    clickhouse_connector = ClickhouseConnector(connection_string=CONNECTION_STRING, database_name=DATABASE_NAME,
                                               table_name=TABLE_NAME)

    item = {
        "a": 1,
        "b": {
            "c": 2,
            "d": [1, 2, 3, 4, 5, 6, 7],
            "e": "test"}
    }

    data = []
    for i in range(250):
        data.append(item)
    clickhouse_connector.save_data(data, replace=True)

    df = clickhouse_connector.read_data()

    print(df)


if __name__ == "__main__":
    test_add_record()
    #main()
