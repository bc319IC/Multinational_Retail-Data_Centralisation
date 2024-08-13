import pandas as pd

class DataExtractor():

    def __init__(self, db_connector, table_name):
        self.db_connector = db_connector
        self.table_name = table_name

    def read_rds_table(self):
        engine = self.db_connector.init_db_engine()
        df = pd.read_sql_table(self.table_name, engine)
        return df