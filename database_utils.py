import yaml
from sqlalchemy import create_engine, inspect


class DatabaseConnector():

    @staticmethod
    def read_db_creds():
        '''
        Saves the credentials from a yaml file as a dictionary.

        Parameters
        ----------
        None

        Returns
        -------
        creds
        '''
        file_path = "db_creds.yaml"
        try:
            with open(file_path, 'r') as file:
                creds = yaml.safe_load(file)
                return creds
        except FileNotFoundError:
            print(f"Error: File '{file_path}' not found.")
            return None
        except yaml.YAMLError as e:
            print(f"Error loading YAML file: {e}")
            return None
        
    def init_db_engine(self):
        '''
        Initialises the SQLAlechemy engine.

        Parameters
        ----------
        None

        Returns
        -------
        engine
        '''
        # Construct a database URL for SQLAlchemy
        creds = self.read_db_creds()
        db_url = f"{'postgresql'}://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        # Create SQLAlchemy engine
        engine = create_engine(db_url)
        return engine

    def list_db_tables(self):
        '''
        Lists the tables retrieved using the SQLAlechemy engine.

        Parameters
        ----------
        None

        Returns
        -------
        tables
        '''
        engine = self.init_db_engine()
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables
    
    def upload_to_db(self, df, table_name, local_db_engine, dtype_dic):
        '''
        Uploads the dataframe to the local postgres server.

        Parameters
        ----------
        table_name - name of the table to be uploaded.
        local_db_engine - the SQLAlechemy engine to be used.
        dtype=dtype_dic - a dictionary of the column types of the dataframe when converted to SQL.

        Returns
        -------
        tables
        '''
        df.to_sql(table_name, local_db_engine, if_exists='replace', index=False, dtype=dtype_dic)