import pandas as pd
import tabula
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from io import StringIO, BytesIO
import time


class DataExtractor():

    def __init__(self):
        '''
        Initialises the DataExtractor class with the api key.

        Parameters
        ----------
        None

        Returns
        -------
        None
        '''
        self.headers = {'x-api-key': "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}

    def read_rds_table(self, db_connector, table_name):
        '''
        Reads the specified table and returns as a pandas dataframe.

        Parameters
        ----------
        db_connector - an instance of the DatabaseConnector class.
        table_name - name of the table to be read.

        Returns
        -------
        df
        '''
        engine = db_connector.init_db_engine()
        df = pd.read_sql_table(table_name, engine)
        return df
    
    def retrieve_pdf_data(self, pdf_link):
        '''
        Retrieves the dataframe from the specified pdf url and returns as a pandas dataframe.

        Parameters
        ----------
        pdf_link

        Returns
        -------
        df
        '''
        # Extract the pdf data into a list of dataframes, one per page
        dfs = tabula.read_pdf(pdf_link, pages='all', multiple_tables=True)
        # Combine all the dataframes into a single dataframe
        df = pd.concat(dfs, ignore_index=True)
        return df
    
    def list_number_of_stores(self, endpoint):
        '''
        Returns the number of stores using the specified endpoint url.

        Parameters
        ----------
        endpoint

        Returns
        -------
        number_of_stores
        '''
        response = requests.get(endpoint, headers=self.headers)
        if response.status_code == 200:
            number_of_stores = response.json().get('number_stores')
            return number_of_stores
        else:
            print(f"Failed to retrieve number of stores. Status code: {response.status_code}")
            return None
        
    def get_store_data(self, url, retries=3, delay=2):
        '''
        A helper function for the retrieve_stores_data function. Makes a GET request for the json data.

        Parameters
        ----------
        url, retries=3, delay=2

        Returns
        -------
        response.json()
        '''
        for attempt in range(retries):
            try:
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                print(f"Error retrieving store data from {url}: {e}")
                if response is not None:
                    print(f"Response status code: {response.status_code}")
                    print(f"Response text: {response.text}")
                if attempt < retries - 1:
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
                else:
                    return None

    def retrieve_stores_data(self, store_endpoint, number_of_stores):
        '''
        Retrieves the stores dataframe using the url for the store details endpoint.

        Parameters
        ----------
        store_endpoint, number_of_stores

        Returns
        -------
        df
        '''
        stores_data = []
        # Loop through each store
        urls = [store_endpoint.format(store_number=i) for i in range(0, number_of_stores)]
        # Speed up process with multi threading
        with ThreadPoolExecutor(max_workers=10) as executor:  # Adjust max_workers
            future_to_url = {executor.submit(self.get_store_data, url): url for url in urls}
            # Retain order of completion not submition
            for future in as_completed(future_to_url):
                store_data = future.result()
                if store_data:
                    stores_data.append(store_data)
        # Return df if not empty
        if stores_data:
            df = pd.DataFrame(stores_data)
            return df
        else:
            print("No store data retrieved.")
            return None
        
    def extract_csv_from_s3(self, s3_address):
        '''
        Extracts the contents from a csv file at the specified s3 address into a pandas dataframe.

        Parameters
        ----------
        s3_address

        Returns
        -------
        df
        '''
        # Extract bucket name and file key from the s3 address
        bucket_name = s3_address.split('/')[2]
        file_key = '/'.join(s3_address.split('/')[3:])
        # Initialize the s3 client
        s3_client = boto3.client('s3')
        # Get the csv file from s3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        csv_content = response['Body'].read().decode('utf-8')
        # Load the csv content into a pandas dataframe
        df = pd.read_csv(StringIO(csv_content))
        return df
    
    def retrieve_json_data(self, json_link):
        '''
        Retrieves the dataframe from a json link.

        Parameters
        ----------
        json_link

        Returns
        -------
        df
        '''
        # Send a GET request to the JSON file URL
        response = requests.get(json_link)
        # Convert bytes data to a file-like object
        json_data = BytesIO(response.content)
        # Load JSON data into a pandas DataFrame
        df = pd.read_json(json_data)
        return df