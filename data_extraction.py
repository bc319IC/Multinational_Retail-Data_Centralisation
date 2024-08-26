import pandas as pd
import tabula
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from io import StringIO


class DataExtractor():

    def __init__(self):
        self.headers = {'x-api-key': "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}

    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine()
        df = pd.read_sql_table(table_name, engine)
        return df
    
    def retrieve_pdf_data(self, pdf_link):
        # Extract the PDF data into a list of DataFrames, one per page
        dfs = tabula.read_pdf(pdf_link, pages='all', multiple_tables=True)
        # Combine all the DataFrames into a single DataFrame
        df = pd.concat(dfs, ignore_index=True)
        return df
    
    def list_number_of_stores(self, endpoint):
        response = requests.get(endpoint, headers=self.headers)
        if response.status_code == 200:
            return response.json().get('number_stores')
        else:
            print(f"Failed to retrieve number of stores. Status code: {response.status_code}")
            return None
        
    def get_store_data(self, url):
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error retrieving store data from {url}: {e}")
            return None

    def retrieve_stores_data(self, store_endpoint, number_of_stores):
        stores_data = []
        urls = [store_endpoint.format(store_number=i) for i in range(1, number_of_stores + 1)]
        with ThreadPoolExecutor(max_workers=10) as executor:  # Adjust max_workers based on your needs
            future_to_url = {executor.submit(self.get_store_data, url): url for url in urls}
            for future in as_completed(future_to_url):
                store_data = future.result()
                if store_data:
                    stores_data.append(store_data)
        if stores_data:
            df_stores = pd.DataFrame(stores_data)
            return df_stores
        else:
            print("No store data retrieved.")
            return None
        
    def extract_from_s3(self, s3_address):
        # Extract bucket name and file key from the S3 address
        bucket_name = s3_address.split('/')[2]
        file_key = '/'.join(s3_address.split('/')[3:])
        # Initialize the S3 client
        s3_client = boto3.client('s3')
        # Get the CSV file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        csv_content = response['Body'].read().decode('utf-8')
        # Load the CSV content into a pandas DataFrame
        df = pd.read_csv(StringIO(csv_content))
        return df