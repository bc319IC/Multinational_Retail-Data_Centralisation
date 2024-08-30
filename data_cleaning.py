import pandas as pd
import re
import numpy as np


class DataCleaning():
    
    def clean_user_data(self, df):
        '''
        Cleans the user data.

        Parameters
        ----------
        df

        Returns
        -------
        df
        '''
        df = df.copy()

        # Drop column
        df = df.drop(columns=['index'])
        
        # Check email format
        email_pattern = r'^[\w\.\+\-]+@[a-zA-Z0-9\-]+\.[a-zA-Z0-9\-.]+$'
        # Fix repeated @ symbols
        df['fixed_email_address'] = df['email_address'].str.replace(r'@@', '@', regex=True)
        # Validate emails
        df['valid_email_address'] = df['fixed_email_address'].str.match(email_pattern)
        # Drop rows with invalid emails
        df = df[df['valid_email_address']].drop(columns=['valid_email_address'])
        
        # Remove invalid countries
        valid_countries = ['United Kingdom', 'Germany', 'United States']
        df = df[df['country'].isin(valid_countries)]
        
        # Check correct country code
        country_to_code = {
            'United Kingdom': 'GB',
            'United States': 'US',
            'Germany': 'DE'
        }
        # Check if the country code matches the expected code from the dictionary
        df['correct_country_code'] = df['country'].map(country_to_code)
        df['is_match'] = df['country_code'] == df['correct_country_code']
        # Correct mismatches
        df.loc[~df['is_match'], 'country_code'] = df['correct_country_code']
        df = df.drop(columns=['correct_country_code', 'is_match'])

        # Check phone number format
        df['phone_number'] = df['phone_number'].str.replace(r'\+44\(0\)|\+49\(0\)|\+44|\+49|\(0\)', '0', regex=True)
        df['phone_number'] = df['phone_number'].apply(lambda phone: re.sub(r'\D', '', phone))
        df['phone_number'] = df['phone_number'].apply(lambda phone: phone[:5] + ' ' + phone[5:] if len(phone) > 5 else phone)
        
        return df
    

    def clean_card_data(self, df):
        '''
        Cleans the card data.

        Parameters
        ----------
        df

        Returns
        -------
        df
        '''
        df = df.copy()

        # Check card number contains only digits
        df['card_number'] = df['card_number'].astype(str).str.replace(r'[^0-9]', '', regex=True)

        # Remove invalid expiry dates
        expiry_pattern = r'^\d{2}/\d{2}$'
        valid_expiry_mask = df['expiry_date'].str.match(expiry_pattern, na=False)
        df = df[valid_expiry_mask]
        
        return df
    

    def clean_store_data(self, df):
        '''
        Cleans the store data.

        Parameters
        ----------
        df

        Returns
        -------
        df
        '''
        df = df.copy()

        # Drop columns
        df = df.drop(columns=['index', 'lat'])

        # Check staff numbers only consists of numbers
        df['staff_numbers'] = df['staff_numbers'].astype(str).str.replace(r'[^0-9]', '', regex=True)

        # Remove entries with numbers in the 'locality' column
        df = df[~df['locality'].str.contains(r'\d', regex=True)]

        # Remove invalid store types
        valid_store_type = ['Super Store', 'Local', 'Outlet', 'Mall Kiosk', 'Web Portal']
        df = df[df['store_type'].isin(valid_store_type)]

        # Remove invalid country codes
        valid_store_type = ['US', 'GB', 'DE']
        df = df[df['country_code'].isin(valid_store_type)]

        # Check correct continent
        code_to_continent = {
            'US': 'America',
            'GB': 'Europe',
            'DE': 'Europe'
        }
        # Check if the continent matches the expected continent from the dictionary
        df['correct_continent'] = df['country_code'].map(code_to_continent)
        df['is_match'] = df['continent'] == df['correct_continent']
        # Correct mismatches
        df.loc[~df['is_match'], 'continent'] = df['correct_continent']
        df = df.drop(columns=['correct_continent', 'is_match'])

        # Replace N/A with nulls
        df.replace('N/A', np.nan, inplace=True)
        
        return df
    

    def convert_product_weights(self, df):
        '''
        Converts the weights in the specified dataframe to kg.

        Parameters
        ----------
        df

        Returns
        -------
        df
        '''
        def convert_weight(weight):
            '''
            Helper function for convert_product_weights function. Converts the values to kg.

            Parameters
            ----------
            weight

            Returns
            -------
            weight - in kg
            '''
            # Convert to lowercase and remove spaces
            weight = str(weight).lower().replace(" ", "")
            # Handle different unit conversions
            if 'kg' in weight:
                return float(re.sub(r'[^\d.]+', '', weight))
            elif 'g' in weight:
                return float(re.sub(r'[^\d.]+', '', weight)) / 1000
            elif 'ml' in weight:
                return float(re.sub(r'[^\d.]+', '', weight)) / 1000
            elif 'l' in weight:
                return float(re.sub(r'[^\d.]+', '', weight))
            elif 'oz' in weight:
                return float(re.sub(r'[^\d.]+', '', weight)) * 0.0283495
            else:
                return None
        # Apply the conversion to the weight column
        df['weight'] = df['weight'].apply(convert_weight)
        return df
    

    def clean_product_data(self, df):
        '''
        Cleans the product data.

        Parameters
        ----------
        df

        Returns
        -------
        df
        '''
        df = df.copy()

        # Drop column
        df = df.drop(columns=['Unnamed: 0'])

        # Drop rows with null values
        df.dropna(inplace=True)

        # Change column names to lower case
        df.columns = [col.lower() for col in df.columns]

        # Remove invalid category
        valid_category = ['diy', 'health-and-beauty', 'pets', 'food-and-drink', 'sports-and-leisure', 'homeware', 'toys-and-games']
        df = df[df['category'].isin(valid_category)]

        # Remove invalid still available and convert to bool
        valid_removed = ['Removed', 'Still_avaliable']
        df = df[df['removed'].isin(valid_removed)]
        df['removed'] = df['removed'].str.replace('Still_avaliable', 'TRUE')
        df['removed'] = df['removed'].str.replace('Removed', 'FALSE')
        df['removed'] = df['removed'].astype(bool)
        df = df.rename(columns={'removed': 'still_available'})

        # Check price format
        df['product_price'] = df['product_price'].astype(str)
        df['numeric_part'] = df['product_price'].str.extract(r'(\d+\.\d{2})')
        df['product_price'] = df['numeric_part']
        df.drop(columns=['numeric_part'], inplace=True)

        # Check no extra nulls have been produced
        df.dropna(inplace=True)

        # Add weight class column
        # Define conditions and choices for weight_class
        conditions = [
            (df['weight'] < 2),
            (df['weight'] >= 2) & (df['weight'] < 40),
            (df['weight'] >= 40) & (df['weight'] < 140),
            (df['weight'] >= 140)
        ]
        choices = [
            'Light',
            'Mid_Sized',
            'Heavy',
            'Truck_Required'
        ]
        df['weight_class'] = np.select(conditions, choices, default='Unknown')

        return df
    

    def clean_order_data(self, df):
        '''
        Cleans the order data.

        Parameters
        ----------
        df

        Returns
        -------
        df
        '''
        df = df.copy()

        # Drop columns
        df = df.drop(columns=['first_name', 'last_name', '1', 'level_0', 'index'])

        # Drop rows with null values
        df.dropna(inplace=True)

        return df
    

    def clean_date_data(self, df):
        '''
        Cleans the date data.

        Parameters
        ----------
        df

        Returns
        -------
        df
        '''
        df = df.copy()

        # Drop rows with null values
        df.dropna(inplace=True)

        # Create a single date column
        df['date'] = pd.to_datetime(df[['year', 'month', 'day']], errors='coerce')
        # Reformat to yyyy-mm-dd
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')

        # Check timestamp format
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        # Extract just the time part and format it as hh:mm:ss
        df['timestamp'] = df['timestamp'].dt.strftime('%H:%M:%S')

        # Remove invalid time period
        valid_category = ['Late_Hours', 'Morning', 'Midday', 'Evening']
        df = df[df['time_period'].isin(valid_category)]

        # Check no extra nulls have been produced
        df.dropna(inplace=True)

        return df