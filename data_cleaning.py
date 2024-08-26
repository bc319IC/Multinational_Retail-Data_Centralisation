import pandas as pd
import re


class DataCleaning():
    
    def clean_user_data(self, df):
        df = df.copy()

        # Drop column
        df = df.drop(columns=['index'])

        # Drop rows with NULL values
        df.dropna(inplace=True)

        # Check DOB format
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
        # Check for invalid date entries that were converted to NaT
        if df['date_of_birth'].isna().any():
            df = df.dropna(subset=['date_of_birth'])
        # Reformat to yyyy-mm-dd
        df['date_of_birth'] = df['date_of_birth'].dt.strftime('%Y-%m-%d')

        # Check email format
        email_pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
        # Validate emails
        df['valid_email_address'] = df['email_address'].str.match(email_pattern)
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
        # Check if the country_code matches the expected code from the dictionary
        df['correct_country_code'] = df['country'].map(country_to_code)
        df['is_match'] = df['country_code'] == df['correct_country_code']
        # Correct mismatches
        df.loc[~df['is_match'], 'country_code'] = df['correct_country_code']
        df = df.drop(columns=['correct_country_code', 'is_match'])

        # Check phone number format
        df['phone_number'] = df['phone_number'].str.replace(r'\+44\(0\)|\+49\(0\)|\+44|\+49|\(0\)', '0', regex=True)
        df['phone_number'] = df['phone_number'].apply(lambda phone: re.sub(r'\D', '', phone))
        df = df[df['phone_number'].str.len().between(10, 11)]
        df['phone_number'] = df['phone_number'].apply(lambda phone: phone[:5] + ' ' + phone[5:] if len(phone) > 5 else phone)

        # Check join date format
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')
        # Check for invalid date entries that were converted to NaT
        if df['join_date'].isna().any():
            df = df.dropna(subset=['join_date'])
        # Reformat to yyyy-mm-dd
        df['join_date'] = df['join_date'].dt.strftime('%Y-%m-%d')

        # Check no extra nulls have been produced
        df.dropna(inplace=True)

        return df
    

    def clean_card_data(self, df):
        df = df.copy()

        # Drop rows with NULL values
        df.dropna(inplace=True)
        
        # Check expiry date format
        # Attempt to convert 'exp_date' to datetime using MM/YY format, and keep original if valid
        def convert_exp_date(date_str):
            try:
                return pd.to_datetime(date_str, format='%m/%y').strftime('%m/%y')
            except ValueError:
                return pd.NaT  # Retain invalid entries as NaT
        # Apply the conversion function
        df['expiry_date'] = df['expiry_date'].apply(convert_exp_date)
        
        # Check date payment confirmed format
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        # Check for invalid date entries that were converted to NaT
        if df['date_payment_confirmed'].isna().any():
            df = df.dropna(subset=['date_payment_confirmed'])
        # Reformat to yyyy-mm-dd
        df['date_payment_confirmed'] = df['date_payment_confirmed'].dt.strftime('%Y-%m-%d')
        
        # Check card number length
        # Extract digit information from the provider column
        def validate_card_number(row):
            match = re.search(r'(\d+)\s*digit', row['card_provider'])
            if match:
                expected_length = int(match.group(1))
                actual_length = len(str(row['card_number']))
                return expected_length == actual_length
            return True  # If no digit is mentioned, assume it's valid
        # Apply the validation to the DataFrame
        df['valid_card_number'] = df.apply(validate_card_number, axis=1)
        # Filter out rows where the card number length does not match the provider description
        df = df[df['valid_card_number']].drop(columns=['valid_card_number'])
        
        # Check no extra nulls have been produced
        df.dropna(inplace=True)
        
        return df
    

    def clean_store_data(self, df):
        df = df.copy()

        # Drop columns
        df = df.drop(columns=['index', 'lat'])

        # Drop rows with NULL values
        df.dropna(inplace=True)

        # Change column types to numeric
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')

        # Change column type to int
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce')
        df.dropna(subset=['staff_numbers'], inplace=True)
        df['staff_numbers'] = df['staff_numbers'].astype(int)

        # Remove non-letter entries in locality
        df = df[df['locality'].str.match(r'^[A-Za-z]+$', na=False)]

        # Check opening date format
        df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')
        # Check for invalid date entries that were converted to NaT
        if df['opening_date'].isna().any():
            df = df.dropna(subset=['opening_date'])
        # Reformat to yyyy-mm-dd
        df['opening_date'] = df['opening_date'].dt.strftime('%Y-%m-%d')

        # Remove invalid store types
        valid_store_type = ['Super Store', 'Local', 'Outlet', 'Mall Kiosk']
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

        # Check no extra nulls have been produced
        df.dropna(inplace=True)
        
        return df