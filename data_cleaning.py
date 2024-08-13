import pandas as pd
import re

class DataCleaning():
    
    def clean_user_data(self, df):
        df = df.copy()

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

        return df