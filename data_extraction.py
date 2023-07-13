
import pandas as pd
from sqlalchemy import inspect
import requests
import tabula
import boto3
class DataExtractor:
    def list_db_tables(self,engine):
        '''
        Retrieves a list of database tables from the specified database engine.

        Args:
            engine: The SQLAlchemy engine object connected to the database.

        Returns:
            A list of table names present in the connected database.
        '''
        inspector = inspect(engine)# Create an inspector object to access database metadata
        return inspector.get_table_names()  # Retrieve a list of table names from the database
    def read_rds_table(self,engine,table_name):
        '''
        This function reads data from an database table using the provided SQLAlchemy engine.
    
        Args:
            engine (sqlalchemy.engine.Engine): The SQLAlchemy engine connected to the database.
            table_name (str): The name of the table to read from.
        
        Returns:
            pandas.DataFrame: A DataFrame containing the data retrieved from the specified table.
        '''
        with engine.begin() as conn:
            return pd.read_sql_table(table_name, con=conn)
    def retrieve_pdf_data(self,pdf_location):
        '''
        Retrieves data from a PDF file located at the specified location using tabula-py.

        Args:
            pdf_location (str): The path or URL of the PDF file.

        Returns:
            pandas.DataFrame: A pandas DataFrame containing the concatenated data from all pages of the PDF.
            
        '''
        return pd.concat(tabula.read_pdf(pdf_location, pages='all'))
    def api_key(self):
        '''
        Retrieve the API key.
    
        Returns:
            dict: A dictionary containing the API key with the key name "x-api-key".
    
        '''
        return { "x-api-key" : "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    
    def list_number_of_stores(self):
        '''
            Retrieves the number of stores from an API endpoint.

        Returns:
            int: The number of stores.
        '''
        api_url_base = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        response = requests.get(
                                api_url_base,
                                headers=self.api_key()
                                )
        return response.json()['number_stores']
    def retrieve_stores_data(self):
        '''
        Retrieves store data from an API and returns it as a concatenated pandas DataFrame.

        Returns:
            pd.DataFrame: Concatenated DataFrame containing store data.
        '''
        list_of_frames = []  # List to store individual store DataFrames
        store_number   = self.list_number_of_stores() # Retrieves the total number of stores
        # Iterate over the store numbers
        for _ in range(store_number):
            api_url_base = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{_}'
            # Make a GET request to the API endpoint
            response = requests.get(
                                    api_url_base,
                                    headers=self.api_key()
                                    )
            # Append the normalized JSON response to the list of frames
            list_of_frames.append( pd.json_normalize(response.json()))
        # Concatenate the list of frames into a single DataFrame    
        return pd.concat(list_of_frames)
    def extract_from_s3(self):
        '''
        This function extracts a file named 'products.csv' from an S3 bucket called 'data-handling-public'.
        It utilizes the Boto3 library to interact with Amazon S3 and Pandas to read the extracted CSV file.

        Returns:
            pandas.DataFrame: The contents of the extracted CSV file as a DataFrame if the extraction is successful.
            Otherwise, None is returned.

        '''
        s3_client = boto3.client(
                                    "s3"
                                )
        response = s3_client.get_object(Bucket='data-handling-public', Key='products.csv')
        status   = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            return pd.read_csv(response.get("Body"))
        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")
    def extract_from_s3_by_link(self):
        '''
        This function retrieves data from an S3 bucket by making an HTTP GET request to a specific link.
        Returns:
        - A pandas DataFrame containing the retrieved data.
        '''
        response = requests.get('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
        return pd.DataFrame.from_dict(response.json())
