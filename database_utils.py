import yaml
from sqlalchemy import create_engine
class DatabaseConnector:
    def read_db_creds(self, location ):
        '''
        This function is used to retrieve data from a YAML file at the specified location
        and return the credentials in a dictionary format.
        
        Parameters:
            location (str): The path to the YAML file.
        
        Returns:
            dict: A dictionary containing the credentials loaded from the YAML file.
                The structure of the dictionary will depend on the content of the YAML file.
                If the YAML file is empty or cannot be parsed, None will be returned.
        '''
        with open(location, 'r') as stream:
            try :
                return yaml.safe_load(stream)
            except yaml.YAMLError as exc : 
                print(exc)
    def init_db_engine(self,credential):
        '''
        This function is used to connect to the specified database engine using the given credentials.
    
        Args:
            credential (dict): A dictionary containing the necessary credentials to connect to the database.
                Required keys:
                    - "RDS_HOST": The host or endpoint of the database server.
                    - "RDS_USER": The username for the database connection.
                    - "RDS_PASSWORD": The password for the database connection.
                    - "RDS_PORT": The port number for the database connection.
                    - "RDS_DATABASE": The name of the database to connect to.
        
        Returns:
            engine: An instance of the SQLAlchemy engine connected to the specified database.
    
        '''
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = credential["RDS_HOST"]
        USER = credential["RDS_USER"]
        PASSWORD = credential["RDS_PASSWORD"]
        PORT = credential["RDS_PORT"]
        DATABASE = credential["RDS_DATABASE"]
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        return engine
    def upload_to_db(self , table_name , df):
        '''
        Uploads a DataFrame to a database table.

        Args:
            table_name (str): The name of the table to upload the data to.
            df (pandas.DataFrame): The DataFrame containing the data to upload.

        Returns:
            None

        Raises:
            Exception: If there is an error during the upload process.
        '''

        # Retrieve database credentials from a YAML file
        credential = self.read_db_creds("local_db.yaml")
        # Initialize the database engine using the retrieved credentials
        engine = self.init_db_engine(credential)
        # Connect to the database
        conn = engine.connect()
        try:
            # Upload the DataFrame to the specified table in the database,
            # replacing any existing data
            df.to_sql(table_name, engine, if_exists = "replace")
        except Exception as e:
             # Raise an exception if there is an error during the upload process
            raise Exception("Error uploading data to the database: " + str(e))
        finally:
            # Close the database connection
            conn.close()
