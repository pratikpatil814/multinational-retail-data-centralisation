
# Multinational Retail Data Centralisation

This program aims to centralize and analyze sales data for a multinational company that sells various goods worldwide. The company currently faces challenges with data accessibility and analysis due to the data being spread across multiple sources. To address this, the program provides a solution to store the company's sales data in a centralized database, acting as a single source of truth. It also offers functionalities to query the database and generate up-to-date metrics for the business.  

# Dependencies

The program relies on the following dependencies:

- pandas: For working with DataFrames.
- yaml: For reading and parsing YAML files.
- sqlalchemy: For database connectivity and operations.
- requests: For making API requests.
- tabula: For extracting data from PDF files.
- boto3: For interacting with Amazon S3.

Make sure you have these dependencies installed before running the program.

## Features
- Data Centralization: The program connects to various data sources, including databases, PDF files, APIs, and S3 buckets, to extract sales data and store it in a centralized database. This ensures that all sales data is accessible from one location.

- Database Connectivity: The program includes a database_utils module that facilitates database connectivity. It enables the reading of database credentials from YAML files, initialization of the database engine using SQLAlchemy, and uploading of data to a specified database table.

- Data Extraction: The data_extraction module provides functions to extract sales data from different sources. It includes methods to retrieve database table names, read data from a database table, extract data from PDF files, make API requests, and extract data from S3 buckets.

- Data Cleaning and Processing: The data_cleaning module offers functions to clean and process sales data. It includes operations such as handling dates, removing unwanted characters, converting weights, and more. The cleaning operations ensure that the data is standardized and ready for analysis.

- Up-to-Date Metrics: Once the sales data is centralized in the database, the program enables querying the database to generate up-to-date metrics for the business. Users can perform custom queries to retrieve specific sales insights, such as total revenue, top-selling products, sales by region, and more.

## Initializing a Local Database for Storing Extracted Data

To store the extracted data from various sources, we need to set up a new database locally using pgAdmin 4. Follow the steps below to create a new database named "sales_data" within pgAdmin 4.

Prerequisites
Before proceeding, make sure you have the following:

- pgAdmin 4 installed on your local machine.
## Steps
1. Launch pgAdmin 4: Once you have pgAdmin 4 installed, launch the application.

2. Connect to the PostgreSQL Server: In pgAdmin 4, navigate to the "Browser" section on the left-hand side. Expand the "Servers" tree and select the PostgreSQL server you want to connect to. If you haven't set up a PostgreSQL server, you can create a new one using the "Create" option.

3. Create a New Database: Right-click on the "Databases" node under the selected PostgreSQL server and choose "Create" > "Database" from the context menu.

4. Configure the Database: In the "Create - Database" dialog, enter "sales_data" as the "Database Name". You can leave the other settings as their default values for now. Click "Save" to create the database.

5. Verify the Database: The newly created "sales_data" database should now appear under the "Databases" node in the pgAdmin 4 browser tree.

## Usage
Now that you have set up the "sales_data" database, you can use it as a central location to store all the company information extracted from various data sources. Update your program's configuration or connection settings to use this database for storing the extracted data.


## Demo
```python
#Example configuration for connecting to the "sales_data" database
DATABASE = {
    'host': 'localhost',
    'port': '5432',
    'database': 'sales_data',
    'username': 'your_username',
    'password': 'your_password'
}
```
Make sure to replace 'your_username' and 'your_password' with your actual database credentials.
By following these steps, you have successfully initialized a local database named "sales_data" using pgAdmin 4. This database will serve as the central repository for storing all the company information extracted from the various data sources. Ensure that your program is configured to connect to this database using the appropriate credentials to store the extracted data efficiently.

# Defining Data Extraction, Database Connection, and Data Cleaning Scripts

To handle data extraction, database connection, and data cleaning, we will create three separate Python scripts: data_extraction.py, database_utils.py, and data_cleaning.py. These scripts will contain classes and methods to perform specific tasks related to each aspect of the data pipeline.

## Step 1: data_extraction.py
Create a new Python script named data_extraction.py and define a class named DataExtractor. This class will serve as a utility class to extract data from different data sources. The methods within this class will be designed to extract data from specific sources such as CSV files, an API, and an S3 bucket.

## Step 2: database_utils.py
Create another Python script named database_utils.py and define a class named DatabaseConnector. This class will be responsible for connecting to the database and uploading data to it. It will provide methods to initialize the database connection and upload data to the specified tables.

## Step 3: data_cleaning.py
Create a script named data_cleaning.py and define a class named DataCleaning within it. This class will handle the cleaning of data extracted from various sources. It will contain methods to clean data specific to each data source, ensuring that the extracted data is consistent and suitable for analysis.

The methods within the DataCleaning class will be defined in subsequent tasks when required. These methods will handle tasks such as cleaning data errors, formatting dates and timestamps, handling null values, and converting data formats.

By organizing our code into these separate scripts and classes, we can achieve modularization and maintainability, allowing us to focus on specific tasks in each part of the data pipeline.




## Extracting Data from AWS RDS Database and Cleaning User Data

To extract data from the AWS RDS database and clean the user data, we will update the `DataExtractor` and `DatabaseConnector` classes with the necessary methods. We will also create a `DataCleaning` class to handle the cleaning of user data.

### Step 1: Create `db_creds.yaml` File

Create a `db_creds.yaml` file in your project directory and add the following credentials:

```yaml
RDS_HOST: data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com
RDS_PASSWORD: AiCore2022
RDS_USER: aicore_admin
RDS_DATABASE: postgres
RDS_PORT: 5432
```

Make sure to add `db_creds.yaml` to your `.gitignore` file to prevent the credentials from being uploaded to your public GitHub repository.

### Step 2: Implement `read_db_creds` Method

In the `DatabaseConnector` class, create a method called `read_db_creds` to read the credentials from the `db_creds.yaml` file and return them as a dictionary. Install the `PyYAML` package and import the `yaml` module to accomplish this.

### Step 3: Implement `init_db_engine` Method

In the `DatabaseConnector` class, implement the `init_db_engine` method. This method should use the credentials obtained from the `read_db_creds` method to initialize and return an SQLAlchemy database engine.

### Step 4: Implement `list_db_tables` Method

In the `DatabaseConnector` class, create a method called `list_db_tables` to list all the tables in the database. This method should use the initialized database engine to retrieve the table names and return them as a list.

### Step 5: Implement `read_rds_table` Method

In the `DataExtractor` class, create a method called `read_rds_table` to extract a specific database table to a pandas DataFrame. This method should take an instance of the `DatabaseConnector` class and the table name as arguments. Use the `list_db_tables` method to get the name of the table containing user data, and then use the initialized database engine to read the table data into a pandas DataFrame. Finally, return the DataFrame.

### Step 6: Implement `clean_user_data` Method

In the `DataCleaning` class, create a method called `clean_user_data` to perform the cleaning of the user data. This method should take a pandas DataFrame as an argument and apply various cleaning operations to handle NULL values, errors with dates, incorrectly typed values, and rows filled with incorrect information. Make sure to handle these cleaning operations based on the specific structure and requirements of the user data.

### Step 7: Implement `upload_to_db` Method

In the `DatabaseConnector` class, create a method called `upload_to_db` to upload a pandas DataFrame to the sales_data database. This method should take a DataFrame and a table name as arguments. Use the initialized database engine to establish a connection to the database and upload the DataFrame to the specified table.

### Step 8: Store Cleaned User Data in dim_users Table

After extracting and cleaning the user data, use the `upload_to_db` method from the `DatabaseConnector` class to store the cleaned data in the sales_data database. Specify the table name as "dim_users" when calling the `upload_to_db` method.

By completing these steps, you will be able to extract data from the AWS RDS database, clean the user data using the `DataCleaning` class, and store the cleaned data in the dim_users table in the sales_data database.

## Extracting Card Details from a PDF Document in AWS S3 Bucket and Cleaning Data

To extract card details from a PDF document stored in an AWS S3 bucket and clean the extracted data, follow these steps:

### Step 1: Install `tabula-py` Package

Install the `tabula-py` package in your Python environment. This package will be used to extract data from the PDF document. Refer to the `tabula-py` documentation for installation instructions.

### Step 2: Implement `retrieve_pdf_data` Method

In the `DataExtractor` class, create a method called `retrieve_pdf_data` that takes a link as an argument and returns a pandas DataFrame. Use the `tabula-py` package to extract data from all pages of the PDF document at the provided link. Return a DataFrame containing the extracted data.

### Step 3: Implement `clean_card_data` Method

In the `DataCleaning` class, create a method called `clean_card_data` to clean the extracted card data. Apply necessary cleaning operations to remove erroneous values, handle NULL values, and address formatting errors. Ensure that the cleaned data meets the required standards.

### Step 4: Upload Cleaned Card Details to Database

Using the `upload_to_db` method in the `DatabaseConnector` class, upload the cleaned card details to the database. Specify the table name as "dim_card_details" when calling the `upload_to_db` method. This will store the cleaned card details in the specified table within the database.

By completing these steps, you will be able to extract card details from the PDF document stored in the AWS S3 bucket using `tabula-py`, clean the extracted data using the `DataCleaning` class, and upload the cleaned data to the "dim_card_details" table in the database using the `upload_to_db` method.

## Retrieving and Cleaning Store Data from API

To retrieve store data from the API and clean the extracted data, follow these steps:

### Step 1: Implement `list_number_of_stores` Method

In the `DataExtractor` class, create a method called `list_number_of_stores` that takes the number of stores endpoint and the header dictionary as arguments. This method should make a GET request to the number of stores endpoint using the provided header dictionary and return the number of stores.

### Step 2: Determine the Number of Stores

Using the `list_number_of_stores` method, determine the number of stores that need to be extracted from the API.

### Step 3: Implement `retrieve_stores_data` Method

In the `DataExtractor` class, create a method called `retrieve_stores_data` that takes the retrieve a store endpoint as an argument. This method should extract all the stores from the API by making a GET request to the retrieve a store endpoint for each store number. Save the extracted data in a pandas DataFrame and return it.

### Step 4: Implement `clean_store_data` Method

In the `DataCleaning` class, create a method called `clean_store_data` that takes the DataFrame containing the retrieved store data as an argument. Apply necessary cleaning operations to the store data to handle any errors, inconsistencies, or missing values. Return the cleaned DataFrame.

### Step 5: Upload Cleaned Store Data to Database

Using the `upload_to_db` method in the `DatabaseConnector` class, upload the cleaned store data to the database. Specify the table name as "dim_store_details" when calling the `upload_to_db` method. This will store the cleaned store data in the specified table within the database.

By completing these steps, you will be able to retrieve store data from the API, clean the extracted data using the `DataCleaning` class, and upload the cleaned data to the "dim_store_details" table in the database using the `upload_to_db` method.

## Extracting and Cleaning Orders Data from AWS RDS Database

To extract and clean orders data from the AWS RDS database and upload it to the "orders_table" in the database, follow these steps:

### Step 1: List Database Tables

Using the `list_db_tables` method in the `DatabaseConnector` class, list all the tables in the database to identify the table that contains the orders data.

### Step 2: Extract Orders Data

Using the `read_rds_table` method in the `DataExtractor` class, extract the orders data from the identified table. This method will return a pandas DataFrame containing the extracted data.

### Step 3: Clean Orders Data

Create a method called `clean_orders_data` in the `DataCleaning` class to clean the orders data. Remove the columns named "first_name", "last_name", and "1" to ensure that the table is in the correct form. Perform any additional cleaning operations as necessary to ensure data consistency and accuracy.

### Step 4: Upload Cleaned Orders Data to Database

Using the `upload_to_db` method in the `DatabaseConnector` class, upload the cleaned orders data to the database. Specify the table name as "orders_table" when calling the `upload_to_db` method. This will store the cleaned orders data in the specified table within the database.

By completing these steps, you will be able to extract the orders data from the AWS RDS database, clean the extracted data using the `DataCleaning` class, and upload the cleaned data to the "orders_table" in the database using the `upload_to_db` method. The "orders_table" will serve as the single source of truth for all the company's past orders.

## Extracting and Cleaning Date-Time Data from JSON File

To extract the date-time data from the JSON file stored on S3, perform necessary cleaning, and upload the data to the database in a table named "dim_date_times," follow these steps:

### Step 1: Retrieve the JSON File

Use the appropriate method from your `DataExtractor` class to retrieve the JSON file from the provided link: https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json.

### Step 2: Extract Data from JSON

Using pandas or a suitable JSON parsing library, extract the data from the JSON file and store it in a pandas DataFrame.

### Step 3: Perform Data Cleaning

Create a method called `clean_date_time` in your `DataCleaning` class to clean the date-time data extracted from the JSON. Apply necessary cleaning operations to handle any errors, inconsistencies, or missing values.

### Step 4: Upload Cleaned Data to Database

Using the `upload_to_db` method in the `DatabaseConnector` class, upload the cleaned date-time data to the database. Specify the table name as "dim_date_times" when calling the `upload_to_db` method. This will store the cleaned date-time data in the specified table within the database.

By completing these steps, you will be able to extract the date-time data from the JSON file, clean the extracted data using the `DataCleaning` class, and upload the cleaned data to the "dim_date_times" table in the database.

