import pandas as pd
import re
import numpy as np
class datacleaning():
    def clean_user_data(self , df):
        '''
        Cleans user data by performing specific data cleaning operations on the provided DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame containing user data to be cleaned.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        '''
        df = self.clean_error_with_dates(df,"date_of_birth")
        df = self.clean_error_with_dates(df,"join_date")
        df.dropna()
        return df
    def clean_date_time(self,df):
        '''
        This function cleans and processes the date and time columns in the DataFrame.
    
        Args:
            df (DataFrame): The input DataFrame containing date and time columns.
        
        Returns:
            DataFrame: The cleaned DataFrame with processed date and time columns.
        '''
        df['month'] =  pd.to_numeric( df['month'],errors='coerce', downcast="integer")
        df['year'] =  pd.to_numeric( df['year'], errors='coerce', downcast="integer")
        df['day'] =  pd.to_numeric( df['day'], errors='coerce', downcast="integer")
        df['timestamp'] =  pd.to_datetime(df['timestamp'], format='%H:%M:%S', errors='coerce')
        df.dropna(how='any',inplace= True)
        df.reset_index(inplace=True)     
        return df
    def clean_error_with_dates(self,df,column_name):
        '''
        Cleans a specific column in the DataFrame by converting various date formats to a consistent format.

        Args:
            df (pd.DataFrame): The DataFrame to be cleaned.
            column_name (str): The name of the column to be cleaned.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        '''
        df[column_name] = pd.to_datetime(df[column_name], format='%Y-%m-%d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%Y %B %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%B %Y %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
        df.dropna(subset = column_name,how='any',inplace= True)
        return df
    def clean_null_values_req(df,cloumn_name,change_value):
        '''
        Cleans null values in a specific column of the DataFrame by replacing them with a provided value.

        Args:
            df (pd.DataFrame): The DataFrame to be cleaned.
            cloumn_name (str): The name of the column containing null values.
            change_value: The value to replace the null values with.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        '''
        df[cloumn_name].fillna(change_value, inplace = True)
        return df
    def clean_card_data(self,df):
        '''
        Cleans card data in the DataFrame by performing specific cleaning operations.

        Args:
            df (pd.DataFrame): The DataFrame to be cleaned.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        '''
        df['card_number'] = df['card_number'].apply(str)
        df['card_number'] = df['card_number'].str.replace('?','')
        df = self.clean_error_with_dates(df,'date_payment_confirmed')  
        df.dropna(how='any',inplace= True)
        return df
    def called_clean_store_data(self,df):
        '''
        Cleans store data in the DataFrame by performing specific cleaning operations.

        Args:
            df (pd.DataFrame): The DataFrame to be cleaned.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        '''
        df = self.clean_error_with_dates(df,'opening_date') 
        df['staff_numbers'] =  pd.to_numeric( df['staff_numbers'].apply(self.remove_unwanted_char_from_string),errors='coerce', downcast="integer")
        df.dropna(subset = ['staff_numbers'],how='any',inplace= True)
         
        return df
    
    def remove_unwanted_char_from_string(self,value):
        '''
        Removes unwanted characters from a string.

        Args:
            value (str): The string to be cleaned.

        Returns:
            str: The cleaned string.
        '''
        return re.sub(r'\D', '',value)
    def convert_product_weights(self,df,coloumn_name):
        '''
        Converts product weights in a specific column of the DataFrame to a consistent format.

        Args:
            df (pd.DataFrame): The DataFrame to be cleaned.
            coloumn_name (str): The name of the column containing product weights.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        '''
        df[coloumn_name] = df[coloumn_name].apply(self.get_kg)
        df = self.clean_products_data(df)

        return df
    def get_kg(self,value):
        '''
        Converts a product weight value to kilograms.

        Args:
            value: The value to be converted.

        Returns:
            float: The converted value in kilograms.
        '''
        value=str(value)
        if value.endswith('kg'):
            value = value.replace('kg','')
            value = self.check_multiflication(value)
            return float(value) if self.isfloat(value) else np.nan
        elif value.endswith('g'):   
            value = value.replace('g','')
            value = self.check_multiflication(value)
            return float(value)/1000 if self.isfloat(value) else np.nan
        elif value.endswith('ml'):   
            value = value.replace('ml','')
            value = self.check_multiflication(value)
            return float(value)/1000 if self.isfloat(value) else np.nan
        elif value.endswith('l'):   
            value = value.replace('l','')
            value = self.check_multiflication(value)
            return float(value) if self.isfloat(value) else np.nan
        elif value.endswith('oz'):   
            value = value.replace('oz','')
            value = self.check_multiflication(value)
            return 28.3495*float(value)/1000 if self.isfloat(value) else np.nan
        else:
            np.nan
        
    def check_multiflication(self,value):
        '''
        Checks if the value contains a multiplication operation and performs the multiplication if present.

        Args:
            value (str): The value to be checked.

        Returns:
            str: The modified value.
        '''
        if 'x' in value:
            value.replace(' ','')
            lis_factors = value.split('x')
            return str(float(lis_factors[0])*float(lis_factors[1]))
        return value
    def isfloat(self,num):
        '''
        Checks if a value can be converted to a float.

        Args:
            num: The value to be checked.

        Returns:
            bool: True if the value can be converted to a float, False otherwise.
        '''
        try:
            float(num)
            return True
        except ValueError:
            return False
    def clean_products_data(self,df):
        '''
        Cleans product data in the DataFrame by performing specific cleaning operations.

        Args:
            df (pd.DataFrame): The DataFrame to be cleaned.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        '''
        df =  self.clean_error_with_dates(df,'date_added')
        df.dropna(how='any',inplace= True)
        df.reset_index(inplace=True)       
        return df
    
    def clean_orders_data(self,df):
        '''
        Cleans order data in the DataFrame by performing specific cleaning operations.

        Args:
            df (pd.DataFrame): The DataFrame to be cleaned.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        '''
        df.drop(columns='1',inplace=True)
        df.drop(columns='first_name',inplace=True)
        df.drop(columns='last_name',inplace=True)
        df.drop(columns='level_0',inplace=True)
        df['card_number'] = df['card_number'].apply(self.isnum)
        df.dropna(how='any',inplace= True)
        return df
    def isnum(self,num):
        '''
        Checks if a value is numeric.

        Args:
            num: The value to be checked.

        Returns:
            str: The value if it is numeric, otherwise NaN.
        '''
        return str(num) if str(num).isdigit() else np.nan


    
