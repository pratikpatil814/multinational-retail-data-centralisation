import pandas as pd
from database_utils  import DatabaseConnector as data_conn
from data_extraction import DataExtractor as data_ex
from data_cleaning   import datacleaning as data_cc

def upload_dim_users():
    credential = data_conn().read_db_creds("db_creds.yaml")
    engine = data_conn().init_db_engine(credential)
    engine.connect()
    name_tabel = data_ex().list_db_tables(engine)
    df_name = name_tabel[1]
    df = data_ex().read_rds_table(engine, df_name)
    df = data_cc().clean_user_data(df)
    data_conn().upload_to_db("dim_users",df)

def upload_dim_card_details():
    path = "card_details.pdf"
    df_card_details = data_ex().retrieve_pdf_data(path)
    df_card_details  = data_cc().clean_card_data(df_card_details)
    data_conn().upload_to_db("dim_card_details",df_card_details)

def upload_dim_store_details(): 
    df_dim_store_details = data_ex().retrieve_stores_data()
    df_dim_store_details = data_cc().called_clean_store_data(df_dim_store_details)
    data_conn().upload_to_db("dim_store_details",df_dim_store_details)

def upload_product_weights():
    df_product_weights = data_ex().extract_from_s3()
    df_product_weights = data_cc().convert_product_weights(df_product_weights,"weight")
    data_conn().upload_to_db("dim_products",df_product_weights)

def upload_orders_table():
    credential = data_conn().read_db_creds("db_creds.yaml")
    engine = data_conn().init_db_engine(credential)
    engine.connect()
    name_tabel = data_ex().list_db_tables(engine)
    df_name = name_tabel[2]
    df = data_ex().read_rds_table(engine, df_name)
    df = data_cc().clean_orders_data(df)
    data_conn().upload_to_db("orders_table",df)

def upload_dim_date_times():
    df = data_ex().extract_from_s3_by_link()
    df = data_cc().clean_date_time(df)
    data_conn().upload_to_db("dim_date_times",df)

