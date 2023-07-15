
import pandas as pd
import numpy as np
import requests
import json
import boto3
import io
from io import BytesIO, StringIO
from dags.common.credentials.secrets import *
from dags.common.helpers import *

# General Payments Ingest

# General Payments File Path
payments_2020_path = "https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_GNRL_PGYR2020_P01202023.csv"
payments_2021_path ="https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_GNRL_PGYR2021_P01202023.csv"

# Read Payments Data for 2020 and 2021
payments_2020_df = read_chunks(payments_2020_path, 10000)
payments_2021_df = read_chunks(payments_2021_path, 10000)


keep_cols = ['Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID', 
                              'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name',
                              'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State',
                              'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country',
                                'Change_Type','Record_ID','Covered_Recipient_NPI', 'Covered_Recipient_Profile_ID','Covered_Recipient_Type',
                          'Covered_Recipient_Primary_Type_1','Covered_Recipient_Specialty_1',
                          'Total_Amount_of_Payment_USDollars','Date_of_Payment','Program_Year','Payment_Publication_Date',
                          'Number_of_Payments_Included_in_Total_Amount',
                          'Form_of_Payment_or_Transfer_of_Value','Nature_of_Payment_or_Transfer_of_Value',
                          'Third_Party_Payment_Recipient_Indicator',
                           'Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value','Charity_Indicator', 
                          'Third_Party_Equals_Covered_Recipient_Indicator',
                          'Delay_in_Publication_Indicator', 'Related_Product_Indicator', 'City_of_Travel','State_of_Travel', 
                          'Country_of_Travel','Dispute_Status_for_Publication', 'Physician_Ownership_Indicator', 
                          'Teaching_Hospital_CCN','Teaching_Hospital_ID', 'Teaching_Hospital_Name']

payments_2020_df = payments_2020_df[keep_cols]
payments_2021_df = payments_2021_df[keep_cols]

# upload payments file to S3
payments_2020_file_name = f"{source_s3_key}{general_payments_2020}"
upload_dataframe_to_s3_csv(payments_2020_df, bucket_name, payments_2020_file_name)

payments_2021_file_name = f"{source_s3_key}{general_payments_2021}"
upload_dataframe_to_s3_csv(payments_2021_df, bucket_name, payments_2021_file_name)


###################################################################
# Function to transform general payment data to get multiple tables
###################################################################

# General Payment dim table:  

def transform_dim__gp_manufacturer_gpo(df):    
    # Creating a sub dataframe to contain values related to dimension table for 2020 general payments
    dim_gp_manufacturer_gpo = df[['Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID', 
                              'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name',
                              'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State',
                              'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country']]

    # Check for nulls
    null_values = dim_gp_manufacturer_gpo.isnull().sum()
    null_values

    # Check for duplicates
    duplicate_rows=dim_gp_manufacturer_gpo[dim_gp_manufacturer_gpo.duplicated(subset=['Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID', 
                          'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name',
                          'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State',
                          'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country'], keep=False)]
    duplicate_rows
    
    # remove duplicates
    dim_gp_manufacturer_gpo = dim_gp_manufacturer_gpo.drop_duplicates()
    
    # Check for duplicates again
    duplicate = dim_gp_manufacturer_gpo[dim_gp_manufacturer_gpo.duplicated()]
    duplicate
    
    # Renaming columns for consistency 
    dim_gp_manufacturer_gpo.rename(columns = {'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID':'Manufacturer_ID', 
                              'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name': 'Manufacturer_Name',
                              'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State':'Manufacturer_State',
                              'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country': 'Manufacturer_Country'}, inplace = True)

    # Check for data type errors
    dim_gp_manufacturer_gpo.info()
     
    # Handle data type errors
    
    return dim_gp_manufacturer_gpo

# General Payment fact table:  
def transform_fact_general_payment(df):    
        
    # Creating a sub dataframe to contain values related to fact table for 2020 general payments

    fact_general_payment = df[['Change_Type','Record_ID','Covered_Recipient_NPI', 'Covered_Recipient_Profile_ID','Covered_Recipient_Type',
                          'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID',
                          'Covered_Recipient_Primary_Type_1','Covered_Recipient_Specialty_1',
                          'Total_Amount_of_Payment_USDollars','Date_of_Payment','Program_Year','Payment_Publication_Date',
                          'Number_of_Payments_Included_in_Total_Amount',
                          'Form_of_Payment_or_Transfer_of_Value','Nature_of_Payment_or_Transfer_of_Value',
                          'Third_Party_Payment_Recipient_Indicator', 
                          'Delay_in_Publication_Indicator', 'Related_Product_Indicator','Dispute_Status_for_Publication', 'Physician_Ownership_Indicator']]

    
    # Checking for data type errors   
    # Updating Date data types
    fact_general_payment['Date_of_Payment'] = pd.to_datetime(fact_general_payment["Date_of_Payment"])
    fact_general_payment['Payment_Publication_Date'] = pd.to_datetime(fact_general_payment["Payment_Publication_Date"])
    
    # Update any other data type error
    
    # Check for nulls
    null_values = fact_general_payment.isnull().sum()
    print(null_values)
    
    # drop null values/subset
    fact_general_payment.dropna(subset = ['Covered_Recipient_Profile_ID'], inplace=True)
    fact_general_payment.dropna(subset = ['Covered_Recipient_NPI'], inplace=True)
    
    # Replace Nulls with NULL
    fact_general_payment["Covered_Recipient_Primary_Type_1"].fillna("NULL", inplace = True)
    fact_general_payment["Covered_Recipient_Specialty_1"].fillna("NULL", inplace = True)

    # Check for duplicates
    duplicate_rows=fact_general_payment[fact_general_payment.duplicated(subset=['Change_Type','Record_ID','Covered_Recipient_NPI', 'Covered_Recipient_Profile_ID','Covered_Recipient_Type',
                          'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID',
                          'Covered_Recipient_Primary_Type_1','Covered_Recipient_Specialty_1',
                          'Total_Amount_of_Payment_USDollars','Date_of_Payment','Program_Year','Payment_Publication_Date',
                          'Number_of_Payments_Included_in_Total_Amount',
                          'Form_of_Payment_or_Transfer_of_Value','Nature_of_Payment_or_Transfer_of_Value',
                          'Third_Party_Payment_Recipient_Indicator', 
                          'Delay_in_Publication_Indicator', 'Related_Product_Indicator','Dispute_Status_for_Publication', 'Physician_Ownership_Indicator'], keep=False)]
    print(duplicate_rows)
    
    # remove duplicates
    fact_general_payment = fact_general_payment.drop_duplicates()
    
    # Check for duplicates again
    duplicate = fact_general_payment[fact_general_payment.duplicated()]
    duplicate
    
    # Renaming columns for consistency 
    fact_general_payment.rename(columns = {'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID':'Manufacturer_ID'}, inplace = True)
    
    dim_gpo = transform_dim__gp_manufacturer_gpo(df)                                                      
                                       
    return fact_general_payment, dim_gpo


payments_2020_df, payments_gpo_2020_df = transform_fact_general_payment(payments_2020_df)
payments_2021_df, payments_gpo_2021_df = transform_fact_general_payment(payments_2021_df)


payments_all_df = pd.concat([payments_2020_df, payments_2021_df])
gpo_all_df = pd.concat([payments_gpo_2020_df, payments_gpo_2021_df])

payments_all_df.columns = payments_all_df.columns.str.lower()
gpo_all_df.columns = gpo_all_df.columns.str.lower()


# upload payments file to S3
payments_all_file_name = f"{transform_s3_key}fact_general_payments.csv"
upload_dataframe_to_s3_csv(payments_all_df, bucket_name, payments_all_file_name)

gp_gpo_file_name = f"{transform_s3_key}general_dim_manufacture_gpo.csv"
upload_dataframe_to_s3_csv(gpo_all_df, bucket_name, gp_gpo_file_name)

# load to rds

def load_general_to_rds():
    print('loading general file.')
    general_obj = get_s3_obj(s3_aws_access_key_id, s3_secret_access_key, aws_region, bucket_name,f'{transform_s3_key}{fact_general_payments_file}')
    general_df = read_chunks_bytes(general_obj, 50000)
    load_to_rds(general_df, rds_host, rds_port, rds_db, rds_username, rds_pw, general_table, create_general_sql)
    
load_general_to_rds()

