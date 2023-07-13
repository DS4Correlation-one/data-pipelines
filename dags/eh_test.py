from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import numpy as np
import requests
import json
import boto3
import io
import logging
from io import BytesIO, StringIO
from common.helpers import read_chunks, upload_dataframe_to_s3_csv, read_chunks_bytes, get_s3_obj, load_to_rds
from common.credentials.secrets import *

logger = logging.getLogger(__name__)

# DAGs for Ingest

with DAG("etl_dag",  # Dag id
         # start date, the 1st of January 2021
         start_date=datetime(2021, 1, 1),
         description='random text',
         # Cron expression, here it is a preset of Airflow, @daily means once every day.
         schedule_interval='@daily',
         catchup=False  # Catchup
         ) as dag:

    ################################################################
    ## Initialize Ingest Job ##
    ################################################################
    def initialize_ingest():
        logging.info('initializing ingest job.')

    t0_initialize = PythonOperator(task_id='initialize_ingest',
                                   python_callable=initialize_ingest,
                                   dag=dag)

    ################################################################
    ## Research Payments Ingest ##
    ################################################################

    def ingest_research_2020():
        logging.info('ingesting 2020 research payments file.')
        # Research Payments File Path
        research_2020_path = "https://download.cms.gov/openpayments/PGYR20_P063023/OP_DTL_RSRCH_PGYR2020_P06302023.csv"
        research_2020_df = read_chunks(research_2020_path, 50000)
        # upload dataframe as csv
        upload_dataframe_to_s3_csv(s3_aws_access_key_id, s3_secret_access_key, aws_region, research_2020_df, bucket_name, f"{source_s3_key}{research_payments_2020}")
        logging.info('Research 2020 payment file ingest complete.')
        
        
    t1_ingest_research_2020 = PythonOperator(task_id='ingest_research_2020',
                                              python_callable=ingest_research_2020,
                                              dag=dag)
    
    def ingest_research_2021():
        logging.info('ingesting 2021 research payments file.')
        # Research Payments File Path
        research_2021_path = "https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_RSRCH_PGYR2021_P01202023.csv"
        research_2021_df = read_chunks(research_2021_path, 50000)
        upload_dataframe_to_s3_csv(s3_aws_access_key_id, s3_secret_access_key, aws_region, research_2021_df, bucket_name, f"{source_s3_key}{research_payments_2021}")
        logging.info('Research 2021 payment file ingest complete.')
        
    t1_ingest_research_2021 = PythonOperator(task_id='ingest_research_2021',
                                              python_callable=ingest_research_2021,
                                              dag=dag)
   
    ################################################################
    ## General Payments Ingest ##
    ################################################################
    
    # def ingest_general_2020():
    #     logging.info('ingesting 2020 general payments file.')
    #     # General Payments File Path
    #     payments_2020_path = "https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_GNRL_PGYR2020_P01202023.csv"
    #     payments_2020_df = read_chunks(payments_2020_path, 10000)
    
    # def ingest_general_2021():
    #     logging.info('ingesting 2021 general payments file.')
    #     # General Payments File Path
    #     payments_2021_path ="https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_GNRL_PGYR2021_P01202023.csv"
    #     payments_2021_df = read_chunks(payments_2021_path, 10000)
   
    # t2_ingest_general_2020 = PythonOperator(task_id='ingest_general_2020',
    #                                           python_callable=ingest_general_2020,
    #                                           dag=dag)
   
    # t2_ingest_general_2021 = PythonOperator(task_id='ingest_general_2021',
    #                                           python_callable=ingest_general_2021,
    #                                           dag=dag)
   
   ################################################################
   ## Ownership Data Ingest ##
   ################################################################
    def ingest_ownership():
        logging.info('ingesting hospital ownership payment file.')
        ownership_2020_path = "https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_OWNRSHP_PGYR2020_P01202023.csv"
        ownership_2021_path = "https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_OWNRSHP_PGYR2021_P01202023.csv"

        # Read Ownership Data for 2020 and 2021
        ownership_2020_df = read_chunks(ownership_2020_path, 50000)
        upload_dataframe_to_s3_csv(s3_aws_access_key_id, s3_secret_access_key, aws_region, ownership_2020_df, bucket_name, f"{source_s3_key}{ownership_payments_2020}")

        ownership_2021_df = read_chunks(ownership_2021_path, 50000)
        upload_dataframe_to_s3_csv(s3_aws_access_key_id, s3_secret_access_key, aws_region, ownership_2021_df, bucket_name, f"{source_s3_key}{ownership_payments_2021}")
        logging.info('Hospital ownership payment file ingest complete.')

    t3_ingest_ownersip = PythonOperator(task_id='ingest_ownership',
                                              python_callable=ingest_ownership,
                                              dag=dag)

    ################################################################
    ## MIPs Ingest ##
    ################################################################
    def ingest_mips():
        logging.info('ingesting MIPs data file.')
        # MIPS Ratings
        mips_data_path = 'https://data.cms.gov/provider-data/sites/default/files/resources/a0f235e13d54670824f07977299e80e3_1676693125/ec_score_file.csv'

        # MIPS Data
        mips_df = pd.read_csv(mips_data_path)
        upload_dataframe_to_s3_csv(s3_aws_access_key_id, s3_secret_access_key, aws_region, mips_df, bucket_name, f"{source_s3_key}{mips_file}")
        logging.info('MIPs data file ingest complete.')

    t4_ingest_mips = PythonOperator(task_id='ingest_mips',
                                    python_callable=ingest_mips,
                                    dag=dag)

    ################################################################
    ## Hospital Owner Ingest ##
    ################################################################
    def ingest_hospital_owner():
        logging.info('ingest hospital owner file.')

        # Hospital Owner API URL
        hospital_url = "https://data.cms.gov/data-api/v1/dataset/029c119f-f79c-49be-9100-344d31d10344/data"

        # hospital owner data
        hospital_response = requests.get(hospital_url)
        hospital_df = pd.DataFrame(hospital_response.json())
        upload_dataframe_to_s3_csv(s3_aws_access_key_id, s3_secret_access_key, aws_region, hospital_df, bucket_name, f"{source_s3_key}{hospital_owner}")
        logging.info('hospital owner data file ingest complete.')

    t5_ingest_hospital_owner_info = PythonOperator(task_id='ingest_hospital_owner',
                                              python_callable=ingest_hospital_owner,
                                              dag=dag)

    ################################################################
    ## Hospital Owner Ingest ##
    ################################################################
    def ingest_physician_profile():
        logging.info('ingest physcian profile file.')
        # Physician Profile File Path
        profile_path = "https://download.cms.gov/openpayments/PHPRFL_P012023/OP_CVRD_RCPNT_PRFL_SPLMTL_P01202023.csv"
        # # Read Physician Profile Supplement - no concatenation
        profile_df = read_chunks(profile_path, 50000)
        upload_dataframe_to_s3_csv(s3_aws_access_key_id, s3_secret_access_key, aws_region, profile_df, bucket_name, f"{source_s3_key}{physician_profile}")
        logging.info('physician profile file ingest complete.')

    t6_ingest_physician_profile = PythonOperator(task_id='ingest_physician_profile',
                                                 python_callable=ingest_physician_profile,
                                                 dag=dag)
    
    ################################################################
    ## MIPS ETL ##
    ################################################################
    def mips_etl():
        logging.info('transforming mips file.')
        mips_object = get_s3_obj(s3_aws_access_key_id, s3_secret_access_key, aws_region, bucket_name,f'{source_s3_key}{mips_file}')
        fact_mips = read_chunks_bytes(mips_object, 50000)
        # transform data
        columns_to_drop = [' facility_ccn',' facility_lbn',' Cost_category_score', ' lst_nm', ' frst_nm']
        fact_mips.drop(columns_to_drop, axis=1, inplace=True)
        # Renaming columns for consistency 
        fact_mips.rename(columns = {' Org_PAC_ID':'Org_PAC_ID',
                              ' source': 'Source',
                              ' Quality_category_score': 'Quality_Catgegory_Score',
                              ' PI_category_score': 'PI_Category_Score',
                              ' IA_category_score': 'IA_Category_Score',
                             ' Cost_category_score': 'Cost_Category_Score',
                             ' final_MIPS_score_without_CPB': 'Final_MIPS_Score_Without_CPB',
                             ' final_MIPS_score': 'Final_MIPS_Score'}, inplace = True)
        # check data type errors
        
        # drop Null in Org_PAC_ID row
        fact_mips.dropna(subset = ['Org_PAC_ID'], inplace=True)
        
        # Fill non-ID nulls with NULL
        fact_mips["Quality_Catgegory_Score"].fillna(0, inplace = True)
        fact_mips["PI_Category_Score"].fillna(0, inplace = True)
        fact_mips["IA_Category_Score"].fillna(0, inplace = True)
        
        # updating dataype errors
        fact_mips['Org_PAC_ID'] = fact_mips["Org_PAC_ID"].astype(int)
        fact_mips['Quality_Catgegory_Score'] = fact_mips["Quality_Catgegory_Score"].astype(int)
        fact_mips['PI_Category_Score'] = fact_mips["PI_Category_Score"].astype(int)
        fact_mips['IA_Category_Score'] = fact_mips["IA_Category_Score"].astype(int)
        fact_mips['Final_MIPS_Score_Without_CPB'] = fact_mips["Final_MIPS_Score_Without_CPB"].astype(int)
        fact_mips['Final_MIPS_Score'] = fact_mips["Final_MIPS_Score"].astype(int)
        print(fact_mips.info())
        
        # sum nulls
        null_values = fact_mips.isnull().sum()
        print(f'found {null_values} nulls')
        # fill nulls
        fact_mips["Quality_Catgegory_Score"].fillna("NULL", inplace = True)
        fact_mips["PI_Category_Score"].fillna("NULL", inplace = True)
        fact_mips["IA_Category_Score"].fillna("NULL", inplace = True)
        
        # check duplicates
        duplicate_rows=fact_mips[fact_mips.duplicated(subset=['NPI', 'Org_PAC_ID', 'Source',
           'Quality_Catgegory_Score', 'PI_Category_Score', 'IA_Category_Score',
           'Final_MIPS_Score_Without_CPB', 'Final_MIPS_Score'], keep=False)]
        print(duplicate_rows)
        
        fact_mips.columns= fact_mips.columns.str.lower()
        
        # upload transformed csv
        upload_dataframe_to_s3_csv(s3_aws_access_key_id, s3_secret_access_key, aws_region, fact_mips, bucket_name, f"{transform_s3_key}{fact_mips_file}")
        logging.info('successfully transformed mips file.')
    
    t7_mips_etl = PythonOperator(task_id='transform_mips_data',
                                                 python_callable=mips_etl,
                                                 dag=dag)
    
    ################################################################
    ## Ownership Payments ETL ##
    ################################################################
    def ownership_payment_etl():
        logging.info('transforming ownership payment file.')
        op_2020_object = get_s3_obj(s3_aws_access_key_id, s3_secret_access_key, aws_region, bucket_name,f'{source_s3_key}{ownership_payments_2020}')
        op_2021_object = get_s3_obj(s3_aws_access_key_id, s3_secret_access_key, aws_region, bucket_name,f'{source_s3_key}{ownership_payments_2021}')
        # read ownership payment data
        fact_op_2020_df = read_chunks_bytes(op_2020_object, 50000)
        fact_op_2021_df = read_chunks_bytes(op_2021_object, 50000)
        
        # concat df - create fact and dim table
        fact_ownership_payment = pd.concat([fact_op_2020_df, fact_op_2021_df])
        dim_manufacture_gpo = fact_ownership_payment[['Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID', 
                              'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name',
                              'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State',
                              'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country']]
        
        fact_ownership_payment = fact_ownership_payment[['Record_ID','Physician_Profile_ID','Physician_NPI','Total_Amount_Invested_USDollars','Value_of_Interest',
                      'Terms_of_Interest','Program_Year','Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID',
                      'Dispute_Status_for_Publication','Interest_Held_by_Physician_or_an_Immediate_Family_Member',
                      'Payment_Publication_Date','Physician_Primary_Type','Physician_Specialty']]
        fact_ownership_payment.rename(columns = {'Physician_NPI':'Covered_Recipient_NPI', 'Physician_Profile_ID': 'Covered_Recipient_Profile_ID',
                                            'Physician_First_Name': 'Covered_Recipient_Profile_First_Name',
                                            'Physician_Middle_Name': 'Covered_Recipient_Profile_Middle_Name',
                                            'Physician_Last_Name': 'Covered_Recipient_Profile_Last_Name',
                                            'Physician_Name_Suffix': 'Covered_Recipient_Profile_Suffix',
                                            'Recipient_City': 'Covered_Recipient_Profile_City',
                                            'Physician_Primary_Type': 'Covered_Recipient_Primary_Type_1',
                                            'Physician_Specialty': 'Covered_Recipient_Specialty_1',
                                            'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID':'Manufacturer_ID',
                                            'Interest_Held_by_Physician_or_an_Immediate_Family_Member':'Interest_Held_by_Physician_Family'},
                                             inplace = True)
        #Updating Date data types
        fact_ownership_payment['Payment_Publication_Date'] = pd.to_datetime(fact_ownership_payment["Payment_Publication_Date"])
        
        #check dups
        duplicate_rows=fact_ownership_payment[fact_ownership_payment.duplicated(subset=['Record_ID',
                                                                 'Covered_Recipient_Profile_ID',
                                                                 'Covered_Recipient_NPI',
                                                                'Covered_Recipient_Primary_Type_1',
                                                                'Covered_Recipient_Specialty_1',
                                                                 'Total_Amount_Invested_USDollars',
                                                                 'Value_of_Interest',
                                                                 'Terms_of_Interest',
                                                                 'Program_Year',
                                                                 'Manufacturer_ID',
                                                                 'Dispute_Status_for_Publication',
                                                                 'Interest_Held_by_Physician_Family',
                                                                 'Payment_Publication_Date'], keep=False)]
        print(duplicate_rows)
        # drop dups
        fact_ownership_payment = fact_ownership_payment.drop_duplicates()

        # Check for nulls
        null_values = fact_ownership_payment.isnull().sum()
        print(null_values)
        
        # Fill null values with 'NULL' in multiple columns
        columns_to_fill = ['Terms_of_Interest']
        fact_ownership_payment[columns_to_fill] = fact_ownership_payment[columns_to_fill].fillna('NULL')
        
        # drop Null NPI row
        fact_ownership_payment.dropna(subset = ['Covered_Recipient_NPI'], inplace=True)
        
        null_values = fact_ownership_payment.isnull().sum()
        print(f"Double check nulls: {null_values}")
        
        #check for data types
        print(fact_ownership_payment.info())
        
        fact_ownership_payment.columns = fact_ownership_payment.columns.str.lower()
        
        # upload to s3
        upload_dataframe_to_s3_csv(s3_aws_access_key_id, s3_secret_access_key, aws_region, fact_ownership_payment, bucket_name, f"{transform_s3_key}{fact_ownership_payments_file}")
        logging.info('successfully transformed fact_ownership_payments.')
        
        dim_manufacture_gpo = dim_manufacture_gpo.drop_duplicates()
        dim_manufacture_gpo.rename(columns = {'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID':'Manufacturer_ID', 
                              'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name': 'Manufacturer_Name',
                              'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State':'Manufacturer_State',
                              'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country': 'Manufacturer_Country'}, inplace = True)
        
        dim_manufacture_gpo.columns = dim_manufacture_gpo.columns.str.lower()
        
        upload_dataframe_to_s3_csv(s3_aws_access_key_id, s3_secret_access_key, aws_region, dim_manufacture_gpo, bucket_name, f"{transform_s3_key}ownership_{dim_manufacture_gpo_file}")
        logging.info('successfully transformed ownership payment dim_manufacture_gpo.')
        
    t8_op_etl = PythonOperator(task_id='transform_ownership_payments_data',
                                                 python_callable=ownership_payment_etl,
                                                 dag=dag)
        
    ################################################################
    ## Physician Recipients ETL ##
    ################################################################
    def physician_etl():
        logging.info('transforming physician recipient file.')
        profile_object = get_s3_obj(s3_aws_access_key_id, s3_secret_access_key, aws_region, bucket_name,f'{source_s3_key}{physician_profile}')
        # read profile data
        dim_physician_recipient = read_chunks_bytes(profile_object, 50000)
        dim_physician_recipient =  dim_physician_recipient[['Covered_Recipient_Profile_Type', 'Covered_Recipient_Profile_ID', 'Covered_Recipient_NPI',
                                   'Covered_Recipient_Profile_First_Name','Covered_Recipient_Profile_Middle_Name','Covered_Recipient_Profile_Last_Name', 
                                   'Covered_Recipient_Profile_Suffix','Covered_Recipient_Profile_Address_Line_1',
                                   'Covered_Recipient_Profile_City', 'Covered_Recipient_Profile_State','Covered_Recipient_Profile_Zipcode',
                                   'Covered_Recipient_Profile_Country_Name', 
                                   'Covered_Recipient_Profile_Primary_Specialty','Covered_Recipient_Profile_License_State_Code_1']]
        
        #dataframe info
        print(dim_physician_recipient.info())
        
        # Check for nulls
        null_values = dim_physician_recipient.isnull().sum()
        print(null_values)


         # Replace nulls with 'NULL'
        dim_physician_recipient["Covered_Recipient_Profile_First_Name"].fillna("NULL", inplace = True)
        dim_physician_recipient["Covered_Recipient_Profile_Middle_Name"].fillna("NULL", inplace = True)
        dim_physician_recipient["Covered_Recipient_Profile_Last_Name"].fillna("NULL", inplace = True)
        dim_physician_recipient["Covered_Recipient_Profile_Suffix"].fillna("NULL", inplace = True)
        dim_physician_recipient["Covered_Recipient_Profile_Address_Line_1"].fillna("NULL", inplace = True)
        dim_physician_recipient["Covered_Recipient_Profile_City"].fillna("NULL", inplace = True)
        dim_physician_recipient["Covered_Recipient_Profile_State"].fillna("NULL", inplace = True)
        dim_physician_recipient["Covered_Recipient_Profile_Zipcode"].fillna("NULL", inplace = True)
        dim_physician_recipient["Covered_Recipient_Profile_Country_Name"].fillna("NULL", inplace = True)
        dim_physician_recipient["Covered_Recipient_Profile_Primary_Specialty"].fillna("NULL", inplace = True)
        
        # drop nulls
        dim_physician_recipient.dropna(subset = ['Covered_Recipient_NPI'], inplace=True)

        # Check for nulls again
        null_values = dim_physician_recipient.isnull().sum()
        print(null_values)    
        
        dim_physician_recipient.columns = dim_physician_recipient.columns.str.lower()
        
        # upload to s3
        upload_dataframe_to_s3_csv(s3_aws_access_key_id, s3_secret_access_key, aws_region, dim_physician_recipient, bucket_name, f"{transform_s3_key}{dim_physician_recipient_file}")
        logging.info('successfully transformed ownership payment dim_physician_recipient.')
        
    t9_physician_etl = PythonOperator(task_id='transform_physician_profile_data',
                                                 python_callable=physician_etl,
                                                 dag=dag)

    def research_payment_etl():
        logging.info('transforming ownership payment file.')
        rp_2020_object = get_s3_obj(s3_aws_access_key_id, s3_secret_access_key, aws_region, bucket_name,f'{source_s3_key}{research_payments_2020}')
        rp_2021_object = get_s3_obj(s3_aws_access_key_id, s3_secret_access_key, aws_region, bucket_name,f'{source_s3_key}{research_payments_2021}')
        # read ownership payment data
        fact_rp_2020_df = read_chunks_bytes(rp_2020_object, 50000)
        fact_rp_2021_df = read_chunks_bytes(rp_2021_object, 50000)
        
        # concat df - create fact and dim table
        fact_research_payment = pd.concat([fact_rp_2020_df, fact_rp_2021_df])
        
        dim_manufacture_gpo = fact_research_payment[['Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID', 
                              'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name',
                              'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State',
                              'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country']]
        
        fact_research_payment = fact_research_payment[['Record_ID', 'Covered_Recipient_NPI',
                          'Covered_Recipient_Profile_ID', 'Covered_Recipient_Type','Date_of_Payment',
                           'Covered_Recipient_Primary_Type_1','Covered_Recipient_Specialty_1',
                          'Form_of_Payment_or_Transfer_of_Value','Total_Amount_of_Payment_USDollars',
                          'Product_Category_or_Therapeutic_Area_1','Product_Category_or_Therapeutic_Area_2','Product_Category_or_Therapeutic_Area_3',
                          'Product_Category_or_Therapeutic_Area_4','Product_Category_or_Therapeutic_Area_5','Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID', 'Program_Year']]
        
        # Checking for data type errors
        # Updating Date data types
        fact_research_payment['Date_of_Payment'] = pd.to_datetime(fact_research_payment["Date_of_Payment"])
        
        # Check for nulls
        null_values = fact_research_payment.isnull().sum()
        print(null_values)

        # drop nulls for Profile ID 
        fact_research_payment.dropna(subset = ['Covered_Recipient_Profile_ID'], inplace=True)
        fact_research_payment.dropna(subset = ['Covered_Recipient_NPI'], inplace=True)

        # Fill non-ID nulls with NULL

        fact_research_payment["Product_Category_or_Therapeutic_Area_1"].fillna("NULL", inplace = True)
        fact_research_payment["Product_Category_or_Therapeutic_Area_2"].fillna("NULL", inplace = True)
        fact_research_payment["Product_Category_or_Therapeutic_Area_3"].fillna("NULL", inplace = True)
        fact_research_payment["Product_Category_or_Therapeutic_Area_4"].fillna("NULL", inplace = True)
        fact_research_payment["Product_Category_or_Therapeutic_Area_5"].fillna("NULL", inplace = True)


        null_values = fact_research_payment.isnull().sum()
        print(null_values)
    
        # Check for duplicates
        duplicate_rows=fact_research_payment[fact_research_payment.duplicated(subset=['Record_ID',
                                                     'Covered_Recipient_NPI',
                                                     'Covered_Recipient_Profile_ID',
                                                     'Covered_Recipient_Type',
                                                    'Covered_Recipient_Primary_Type_1',
                                                    'Covered_Recipient_Specialty_1',
                                                     'Date_of_Payment',
                                                     'Form_of_Payment_or_Transfer_of_Value',
                                                     'Total_Amount_of_Payment_USDollars',
                                                     'Product_Category_or_Therapeutic_Area_1',
                                                     'Product_Category_or_Therapeutic_Area_2',
                                                     'Product_Category_or_Therapeutic_Area_3',
                                                     'Product_Category_or_Therapeutic_Area_4',
                                                     'Product_Category_or_Therapeutic_Area_5',
                                                     'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID',
                                                     'Program_Year'], keep=False)]
        print(duplicate_rows)
        
        # Renaming columns for consistency 
        fact_research_payment.rename(columns = {'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID':'Manufacturer_ID'}, inplace = True)
        
        fact_research_payment.columns = fact_research_payment.columns.str.lower()
        
        # upload to s3
        upload_dataframe_to_s3_csv(s3_aws_access_key_id, s3_secret_access_key, aws_region, fact_research_payment, bucket_name, f"{transform_s3_key}{fact_research_payments_file}")
        logging.info('successfully transformed fact_research_payment.')
        
        # dim manufacture gpo
        # Fill non-ID nulls with NULL
        dim_manufacture_gpo["Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State"].fillna("NULL", inplace = True)
        
        # Check for duplicates
        duplicate_rows=dim_manufacture_gpo[dim_manufacture_gpo.duplicated(subset=['Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID', 
                          'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name',
                          'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State',
                          'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country'], keep=False)]
        print(duplicate_rows)
    
        # remove duplicates
        dim_manufacture_gpo = dim_manufacture_gpo.drop_duplicates()
    
        # Check for duplicates again
        duplicate = dim_manufacture_gpo[dim_manufacture_gpo.duplicated()]
        print(duplicate)
    
        # Renaming columns for consistency 
        dim_manufacture_gpo.rename(columns = {'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID':'Manufacturer_ID', 
                              'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name': 'Manufacturer_Name',
                              'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State':'Manufacturer_State',
                              'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country': 'Manufacturer_Country'}, inplace = True)

        # Check for data type errors
        print(dim_manufacture_gpo.info())
        
        dim_manufacture_gpo.columns = dim_manufacture_gpo.columns.str.lower()
        
        # upload to s3
        upload_dataframe_to_s3_csv(s3_aws_access_key_id, s3_secret_access_key, aws_region, dim_manufacture_gpo, bucket_name, f"{transform_s3_key}research_{dim_manufacture_gpo_file}")
        logging.info('successfully transformed fact_research_payment.')
        
    t10_research_payment_etl = PythonOperator(task_id='transform_research_payment_data',
                                                 python_callable=research_payment_etl,
                                                 dag=dag)
    
    ################################################################
    ## Ratings ETL ##
    ################################################################
    def ratings_etl():
        logging.info('transforming ratings file.')
        ratings_object = get_s3_obj(s3_aws_access_key_id, s3_secret_access_key, aws_region, bucket_name,f'{source_s3_key}{doctor_ratings}')
        ratings_df = read_chunks_bytes(ratings_object, 50000)
        csv_data = ratings_df.to_dict(orient='records')
        
        # parse data array
        filteredArr = []
        for entry in csv_data:
            if entry['count'] == 0  and entry['rating'] == 0.0:
                continue
            else:
                entry['npi'] = int( entry['physician_npi'])
                entry['physician_profile_id'] =  int(entry['physician_profile_id'])
                del entry['count']
                del entry['physician_npi']
                filteredArr.append(entry)
                
        fact_ratings_df = pd.DataFrame(filteredArr)
        upload_dataframe_to_s3_csv(s3_aws_access_key_id, s3_secret_access_key, aws_region, fact_ratings_df, bucket_name, f"{transform_s3_key}{fact_ratings_file}")
        logging.info('ratings file transformation complete.')
        
        
        
    t11_ratings_etl = PythonOperator(task_id='transform_ratings_data',
                                                 python_callable=ratings_etl,
                                                 dag=dag)
    
    def load_ratings_to_rds():
        logging.info('loading ratings file.')
        ratings_object = get_s3_obj(s3_aws_access_key_id, s3_secret_access_key, aws_region, bucket_name,f'{transform_s3_key}{fact_ratings_file}')
        ratings_df = read_chunks_bytes(ratings_object, 50000)
        load_to_rds(ratings_df, rds_host, rds_port, rds_db, rds_username, rds_pw, ratings_table, create_ratings_sql)
        logging.info('ratings file loaded.')
        
    t12_load_ratings = PythonOperator(task_id='load_ratings_data_to_rds',
                                                 python_callable=load_ratings_to_rds,
                                                 dag=dag)

    def load_mips_to_rds():
        logging.info('loading MIPS file.')
        mips_object = get_s3_obj(s3_aws_access_key_id, s3_secret_access_key, aws_region, bucket_name,f'{transform_s3_key}{fact_mips_file}')
        mips_df = read_chunks_bytes(mips_object, 50000)
        load_to_rds(mips_df, rds_host, rds_port, rds_db, rds_username, rds_pw, mips_table, create_mips_sql)
        logging.info('MIPS file loaded.')
        
    t13_load_mips = PythonOperator(task_id='load_mips_data_to_rds',
                                                 python_callable=load_mips_to_rds,
                                                 dag=dag)

    def load_physician_recipient_to_rds():
        logging.info('loading physician recipient file.')
        physician_object = get_s3_obj(s3_aws_access_key_id, s3_secret_access_key, aws_region, bucket_name,f'{transform_s3_key}{dim_physician_recipient_file}')
        physician_df = read_chunks_bytes(physician_object, 50000)
        load_to_rds(physician_df, rds_host, rds_port, rds_db, rds_username, rds_pw, physician_recipient_table, create_physician_sql)
        logging.info('physician recipient file loaded.')
        
    t14_load_physician = PythonOperator(task_id='load_physician_data_to_rds',
                                                 python_callable=load_physician_recipient_to_rds,
                                                 dag=dag)
    
    def load_ownership_to_rds():
        logging.info('loading ownership payment file.')
        ownership_object = get_s3_obj(s3_aws_access_key_id, s3_secret_access_key, aws_region, bucket_name,f'{transform_s3_key}{fact_ownership_payments_file}')
        ownership_df = read_chunks_bytes(ownership_object, 50000)
        load_to_rds(ownership_df, rds_host, rds_port, rds_db, rds_username, rds_pw, ownership_table, create_ownership_sql)
        logging.info('ownership payment file loaded.')
        
    t15_load_ownership = PythonOperator(task_id='load_ownership_data_to_rds',
                                            python_callable=load_ownership_to_rds,
                                            dag=dag)
    
    def load_research_to_rds():
        logging.info('loading research file.')
        research_obj = get_s3_obj(s3_aws_access_key_id, s3_secret_access_key, aws_region, bucket_name,f'{transform_s3_key}{fact_research_payments_file}')
        research_df = read_chunks_bytes(research_obj, 50000)
        load_to_rds(research_df, rds_host, rds_port, rds_db, rds_username, rds_pw, research_table, create_research_sql)
    
    t16_load_research = PythonOperator(task_id='load_research_data_to_rds',
                                            python_callable=load_research_to_rds,
                                            dag=dag)
    
    def load_gpo_to_rds():
        logging.info('loading gpo file.')
        rp_gpo = get_s3_obj(s3_aws_access_key_id, s3_secret_access_key, aws_region, bucket_name,f'{transform_s3_key}research_{dim_manufacture_gpo_file}')
        op_gpo = get_s3_obj(s3_aws_access_key_id, s3_secret_access_key, aws_region, bucket_name,f'{transform_s3_key}ownership_{dim_manufacture_gpo_file}')
        gp_gpo = get_s3_obj(s3_aws_access_key_id, s3_secret_access_key, aws_region, bucket_name,f'{transform_s3_key}general_{dim_manufacture_gpo_file}')
        rp_df = read_chunks_bytes(rp_gpo, 50000)
        op_df = read_chunks_bytes(op_gpo, 50000)
        gp_df = read_chunks_bytes(gp_gpo, 50000)
        gpo_all_df = pd.concat([rp_df, op_df, gp_df]).drop_duplicates()
        load_to_rds(gpo_all_df, rds_host, rds_port, rds_db, rds_username, rds_pw, gpo_table, create_gpo_sql)
        logging.info('gpo file loaded.')
        
    t17_load_gpo = PythonOperator(task_id='load_gpo_data_to_rds',
                                            python_callable=load_gpo_to_rds,
                                            dag=dag)
    
    # def load_general_to_rds():
    #     logging.info('loading general file.')
    #     general_obj = get_s3_obj(s3_aws_access_key_id, s3_secret_access_key, aws_region, bucket_name,f'{transform_s3_key}{fact_general_payments_file}')
    #     general_df = read_chunks_bytes(general_obj, 50000)
    #     load_to_rds(general_df, rds_host, rds_port, rds_db, rds_username, rds_pw, general_table, create_general_sql)
    
    # t18_load_general = PythonOperator(task_id='load_general_data_to_rds',
    #                                         python_callable=load_general_to_rds,
    #                                         dag=dag)

# # extraction
t0_initialize >> [t1_ingest_research_2020, t1_ingest_research_2021, t3_ingest_ownersip, t4_ingest_mips, t5_ingest_hospital_owner_info, t6_ingest_physician_profile]

# # transform
t4_ingest_mips >> t7_mips_etl
t3_ingest_ownersip >> t8_op_etl
t6_ingest_physician_profile >> t9_physician_etl
[t1_ingest_research_2020, t1_ingest_research_2021] >> t10_research_payment_etl
t0_initialize >> t11_ratings_etl

# load
# load dim tables first
t9_physician_etl >> [t14_load_physician, t17_load_gpo]
# then load fact_tables

[t11_ratings_etl, t14_load_physician, t17_load_gpo] >> t12_load_ratings
[t7_mips_etl , t14_load_physician, t17_load_gpo] >> t13_load_mips
[t8_op_etl, t14_load_physician, t17_load_gpo] >> t15_load_ownership
[t10_research_payment_etl, t14_load_physician, t17_load_gpo] >> t16_load_research