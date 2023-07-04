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
from common.helpers import read_chunks
from secrets import *

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

    t1_initialize = PythonOperator(task_id='initialize_ingest',
                                   python_callable=initialize_ingest,
                                   dag=dag)

    ################################################################
    ## Research Payments Ingest ##
    ################################################################
    def ingest_research():
        logging.info('ingesting research payments file.')
        # Research Payments File Path
        research_2020_path = "https://download.cms.gov/openpayments/PGYR20_P063023/OP_DTL_RSRCH_PGYR2020_P06302023.csv"
        research_2021_path = "https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_RSRCH_PGYR2021_P01202023.csv"
        # Read Research Data for 2020 and 2021
        research_2020_df = read_chunks(research_2020_path, 50000)
        # concat research df across both years
        research_keep_col_list = ['Change_Type',
                                'ClinicalTrials_Gov_Identifier',
                                'Context_of_Research',
                                'Covered_Recipient_First_Name',
                                'Covered_Recipient_Last_Name',
                                'Covered_Recipient_License_State_code1',
                                'Covered_Recipient_License_State_code2',
                                'Covered_Recipient_License_State_code3',
                                'Covered_Recipient_License_State_code4',
                                'Covered_Recipient_License_State_code5',
                                'Covered_Recipient_Middle_Name',
                                'Covered_Recipient_Name_Suffix',
                                'Covered_Recipient_NPI',
                                'Covered_Recipient_Primary_Type_1',
                                'Covered_Recipient_Primary_Type_2',
                                'Covered_Recipient_Primary_Type_3',
                                'Covered_Recipient_Primary_Type_4',
                                'Covered_Recipient_Primary_Type_5',
                                'Covered_Recipient_Primary_Type_6',
                                'Covered_Recipient_Profile_ID',
                                'Covered_Recipient_Type',
                                'Date_of_Payment',
                                'Dispute_Status_for_Publication',
                                'Expenditure_Category1',
                                'Expenditure_Category2',
                                'Expenditure_Category3',
                                'Expenditure_Category4',
                                'Expenditure_Category5',
                                'Expenditure_Category6',
                                'Form_of_Payment_or_Transfer_of_Value',
                                'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_2',
                                'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_3',
                                'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_4',
                                'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_5',
                                'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1',
                                'Name_of_Study',
                                'Payment_Publication_Date',
                                'Product_Category_or_Therapeutic_Area_1',
                                'Product_Category_or_Therapeutic_Area_2',
                                'Product_Category_or_Therapeutic_Area_3',
                                'Product_Category_or_Therapeutic_Area_4',
                                'Product_Category_or_Therapeutic_Area_5',
                                'Program_Year',
                                'Recipient_City',
                                'Recipient_Country',
                                'Recipient_Postal_Code',
                                'Recipient_Primary_Business_Street_Address_Line2',
                                'Recipient_Primary_Business_Street_Address_Line1',
                                'Recipient_Province',
                                'Recipient_State',
                                'Recipient_Zip_Code',
                                'Record_ID',
                                'Teaching_Hospital_CCN',
                                'Teaching_Hospital_ID',
                                'Teaching_Hospital_Name',
                                'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID',
                                'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name',
                                'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State',
                                'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country',
                                'Total_Amount_of_Payment_USDollars']

        # upload dataframe as csv
        research_2020_df = read_chunks(research_2020_path, 50000)
        research_2020_df = research_2020_df[research_keep_col_list]
        # research_2020_file_name = f"{s3_key}2020_research_payments.csv"
        # upload_dataframe_to_s3_csv(
        #     research_2020_df, bucket_name, research_2020_file_name)

        research_2021_df = read_chunks(research_2021_path, 50000)
        research_2021_df = research_2021_df[research_keep_col_list]
        logging.info('Research payment file ingest complete.')
        # research_2021_file_name = f"{s3_key}2021_research_payments.csv"
        # upload_dataframe_to_s3_csv(
        #     research_2021_df, bucket_name, research_2021_file_name)
        
    t2_ingest_research = PythonOperator(task_id='ingest_research',
                                              python_callable=ingest_research,
                                              dag=dag)
   ################################################################
   ## Ownership Data Ingest ##
   ################################################################
    def ingest_ownership():
        logging.info('ingesting hospital ownership payment file.')
        ownership_2020_path = "https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_OWNRSHP_PGYR2020_P01202023.csv"
        ownership_2021_path = "https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_OWNRSHP_PGYR2021_P01202023.csv"

        # Read Ownership Data for 2020 and 2021
        ownership_2020_df = read_chunks(ownership_2020_path, 50000)
        # ownership_2020_file_name = f"{s3_key}2020_ownership_payments.csv"
        # upload_dataframe_to_s3_csv(ownership_2020_df, bucket_name, ownership_2020_file_name)

        ownership_2021_df = read_chunks(ownership_2021_path, 50000)
        logging.info('Hospital ownership payment file ingest complete.')
        # ownership_2021_file_name = f"{s3_key}2021_ownership_payments.csv"
        # upload_dataframe_to_s3_csv(ownership_2021_df, bucket_name, ownership_2021_file_name)

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
        mips_df.drop(columns=[' Org_PAC_ID', ' facility_ccn',
                              ' facility_lbn', ' Cost_category_score'], inplace=True)
        logging.info('MIPs data file ingest complete.')
        # mips_file_name = f"{s3_key}mips_score.csv"
        # upload_dataframe_to_s3_csv(mips_df, bucket_name, mips_file_name)

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
        logging.info('hospital owner data file ingest complete.')
        # hospital_file_name = f"{s3_key}hospital_owner.csv"
        # upload_dataframe_to_s3_csv(hospital_df, bucket_name, hospital_file_name)

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
        logging.info('physician profile file ingest complete.')
        # profile_file_name = f"{s3_key}physician_profile.csv"
        # upload_dataframe_to_s3_csv(profile_df, bucket_name, profile_file_name)

    t6_ingest_physician_profile = PythonOperator(task_id='ingest_physician_profile',
                                                 python_callable=ingest_physician_profile,
                                                 dag=dag)

t1_initialize >> [t2_ingest_research,t3_ingest_ownersip, t4_ingest_mips,
    t5_ingest_hospital_owner_info, t6_ingest_physician_profile]
