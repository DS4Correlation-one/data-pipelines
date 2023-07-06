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
from common.helpers import read_chunks, upload_dataframe_to_s3_csv
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
        upload_dataframe_to_s3_csv(research_2020_df, bucket_name, f"{source_s3_key}{research_payments_2020}")
        logging.info('Research 2020 payment file ingest complete.')
        
        
    t1_ingest_research_2020 = PythonOperator(task_id='ingest_research_2020',
                                              python_callable=ingest_research_2020,
                                              dag=dag)
    
    def ingest_research_2021():
        logging.info('ingesting 2021 research payments file.')
        # Research Payments File Path
        research_2021_path = "https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_RSRCH_PGYR2021_P01202023.csv"
        research_2021_df = read_chunks(research_2021_path, 50000)
        upload_dataframe_to_s3_csv(research_2021_df, bucket_name, f"{source_s3_key}{research_payments_2021}")
        logging.info('Research 2021 payment file ingest complete.')
        
    t1_ingest_research_2021 = PythonOperator(task_id='ingest_research_2021',
                                              python_callable=ingest_research_2021,
                                              dag=dag)
   
    ################################################################
    ## General Payments Ingest ##
    ################################################################
    
    def ingest_general_2020():
        logging.info('ingesting 2020 general payments file.')
        # General Payments File Path
        payments_2020_path = "https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_GNRL_PGYR2020_P01202023.csv"
        # payments_2020_df = read_chunks(payments_2020_path, 5000)
        payments_2020_df = pd.read_csv(payments_2020_path, low_memory=False, nrows=300000)
    
    def ingest_general_2021():
        logging.info('ingesting 2021 general payments file.')
        # General Payments File Path
        payments_2021_path ="https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_GNRL_PGYR2021_P01202023.csv"
        # payments_2021_df = read_chunks(payments_2021_path, 5000)
   
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
        upload_dataframe_to_s3_csv(ownership_2020_df, bucket_name, f"{source_s3_key}{ownership_payments_2020}")

        ownership_2021_df = read_chunks(ownership_2021_path, 50000)
        upload_dataframe_to_s3_csv(ownership_2021_df, bucket_name, f"{source_s3_key}{ownership_payments_2021}")
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
        # upload_dataframe_to_s3_csv(mips_df, bucket_name, f"{source_s3_key}{mips_file}")
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
        upload_dataframe_to_s3_csv(hospital_df, bucket_name, f"{source_s3_key}{hospital_owner}")
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
        upload_dataframe_to_s3_csv(profile_df, bucket_name, f"{source_s3_key}{physician_profile}")
        logging.info('physician profile file ingest complete.')

    t6_ingest_physician_profile = PythonOperator(task_id='ingest_physician_profile',
                                                 python_callable=ingest_physician_profile,
                                                 dag=dag)

t0_initialize >> t6_ingest_physician_profile >> t1_ingest_research_2020 >> t1_ingest_research_2021
t0_initialize >> [t3_ingest_ownersip, t4_ingest_mips, t5_ingest_hospital_owner_info]
