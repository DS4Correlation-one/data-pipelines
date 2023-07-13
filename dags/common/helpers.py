import pandas as pd
import numpy as np
import requests
import json
import boto3
import io
from io import BytesIO, StringIO
import psycopg2
from sqlalchemy import create_engine


def read_chunks(file_path, size):
    """
    Read staging table in chunks because it has many rows. Returns a concatenated dataframe.
    """
    data_chunks = []
    chunk_counter = 1
    print(f"Reading {file_path} in chunks...")
    for chunk in pd.read_csv(file_path, chunksize=size, low_memory=False):
        print(f"Reading chunk # {str(chunk_counter)}")
        data_chunks.append(chunk)
        chunk_counter += 1
    dataset = pd.concat(data_chunks)
    print('Read complete.')
    return dataset


def upload_dataframe_to_s3_csv(access_key, secret_key, region, dataframe, bucket_name, s3_key):
    # Convert DataFrame to CSV string
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer, index=False)
    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region)
    # Upload the CSV string as a file to S3
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=s3_key)

def get_s3_obj(access_key, secret_key, region, bucket_name, file_path):
    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region)
    s3_object = s3.get_object(Bucket=bucket_name, Key=file_path)
    return s3_object


def read_chunks_bytes(s3_obj, size):
    """
    Read S3 Object in chunks because it has many rows. Returns a concatenated dataframe.
    """
    data_chunks = []
    chunk_counter = 1
    print(f"Reading S3 Object in chunks...")
    for chunk in pd.read_csv(io.BytesIO(s3_obj['Body'].read()), chunksize=size, low_memory=False):
        print(f"Reading byte chunk # {str(chunk_counter)}")
        data_chunks.append(chunk)
        chunk_counter += 1
    dataset = pd.concat(data_chunks)
    print('Read complete.')
    return dataset


def load_to_rds(data, rds_host, rds_port, rds_dbname, rds_username, rds_password, table_name, create_sql):
    
    # establish connections
    conn_string = f'postgresql+psycopg2://{rds_username}:{rds_password}@{rds_host}/{rds_dbname}'
  
    db = create_engine(conn_string)
    conn = db.connect()
    conn1 = psycopg2.connect(
        host=rds_host,
        port=rds_port,
        database=rds_dbname,
        user=rds_username,
        password=rds_password
    )
    
    try:
        conn1.autocommit = True
        cursor = conn1.cursor()
        
        # create the table
        create_table_sql = create_sql
        cursor.execute(create_table_sql)
        
        # converting data to sql
        data.to_sql(table_name, conn, if_exists='replace', index=False, chunksize=10000, method='multi')
        
        # fetching first 10 rows
        fetch_sql=f"select * from {table_name} LIMIT 10;"
        
        cursor.execute(fetch_sql)
        for i in cursor.fetchall():
            print(i)
  
        conn1.commit()
        conn1.close()

        print("Data saved to the '{}' table successfully.".format(table_name))
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: {}".format(error))
    finally:
        cursor.close()
        conn1.close()


if __name__ == '__main__':
    print('loading helper functions.')
