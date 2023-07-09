import pandas as pd
import numpy as np
import requests
import json
import boto3
import io
from io import BytesIO, StringIO
from common.credentials.secrets import *


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


def upload_dataframe_to_s3_csv(dataframe, bucket_name, s3_key):
    # Convert DataFrame to CSV string
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer, index=False)
    s3 = boto3.client(
        's3',
        aws_access_key_id=s3_aws_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
        region_name=aws_region)
    # Upload the CSV string as a file to S3
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=s3_key)

def get_s3_obj(bucket_name, file_path):
    s3 = boto3.client(
        's3',
        aws_access_key_id=s3_aws_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
        region_name=aws_region)
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


if __name__ == '__main__':
    print('loading helper functions.')
