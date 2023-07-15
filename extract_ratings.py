from pymongo import MongoClient
from urllib.parse import quote_plus
import csv
import os
import requests
import pandas as pd
import numpy as np

def read_chunks(file_path, size):
    """
    Read staging table in chunks because it has many rows. Returns a concatenated dataframe.
    """
    data_chunks = []
    chunk_counter = 1
    for chunk in pd.read_csv(file_path, chunksize=size, low_memory=False):
        data_chunks.append(chunk)
        chunk_counter += 1
    dataset = pd.concat(data_chunks)
    return dataset

def get_physicians_data_csv():
    doctors_details = list()

    url = "https://download.cms.gov/openpayments/PHPRFL_P012023/OP_CVRD_RCPNT_PRFL_SPLMTL_P01202023.csv"
    df = read_chunks(url, 10000)
    select_columns = ['Covered_Recipient_Profile_First_Name', 'Covered_Recipient_Profile_Last_Name',
                                   'Covered_Recipient_Profile_Address_Line_1','Covered_Recipient_Profile_Zipcode', 'Covered_Recipient_NPI', 'Covered_Recipient_Profile_ID']
    doctors_details_df = df[select_columns]
    doctors_details = doctors_details_df.values.tolist()
    return doctors_details[1192089:]

#fetch data from the ratesmd collection
def get_doctors_data_rate(doctor_info):
    doctor_info['_id'] = str(doctor_info['_id'])
    return {
        'review_id': doctor_info["id"],
        'rating':  doctor_info["rating"]['average'],
        'count': doctor_info["rating"]['count'],
    }

# helper function for saving data to a csv file
def save_data_to_csv(file_data):
    field_names = ['review_id',
                   'doctor_first_name',
                   'doctor_last_name',
                   'rating',
<<<<<<< HEAD:extract_ratings.py
                   'physician_npi'
=======
                   'physician_npi',
                   'count',
                   'physician_profile_id'
>>>>>>> 9d949853c458a06ec27b22af986a2fdfbe554c96:ratesmd.py
                   ]

    file_exists = os.path.isfile('doctors_ratings.csv')

    with open('resources/doctors_ratings.csv', mode='a', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=field_names)

        if not file_exists:
            writer.writeheader()

        writer.writerow(file_data)

    return

# Invoke get_physicians_data_csv(), get_doctors_data_rate(), save_data_to_csv() function and create a doctors_ratings csv file 
def save_doctor_ratings_csv():
    physician_data = get_physicians_data_csv()
    for physician in physician_data:

        query = {
            'location.postal_code': physician[3],  # 2832
            "location.city.country_name": 'United States',
            "$or": [
                {'full_name': {
                    "$regex": f"^Dr\\. {physician[0]} .* {physician[1]}$"

                }}, {'full_name': {
                    "$regex": f"^Dr\\. {physician[0]} {physician[1]}$"

                }}
            ]
        }
        query_minus_zip_code = query = {

            "location.city.country_name": 'United States',
            "$or": [
                {'full_name': {
                    "$regex": f"^Dr\\. {physician[0]} .* {physician[1]}$"

                }}, {'full_name': {
                    "$regex": f"^Dr\\. {physician[0]} {physician[1]}$"

                }}
            ]
        }

        doctor_info = ratemdDB.find_one(query) if ratemdDB.find_one(
            query) else ratemdDB.find_one(query_minus_zip_code)
        if doctor_info:
            doctor_data_rate = get_doctors_data_rate(doctor_info)
            dr_info = {
                'doctor_first_name':  physician[0],
                'doctor_last_name':  physician[1],
                'physician_npi': physician[4],
                'physician_profile_id': physician[5]

            }
            save_data_to_csv({**doctor_data_rate, ** dr_info })
            

# connect to database 
user = "c1-ds4a-2-team-23"
password = "PhobRoswuBropRaKUM9R"
host = "20.232.135.212:27017"
uri = "mongodb://%s:%s@%s" % (
    quote_plus(user), quote_plus(password), host)
client = MongoClient(uri)

db = client['healthrate']

ratemdDB = db.ratemd

# Invoke main function to save data to doctors_rating.csv
save_doctor_ratings_csv()

