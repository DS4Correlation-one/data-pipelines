from pymongo import MongoClient
from urllib.parse import quote_plus
import csv
import os
from dotenv import load_dotenv

load_dotenv()

#fetch data from the cleaned-data2020.csv file
def get_physicians_data_csv():
    csv_file_path =  os.path.join('resources', 'cleaned-data2020.csv')
    doctors_details = list()

    with open(csv_file_path, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)
        for row in csv_reader:

            doctors_details.append({'physician_first_name': row[2], 'physician_last_name': row[3],
                                   'recipient_primary_business_street_address_line1': row[4], 'recipient_zip_code': row[7], 'physician_npi': row[1]})
    return doctors_details

#fetch data from the ratesmd collection
def get_doctors_data_rate(doctor_info):
    doctor_info['_id'] = str(doctor_info['_id'])
    return {
        'review_id': doctor_info["id"],
        'rating':  doctor_info["rating"]['average'],
    }

# helper function for saving data to a csv file
def save_data_to_csv(file_data):
    file_path =  os.path.join('resources', 'doctors_ratings.csv')
    field_names = ['review_id',
                   'doctor_first_name',
                   'doctor_last_name',
                   'rating',
                   'physician_npi'

                   ]

    file_exists = os.path.isfile(file_path)

    with open(file_path, mode='a', newline='') as csv_file:
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
            'location.postal_code': physician['recipient_zip_code'],  # 2832
            "location.city.country_name": 'United States',
            "$or": [
                {'full_name': {
                    "$regex": f"^Dr\\. {physician['physician_first_name']} .* {physician['physician_last_name']}$"

                }}, {'full_name': {
                    "$regex": f"^Dr\\. {physician['physician_first_name']} {physician['physician_last_name']}$"

                }}
            ]
        }
        query_minus_zip_code = query = {

            "location.city.country_name": 'United States',
            "$or": [
                {'full_name': {
                    "$regex": f"^Dr\\. {physician['physician_first_name']} .* {physician['physician_last_name']}$"

                }}, {'full_name': {
                    "$regex": f"^Dr\\. {physician['physician_first_name']} {physician['physician_last_name']}$"

                }}
            ]
        }

        doctor_info = ratemdDB.find_one(query) if ratemdDB.find_one(
            query) else ratemdDB.find_one(query_minus_zip_code)
        if doctor_info:
            doctor_data_rate = get_doctors_data_rate(doctor_info)
            dr_info = {
                'doctor_first_name':  physician['physician_first_name'],
                'doctor_last_name':  physician['physician_last_name'],
                'physician_npi': physician['physician_npi']

            }
            save_data_to_csv({**doctor_data_rate, ** dr_info })
            

# connect to database 

user = os.getenv('user')
password = os.getenv('pwd')
host = os.getenv('host')

uri = "mongodb://%s:%s@%s" % (
    quote_plus(user.encode('utf-8')), quote_plus(password.encode('utf-8')), quote_plus(host.encode('utf-8')))
client = MongoClient(uri)

db = client['healthrate']

ratemdDB = db.ratemd

# Invoke main function to save data to doctors_rating.csv
save_doctor_ratings_csv()
