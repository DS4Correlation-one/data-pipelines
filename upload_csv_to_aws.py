import boto3

def upload_csv_to_aws_s3(csv_file_path, bucket_name, object_name):
    # Create an S3 client
    s3 = boto3.client('s3',  aws_access_key_id='', 
                      aws_secret_access_key='', 
                      region_name='')

    # Upload the file to S3
    try:
        s3.upload_file(csv_file_path, bucket_name, object_name)
        print("File uploaded successfully to AWS S3!")
    except Exception as e:
        print("Error uploading file to AWS S3:", str(e))

# Example usage:
csv_file_path = './resources/doctors_ratings.csv'
bucket_name = 'ds4a-dataswan'
object_name = 'doctors_ratings.csv'
upload_csv_to_aws_s3(csv_file_path, bucket_name, object_name)