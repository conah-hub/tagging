import boto3
import csv
import os

s3 = boto3.client('s3')

# Specify the bucket name
bucket_name = 'YOUR_BUCKET_NAME'

# Desired tags to check for e.g desired_tags = {'Env': 'Prod'} 
desired_tags = {'TAG_SET_KEY_HERE': 'TAG_SET_VALUE_HERE'} 
matching_object_count = []

# Download path for the final CSV called matching_objects.csv
dir_path = os.getcwd()
report_download_path = str(os.path.join(dir_path, 'matching_objects.csv'))

# Function to create a CSV file:
def create_csv_file(data, filename):

    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for value in data:
            formatted_object = f"s3://{bucket_name}/{value}"
            writer.writerow([formatted_object])
        print(f"CSV file '{filename}' created successfully.\n")


# Function to check if an object has the desired tags
def has_desired_tags(bucket, key):
    try:
        response = s3.get_object_tagging(
            Bucket=bucket,
            Key=key
        )
        tags = response['TagSet']
        tag_length = len(tags)
       
        for i in range(tag_length):
            if list(desired_tags.keys())[0] == tags[i]['Key'] and list(desired_tags.values())[0] == tags[i]['Value']:
                print("-"*30)
                print(f"==> TagSet: {tags[i]} found on object '{key}'")
                matching_object_count.append(key)
                print("-"*30)
            else:
                print(f"Processed: [ {key} ] - no further TagSet found!")  
    except boto3.exceptions.ClientError as e:
        print(f"Error retrieving object tags: {e}")
        return False

# Loop through all objects in the bucket
continuation_token = None
while True:
    if continuation_token:
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            ContinuationToken=continuation_token
        )
    else:
        response = s3.list_objects_v2(
            Bucket=bucket_name
        )
    print("="*30)
    print("")
    for obj in response.get('Contents', []):
        key = obj['Key']
        if has_desired_tags(bucket_name, key):
            continue
        else:
            print(f"Processed: [ {key} ]")
    print("")
    print("="*30)
    total_objects = len(matching_object_count)
    if total_objects > 0:
        print(f"Total objects with matching TagSet: [{total_objects}]")
        create_csv_file(matching_object_count, report_download_path)
    else:
        print(f"No objects found with matching TagSet: {desired_tags}.\n")

    print("="*30)

    if 'NextContinuationToken' in response:
        continuation_token = response['NextContinuationToken']
    else:
        break


    
