import json
import urllib.parse
import boto3
from datetime import date, datetime
import time
from os import environ

non_finance_bucket = "jarvis-sap-nonfinance-eu-dev"
finance_bucket = "jarvis-sap-finance-eu-dev"

files_correction_dict = {
    'j_mm.CSV':'j_material_movements.CSV',
    'j_Matl_Mst.CSV': 'j_Material_Master.CSV',
    'j_inv_10.CSV':'j_Inventory_Balance.CSV'
}

s3 = boto3.client('s3')

def lambda_handler(event, context):
    current_year = str(date.today().year)
    current_month = f"{date.today().month:02d}"
    current_day = f"{date.today().day:02d}"
    current_hour = datetime.now().strftime("%H")
    current_hour_minute_and_second = datetime.now().strftime("%H%M%S")
    source_bucket = event['detail']['source-bucket-name']
    original_key = urllib.parse.unquote_plus(event['detail']['object-key'], encoding='utf-8')
    filename = original_key.split('/')[-1]

    if filename in list(files_correction_dict.keys()):
        new_filename = files_correction_dict[filename]
        print(f'A filename correction is needed, the new filename is: {new_filename}')
        filename = new_filename

    if filename.startswith('j_'):
        if filename.startswith('j_fin_'):
            file_prefix = 'j_fin_'
            destination_bucket = finance_bucket
        else:
            file_prefix = 'j_'
            destination_bucket = non_finance_bucket
        if "." in filename:
            file_extension = filename.split('.')[-1]
            filename_without_extension: object = filename.rsplit('.', 1)[0].rstrip('0123456789')
            folder_name = filename_without_extension.split(file_prefix,1)[-1]
            destination_key = f"raw_data/{folder_name}/{current_year}/{current_month}/{current_day}/{filename_without_extension}_{current_hour_minute_and_second}.{file_extension}"
        else:
            filename = filename.rstrip('0123456789')
            folder_name = filename.split(file_prefix,1)[-1]
            destination_key = f"raw_data/{folder_name}/{current_year}/{current_month}/{current_day}/{filename}_{current_hour_minute_and_second}"
        copy_source = {'Bucket': source_bucket, 'Key': original_key}
        print(f"New uploaded file is: {source_bucket}/{original_key} and the Function will move it to: {destination_bucket}/{destination_key}")
        s3.copy_object(Bucket=destination_bucket, CopySource=copy_source, Key=destination_key)
        s3.delete_object(Bucket=source_bucket, Key=original_key)
