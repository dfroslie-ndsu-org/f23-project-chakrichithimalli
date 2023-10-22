import configparser
import os

import requests
from azure.storage.blob import BlobServiceClient

connection_string = "DefaultEndpointsProtocol=https;AccountName=f23projectstorageaccount;AccountKey=CM4FFzQn8za+Z1t49mntyrkZFNkq6DOfA2s6UpLmjMbMwVnrZRbd7HbQslAVXFAr5NZ8iB0J6AON+ASt/qRYzw==;EndpointSuffix=core.windows.net"
upload_file = "interest_rates.csv"
container_name = "f23projectcontainer"
directory_name = "raw_data/Average-Interest-Rates-on-U.S.-Treasury Securities"

# Azure Blob Storage Upload Function
def upload_to_azure_blob_storage(container_name, blob_data, connection_string, upload_file, directory_name):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        if not container_client.exists():
            container_client.create_container()

        blob_client = container_client.get_blob_client(f"{directory_name}/{upload_file}")
        blob_client.upload_blob(blob_data, overwrite=True)
        print(f"Uploaded {upload_file} to Azure Blob Storage successfully.")
    except Exception as e:
        print(f"Error uploading to Azure Blob Storage: {str(e)}")

# Header for CSV File
header = ["record_date", "security_type_desc", "security_desc", "avg_interest_rate_amt",
          "src_line_nbr", "record_fiscal_year", "record_fiscal_quarter",
          "record_calendar_year", "record_calendar_quarter", "record_calendar_month", "record_calendar_day"]


# file path for the.config file to read the connection string
file_path = "../../Account_Key.config"
def read_connection_string_from_configfile(filepath):
    try:
        print("Current working directory:", os.getcwd())
        config = configparser.ConfigParser()
        config.read(filepath)
        # Check if the 'CONNECTION-STRING' section exists in the file
        if 'CONNECTION-STRING' in config:
            # Check if the 'CONNECTION-STRING' key exists within the 'CONNECTION-STRING' section
            if 'CONNECTION-STRING' in config['CONNECTION-STRING']:
                return config['CONNECTION-STRING']['CONNECTION-STRING']
            else:
                raise KeyError('CONNECTION-STRING key not found in the configuration file.')
        else:
            raise KeyError('CONNECTION-STRING section not found in the configuration file.')
    except FileNotFoundError:
        raise Exception('CONNECTION-STRING configuration file not found.')


# Convert JSON List to CSV String
def json_list_to_csv(json_data_list):
    if not json_data_list:
        return ''

    csv_data = [header]

    for item in json_data_list:
        row = [str(item.get(field, '')) for field in header]
        csv_data.append(row)

    return '\n'.join([','.join(row) for row in csv_data])

# API Request Configuration
base_url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates"
params = {
    'filter': 'record_date:gte:2000-01-01',
    'page[number]': 1,
    'page[size]': 10000
}

# Making the GET request
try:
    response = requests.get(base_url, params)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        data = response.json()
        list_of_records = data['data']

        # Convert the JSON data to a CSV string
        csv_data = json_list_to_csv(list_of_records)

        # Upload CSV data to Azure Blob Storage
        upload_to_azure_blob_storage(container_name, csv_data, read_connection_string_from_configfile(file_path), upload_file, directory_name)
    else:
        print(f"API request failed with status code: {response.status_code}")
except Exception as excep:
    print(f"Error making API request: {str(excep)}")