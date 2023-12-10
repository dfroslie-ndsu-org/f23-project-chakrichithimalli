import concurrent
import configparser
import pandas as pd
import requests
from azure.storage.blob import BlobServiceClient
import logging
import logging as error_log
import pyodbc
from io import StringIO

# Configure the logging for INFO and ERROR Mode
logging.basicConfig(filename='../project_logs/scripts.log', level=logging.INFO, format ='%(asctime)s - %(levelname)s - %(message)s')
error_log.basicConfig(filename='../project_logs/error.log', level=logging.ERROR, format ='%(asctime)s - %(levelname)s - %(message)s')

# Common Configuration
container_name_base = "f23projectcontainer"
file_path = "../../Account_Key.config"
ddl_script_file_path = "../SQL_Transformation/DDL_scripts.sql"

# Configuration for U.S. Treasury Data holdings
directory_name_1 = "processed_data/U.S.Treasury-Monthly-Statement-of-the-Public-Debt-(MSPD)"
directory_name_analysis_output = "analysis_output"
upload_file_1 = "debt_statement.csv"
header_1 = ["record_date", "security_type_desc", "security_class_desc", "debt_held_public_mil_amt",
            "intragov_hold_mil_amt", "total_mil_amt", "src_line_nbr", "record_fiscal_year",
            "record_fiscal_quarter", "record_calendar_year", "record_calendar_quarter", "record_calendar_month",
            "record_calendar_day"]

# Configuration for Average Interest Rates Data
directory_name_2 = "processed_data/Average-Interest-Rates-on-U.S.-Treasury Securities"
upload_file_2 = "interest_rates.csv"
header_2 = ["record_date", "security_type_desc", "security_desc", "avg_interest_rate_amt",
            "src_line_nbr", "record_fiscal_year", "record_fiscal_quarter",
            "record_calendar_year", "record_calendar_quarter", "record_calendar_month", "record_calendar_day"]

# API Request Configuration for U.S. Treasury Data holdings
base_url_1 = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/debt/mspd/mspd_table_1"
params = {'filter': 'record_date:gte:2000-01-01', 'page[number]': 1, 'page[size]': 10000}

# API Request Configuration for Average Interest Rates Data
base_url_2 = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates"

connection_string = "CONNECTION-STRING"
DB_PASSWORD_CONNECTION_STRING = "DB-PASSWORD-CONNECTION-STRING"

# serving_data directory and file name
serving_directory_name = "serving_data"
serving_data = "serving_data.csv"

# Creating a mapping dictionary
mapping_dict = {
    'Bills': 'Treasury Bills',
    'Notes': 'Treasury Notes',
    'Bonds': 'Treasury Bonds',
    'Treasury Inflation-Protected Securities': 'Treasury Inflation-Protected Securities (TIPS)',
    'Floating Rate Notes': 'Treasury Floating Rate Notes (FRN)'
}

# text file for with analysis answers
output_file_path = "../project_logs/analysis_answers.txt"

# columns to drop
unwanted_columns = ['src_line_nbr_df1',
                    'src_line_nbr_df2',
                    'record_fiscal_year_df1',
                    'record_fiscal_quarter_df1',
                    'record_calendar_year_df1',
                    'record_calendar_quarter_df1',
                    'record_calendar_month_df1',
                    'record_calendar_day_df1',
                    'security_type_desc_df1',
                    'security_class_desc']


# Function to upload data to Azure Blob Storage
def upload_to_azure_blob_storage(container_name, blob_data, upload_file, directory_name):
    try:
        logging.info("uploading the data to the Azure Blob Storage")
        blob_service_client = BlobServiceClient.from_connection_string(read_property_from_file(file_path, connection_string))
        container_client = blob_service_client.get_container_client(container_name)

        if not container_client.exists():
            container_client.create_container()

        blob_client = container_client.get_blob_client(f"{directory_name}/{upload_file}")
        blob_client.upload_blob(blob_data, overwrite=True)
        logging.info(f"Uploaded {upload_file} to Azure Blob Storage successfully.")

    except Exception as excep:
        print(f"Error uploading to Azure Blob Storage: {str(excep)}")


# Function to read connection string from config file
def read_property_from_file(filepath, property_name):
    try:
        logging.info("Reading the "+property_name+" from the "+filepath)
        config = configparser.ConfigParser()
        config.read(filepath)

        if property_name in config and property_name in config[property_name]:
            logging.info("successfully read the "+property_name+" from the "+filepath)
            return config[property_name][property_name]
        else:
            raise KeyError(f'{property_name} key not found in the configuration file.')

    except FileNotFoundError as e:
        print(f'Error: {e.strerror} - {filepath} not found.')
        return None

    except KeyError as e:
        print(f'Error: {e.args[0]}')
        return None


# Function to convert JSON list to CSV string
def json_list_to_csv(json_data_list, header):
    if not json_data_list:
        return ''
    csv_data = [header]
    for item in json_data_list:
        row = [str(item.get(field, '')) for field in header]
        csv_data.append(row)
    return '\n'.join([','.join(row) for row in csv_data])


# Function to make API request and process data
def process_api_request(base_url, params, header, upload_file, directory_name):
    try:
        logging.info("Making a GET request to "+base_url)
        response = requests.get(base_url, params)
        if response.status_code == 200:
            logging.info(f"GET request made to" +base_url+ "was successful ")
            logging.info("Data Ingestion Starts now ")
            data = response.json()
            list_of_records = data['data']
            # data cleaning, removal of invalid data
            if upload_file == "interest_rates.csv":
                logging.info("removing unwanted reocrds")
                # Filter records based on conditions
                filtered_records = [
                 record for record in list_of_records
                 if record['security_type_desc'] in ['Marketable', 'Non-marketable']
                 if not record['security_desc'] in ['Total Marketable',
                                                    'TotalMarketable',
                                                    'Total Non-marketable',
                                                    'Hope Bonds',
                                                    'R.E.A. Series',
                                                    'Treasury Inflation-Indexed Bonds',
                                                    'Treasury Inflation-Indexed Notes']
                ]
            else:
             if upload_file == "debt_statement.csv":
                logging.info("removing unwanted reocrds")
                filtered_records = [
                    record for record in list_of_records
                    if record['security_type_desc'] in ['Marketable', 'Nonmarketable']
                    if record['record_date']
                    if not record['security_class_desc'] in ['Other',
                                                             'Hope Bonds',
                                                             'Depositary Compensation Securities',
                                                             'R.E.A. Series',
                                                             'Inflation-Indexed Bonds',
                                                             'Inflation-Indexed Bonds',
                                                             'Inflation-Indexed Notes']
                ]

            # Removing records with null or empty values
            filtered_records = [
                {k: v for k, v in record.items() if v is not None and v != ''}
                for record in filtered_records
            ]

            if upload_file == "debt_statement.csv":
                logging.info(f"adjusting the attribute names in the "+upload_file)
                # Update 'security_desc' values based on the mapping dictionary
                filtered_records = [
                    {**record,
                        'security_class_desc': mapping_dict.get(record['security_class_desc'],
                                                                record['security_class_desc'])}
                    for record in filtered_records
                ]

            # Convert the JSON data to a CSV string
            csv_data = json_list_to_csv(filtered_records, header)

            # Upload CSV data to Azure Blob Storage
            upload_to_azure_blob_storage(container_name_base, csv_data, upload_file, directory_name)

        else:
            print(f"API request failed with status code: {response.status_code}")
    except Exception as excep:
        print(f"Error making API request: {str(excep)}")


# Function to read the file from Azure Blob Storage
def read_from_azure_blob_storage(container_name, blob_name):
    try:
        logging.info("reading the data from the Azure Blob Storage")
        blob_service_client = BlobServiceClient.from_connection_string(read_property_from_file(file_path,connection_string))

        # Get a reference to the container
        container_client = blob_service_client.get_container_client(container_name)

        # Download blob
        blob_client = container_client.get_blob_client(blob_name)

        # Download blob content
        blob_content = blob_client.download_blob()

        # Read the content into a Pandas DataFrame
        df = pd.read_csv(StringIO(blob_content.readall().decode('utf-8')))
        logging.info("data has been read successfully from the Azure Blob Storage")
        return df

    except Exception as excep:
        print(f"Error reading from Azure Blob Storage: {str(excep)}")


def connect_to_azure_sql(ddl_script_file_path, df_merged, DB_PASSWORD_CONNECTION_STRING):

    try:
        logging.info("reading the required DDL statements from "+ ddl_script_file_path)

        with open(ddl_script_file_path, 'r') as script_file:
            ddl_script = script_file.read()

        # Connect to the Azure SQL Database
        with pyodbc.connect(DB_PASSWORD_CONNECTION_STRING) as connection:
            cursor = connection.cursor()
            cursor.execute(ddl_script)
            connection.commit()
            logging.info("executing the required DDL statement for the database")
            # Iterate through the DataFrame and insert rows into the SQL table
            query = """
                INSERT INTO f23_project.US_Treasury_details (
                    record_date, security_type_desc, security_desc, avg_interest_rate_amt,
                    record_fiscal_year_df2, record_fiscal_quarter_df2, record_calendar_year_df2,
                    record_calendar_quarter_df2, record_calendar_month_df2, record_calendar_day_df2,
                    debt_held_public_mil_amt, intragov_hold_mil_amt, total_mil_amt
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            # Convert DataFrame to a list of tuples
            rows_to_insert = [tuple(row) for row in df_merged.itertuples(index=False, name=None)]

            # Use executemany to insert multiple rows at once
            cursor.executemany(query, rows_to_insert)

            # Commit the changes
            connection.commit()

            logging.info('Data in Azure SQL database has been inserted successfully.')

    except Exception as e:
        print(f'Error: {e}')
        # Handle the error as needed
        return None
def read_azure_sql(DB_PASSWORD_CONNECTION_STRING):
    try:
        logging.info("Reading the data from the Azure SQL database")
        # Connect to the Azure SQL Database
        with pyodbc.connect(DB_PASSWORD_CONNECTION_STRING) as connection:
            # Query to retrieve data from the table
            query = ''' SELECT * FROM f23_project.US_Treasury_details '''

            # Execute the query and fetch the data into a DataFrame
            df = pd.read_sql(query, connection)

            return df

    except Exception as e:
        logging.error(f'Error: {e}')
        # Handle the error as needed
        return None


# List of tasks to be executed in parallel
tasks = [
    (base_url_1, params, header_1, upload_file_1, directory_name_1),
    (base_url_2, params, header_2, upload_file_2, directory_name_2)
]

# Data Ingestion starts here
# Using ThreadPoolExecutor for parallel processing
with concurrent.futures.ThreadPoolExecutor() as executor:
    # Execute the tasks asynchronously
    futures = [executor.submit(process_api_request, *task) for task in tasks]

    # Wait for all tasks to complete
    concurrent.futures.wait(futures)

print("Both datasets completed.")
# Data Ingestion ends here

# Data Transformation Starts Here
# After the tasks are completed, read the uploaded files separately
df_uploaded_1 = read_from_azure_blob_storage(container_name_base, f"{directory_name_1}/{upload_file_1}")
df_uploaded_2 = read_from_azure_blob_storage(container_name_base, f"{directory_name_2}/{upload_file_2}")

common_columns_df1 = ['record_date', 'security_desc']
common_columns_df2 = ['record_date', 'security_class_desc']

# Perform an inner join
df_merged = pd.merge(df_uploaded_2,
                     df_uploaded_1,
                     how='inner',
                     left_on=common_columns_df1,
                     right_on=common_columns_df2,
                     suffixes=('_df2', '_df1'))

df_merged = df_merged.drop(columns=unwanted_columns)

merged_csv_data = df_merged.to_csv(index=False)

# call for upload_to_azure_blob_storage: Upload CSV data to Azure Blob Storage
# Data Transformation Ends here
upload_to_azure_blob_storage(container_name_base, merged_csv_data, serving_data, "serving_data")

# Data Serving start here
# call for connect_to_azure_sql: To execute DDL Statements and insert data for serving purpose
connect_to_azure_sql(ddl_script_file_path, df_merged, read_property_from_file(file_path, DB_PASSWORD_CONNECTION_STRING))

# call for read_azure_sql: Reads the serving_data from AzureSQL Database
analysis_df = read_azure_sql(read_property_from_file(file_path, DB_PASSWORD_CONNECTION_STRING))

grouped_data = analysis_df.groupby('security_type_desc').agg({
    'debt_held_public_mil_amt': 'sum',
    'intragov_hold_mil_amt': 'sum'
})

# Calculate the total holding by summing 'debt_held_public_mil_amt' and 'intragov_hold_mil_amt'
grouped_data['total_holding'] = grouped_data['debt_held_public_mil_amt'] + grouped_data['intragov_hold_mil_amt']

# 1 Which category has more holdings
# Find the category with the highest total holding
max_holding_category = grouped_data['total_holding'].idxmax()

analysis_df['record_date'] = pd.to_datetime(analysis_df['record_date'], format='%m/%d/%Y')

# Get the latest month
latest_month = analysis_df['record_date'].max()

# Filter the DataFrame to include only the rows corresponding to the latest month
latest_month_data = analysis_df[analysis_df['record_date'].dt.month == latest_month.month]

# Separate the data into 'marketable' and 'non-marketable' categories for the latest month
marketable_data_latest = latest_month_data[latest_month_data['security_type_desc'] == 'Marketable']
non_marketable_data_latest = latest_month_data[latest_month_data['security_type_desc'] == 'Non-marketable']

# Calculate the total debt held public amount for 'marketable' and 'non-marketable' types
total_debt_marketable = marketable_data_latest['debt_held_public_mil_amt'].sum()
total_debt_non_marketable = non_marketable_data_latest['debt_held_public_mil_amt'].sum()

# 2 Which category has more public holdings
if total_debt_marketable > total_debt_non_marketable:
    max_public_holding_category = 'Marketable'
else:
    max_public_holding_category = 'Non-marketable'

analysis_df['avg_interest_rate_amt'] = pd.to_numeric(analysis_df['avg_interest_rate_amt'], errors='coerce')
analysis_df['record_date'] = pd.to_datetime(analysis_df['record_date'], format='%m/%d/%Y')

# Filter the DataFrame to include only the rows corresponding to the current year
current_year_data = analysis_df[analysis_df['record_date'].dt.year == pd.to_datetime('today').year]

# Get the security with the highest average interest rate in the current year
highest_avg_interest_security = current_year_data.loc[current_year_data['avg_interest_rate_amt'].idxmax()]

last_two_years_data = analysis_df[analysis_df['record_date'] >= pd.to_datetime('today') - pd.DateOffset(years=2)]

# Separate the data into 'marketable' and 'non-marketable' categories for the last 2 years
marketable_data_last_two_years = last_two_years_data[last_two_years_data['security_type_desc'] == 'Marketable']
non_marketable_data_last_two_years = last_two_years_data[last_two_years_data['security_type_desc'] == 'Non-marketable']

# Calculate the average interest rate for 'marketable' and 'non-marketable' types
average_interest_marketable = marketable_data_last_two_years['avg_interest_rate_amt'].mean()
average_interest_non_marketable = non_marketable_data_last_two_years['avg_interest_rate_amt'].mean()

# Calculate the average interest rate for each 'security_desc' in 'marketable' and 'non-marketable' types
average_interest_security_marketable = marketable_data_last_two_years.groupby('security_desc')['avg_interest_rate_amt'].mean()
average_interest_security_non_marketable = non_marketable_data_last_two_years.groupby('security_desc')['avg_interest_rate_amt'].mean()

# Get the top 3 'security_desc' for 'marketable' and 'non-marketable' based on average interest rates
top_3_marketable = average_interest_security_marketable.nlargest(3).index.to_list()
top_3_non_marketable = average_interest_security_non_marketable.nlargest(3).index.to_list()

# getting the answers to a text file for the questions from Business solution
with open(output_file_path, 'w') as file:
    file.write("#1 Which category has more holdings: " + str(max_holding_category) + "\n")
    file.write("#2 Which category has more public holdings: " + str(max_public_holding_category) + "\n")
    file.write(f"#3 The average interest return for 'Marketable' securities in the last 2 years is: {average_interest_marketable}\n")
    file.write(f"#3 The average interest return for 'Non-marketable' securities in the last 2 years is: {average_interest_non_marketable}\n")
    file.write(f"#4 Top 3 marketable securities with good returns are : {top_3_marketable}\n")
    file.write(f"#5 Top 3 non-marketable securities with good returns are : {top_3_non_marketable}\n")
    file.write(f"#6 The security with the highest average interest rate in the current year is: " + str(highest_avg_interest_security['security_desc']) + "\n")

# Data Serving Ends here
print("Results have been saved to:", output_file_path)
