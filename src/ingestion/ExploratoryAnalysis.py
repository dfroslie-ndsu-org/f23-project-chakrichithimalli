# Import necessary libraries
import configparser
import io
import os

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from azure.storage.blob import BlobServiceClient

container_name = "f23projectcontainer"
directory_name_d1 = "raw_data/Average-Interest-Rates-on-U.S.-Treasury Securities"
directory_name_d2 = "raw_data/U.S.Treasury-Monthly-Statement-of-the-Public-Debt-(MSPD)"
blob_name_d1 = "interest_rates.csv"
blob_name_d2 = "debt_statement.csv"

# file path for the.config file to read the connection string
file_path = "../../Account_Key.config"
def read_connection_string_from_configfile(filepath):
    try:
        print("Current working directory:", os.getcwd())
        config = configparser.ConfigParser()
        config.read(filepath)
        if 'CONNECTION-STRING' in config:
            if 'CONNECTION-STRING' in config['CONNECTION-STRING']:
                return config['CONNECTION-STRING']['CONNECTION-STRING']
            else:
                raise KeyError('CONNECTION-STRING key not found in the configuration file.')
        else:
            raise KeyError('CONNECTION-STRING section not found in the configuration file.')
    except FileNotFoundError:
        raise Exception('CONNECTION-STRING configuration file not found.')

# Create a BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(read_connection_string_from_configfile(file_path))

# Get a reference to the container
container_client = blob_service_client.get_container_client(container_name)

# Get a reference to the blob (file)
blob_client_d1 = container_client.get_blob_client(f"{directory_name_d1}/{blob_name_d1}")
blob_client_d2 = container_client.get_blob_client(f"{directory_name_d2}/{blob_name_d2}")

# Download the blob (file)
blob_data_d1 = blob_client_d1.download_blob()
blob_data_d2 = blob_client_d2.download_blob()
data_d1 = blob_data_d1.readall()
data_d2 = blob_data_d2.readall()

# Convert the data to a pandas DataFrame
data_df1 = pd.read_csv(io.BytesIO(data_d1), encoding='utf-8')
data_df2 = pd.read_csv(io.BytesIO(data_d2), encoding='utf-8')
pd.set_option('display.max_columns', None)


# 1. Data Inspection
print("Basic Information about the Dataset 1:")
print(data_df1.info())
print(data_df1.head(10))


# Data Summary
data_summary = data_df1.describe(include='all')

# Univariate Analysis
univariate_analysis = data_df1.describe()

# Display the results
print("Data Summary:")
print(data_summary)

print("Univariate Analysis: ")
print(univariate_analysis)

# Count the occurrences of "Marketable" and "Non-marketable"
security_counts = data_df1['security_type_desc'].value_counts()

# Calculate the percentages
total_securities = security_counts.sum()
percentage_marketable = (security_counts['Marketable'] / total_securities) * 100
percentage_non_marketable = (security_counts['Non-marketable'] / total_securities) * 100
percentage_Interest_bearing_debt = (security_counts['Interest-bearing Debt'] / total_securities) * 100

print(percentage_marketable, percentage_non_marketable, percentage_Interest_bearing_debt,  security_counts)


data_column = data_df1['avg_interest_rate_amt']

# Create a histogram
plt.hist(data_column, bins=30, color='blue', edgecolor='black')
plt.xlabel('Average Interest Rate')
plt.ylabel('Frequency')
plt.title('Histogram of Average Interest Rates')
plt.show()

# Count the occurrences of each security type
security_type_counts = data_df1['security_type_desc'].value_counts()

# Create a bar chart
plt.figure(figsize=(8, 10))
security_type_counts.plot(kind='bar')
plt.title('Security Types Distribution')
plt.xlabel('Security Type')
plt.ylabel('Count')
plt.xticks(rotation=45)
plt.show()


# Create a pivot table for visualization
pivot_table = data_df1.pivot_table(index='record_calendar_year', columns='security_type_desc', values='avg_interest_rate_amt', aggfunc='mean')
sns.heatmap(pivot_table, cmap='YlGnBu')
plt.title('Heatmap of Average Interest Rates by Year and Security Type')
plt.show()


# Box Plot
sns.boxplot(x='security_type_desc', y='avg_interest_rate_amt', data=data_df1)
plt.title('Box Plot of Average Interest Rates by Security Type')
plt.xticks(rotation=60)
plt.show()

# Calculate the count of null values in each column
null_count = data_df1.isnull().sum()
# Filter for columns with null values
null_columns = null_count[null_count > 0]
# Print the columns with null values
print(null_columns)
# print(null_count)

null_counts_by_year = data_df1.groupby('record_fiscal_year').apply(lambda x: x.isnull().sum())

# Print the columns with null values by year and their counts
for year, null_counts in null_counts_by_year.iterrows():
    print(f"Year {year}:")
    for column, count in null_counts.items():
        if count > 0:
            print(f"- {column}: {count} null values")
    print()

# Exploratory for dataset2

print("Dataset 2 -----------")

print("Basic Information about the Dataset 2:")
print(data_df2.info())
print(data_df2.head(10))

# Data Summary
data_summary = data_df2.describe(include='all')

# Univariate Analysis
univariate_analysis = data_df2.describe()

# Display the results
print("Data Summary:")
print(data_summary)

print("Univariate Analysis: ")
print(univariate_analysis)

#Box plot for debt held by security type
plt.figure(figsize=(10, 15))
sns.boxplot(x="security_type_desc", y="debt_held_public_mil_amt", data=data_df2)
plt.title("Debt Held by Security Type")
plt.xlabel("Security Type")
plt.ylabel("Debt Held by Public (Million Amount)")
plt.xticks(rotation=45)
plt.show()

# Bar plot for total nonmarketable debt by month
plt.figure(figsize=(10, 6))
sns.barplot(x="record_calendar_year", y="total_mil_amt", hue="security_type_desc", data=data_df2[data_df2['security_type_desc'] == "Nonmarketable"])
plt.title("Total Nonmarketable Debt by Month")
plt.xlabel("Calendar Year")
plt.ylabel("Total Debt (Million Amount)")
plt.xticks(rotation=45)
plt.legend(title="Security Type")
plt.show()

# Bar plot for total marketable debt by month
plt.figure(figsize=(10, 6))
sns.barplot(x="record_calendar_year", y="total_mil_amt", hue="security_type_desc", data=data_df2[data_df2['security_type_desc'] == "Marketable"])
plt.title("Total Marketable Debt by Month")
plt.xlabel("Calendar Year")
plt.ylabel("Total Debt (Million Amount)")
plt.xticks(rotation=45)
plt.legend(title="Security Type")
plt.show()

security_type_counts = data_df2['security_type_desc'].value_counts()

# Create a bar plot
plt.figure(figsize=(10, 15))
security_type_counts.plot(kind='bar', color='skyblue')
plt.title("Count of Security Types")
plt.xlabel("Security Type")
plt.ylabel("Count")
plt.xticks(rotation=45)
plt.show()

# Calculate the count of null values in each column
null_count = data_df2.isnull().sum()
# Filter for columns with null values
null_columns = null_count[null_count > 0]
# Print the columns with null values
print("Null columsn co")
print(null_columns)
# print(null_count)

# Line plot for Total Public Debt Outstanding over time
plt.figure(figsize=(12, 6))
sns.lineplot(x="record_date", y="total_mil_amt", data=data_df2)
plt.title("Total Public Debt Outstanding Over Time")
plt.xlabel("Record Date")
plt.ylabel("Total Public Debt Outstanding (Million Amount)")
plt.xticks(rotation=45)
plt.show()

debt_held_public = data_df2['debt_held_public_mil_amt']

# Create the histogram
plt.hist(debt_held_public, bins=20, color='skyblue', edgecolor='black')
plt.xlabel('Debt Held by Public (in millions)')
plt.ylabel('Frequency')
plt.title('Histogram of Debt Held by Public')
plt.show()

debt_held_public = data_df2['intragov_hold_mil_amt']

# Create the histogram
plt.hist(debt_held_public, bins=20, color='skyblue', edgecolor='black')
plt.xlabel('Debt Held by Intragovernmental (in millions)')
plt.ylabel('Frequency')
plt.title('Histogram of Debt Held Intragovernmentally')
plt.show()

security_class_counts = data_df2['security_class_desc'].value_counts()

# Create a bar graph
plt.figure(figsize=(8, 6))
security_class_counts.plot(kind='bar')
plt.title('Security Class Distribution')
plt.xlabel('Security Class')
plt.ylabel('Count')
plt.xticks(rotation=45, ha="right")  # Rotate x-axis labels for better readability
plt.show()