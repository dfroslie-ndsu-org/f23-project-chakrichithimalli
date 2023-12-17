[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-718a45dd9cf7e7f842a935f5ebbe5719a5e09af4491e668f4dbf3b35d5cca122.svg)](https://classroom.github.com/online_ide?assignment_repo_id=12395550&assignment_repo_type=AssignmentRepo)
# CSCI 622 Project - Chakri Chithimalli
## Project Overview
The Treasury Interest Rates project focuses on analyzing and visualizing data related to U.S. Treasury securities. The project utilizes two datasets, one containing interest rates of these securities, and the other providing a debt summary categorized as public and intragovernmental. Through data analysis and visualization, we aim to help investors identify potential areas for investment in U.S. Treasury securities. Here, we focus on marketable and non-marketable securities.

### Data Sources
1. **Average Interest Rates in U.S. Treasury Securities:** This dataset provides historical records of interest rates associated with a wide range of U.S. Treasury securities. These securities include Bills, Notes, Bonds, Inflation-Indexed Notes, and more. By examining this dataset, we can trace the evolution of interest rates over time, across different security types. This information is vital for investors who seek to understand how yields on these securities have fluctuated in response to various economic factors.
    - Source: [Average Interest Rates on U.S. Treasury Securities](https://fiscaldata.treasury.gov/datasets/average-interest-rates-treasury-securities/average-interest-rates-on-u-s-treasury-securities)

2. **U.S. Treasury Monthly Statement of Public Debt:** The second dataset presents a holistic summary of the debt tied to U.S. Treasury securities. It categorizes the debt into two main categories—publicly held and intragovernmental holdings. These categorizations offer an essential perspective on the overall financial health of the U.S. government and its liabilities to different entities. Understanding the debt structure is particularly relevant for policymakers and economists.
    - Source: [Summary of Treasury Securities Outstanding](https://fiscaldata.treasury.gov/datasets/monthly-statement-public-debt/summary-of-treasury-securities-outstanding)

   These two datasets belong to a single parent source, [fiscaldata.treasury.gov](https://fiscaldata.treasury.gov/), and do not require any registration or API key for access.
## Business Problem
Investors face the challenge of identifying high-performing securities to strategically allocate capital and achieve substantial returns on investment. The complexity lies in the need to pinpoint securities that exhibit strong performance, making it crucial for investors to make informed decisions about where to invest their capital for optimal profitability.

## Business Solution
The data from the datasets are used for analysis and it answers the following questions:
 - Which category has more holdings? 
 - Which category has more public holdings? 
 - The average interest return for 'Marketable' securities in the last 2 years is? 
 - The average interest return for 'Non-marketable' securities in the last 2 years is?
 - Top 3 marketable securities with good returns are:
 - Top 3 non-marketable securities with good returns are :
 - The security with the highest average interest rate in the current year is:
## Ingestion
### Design Documentation for Ingestion
The data ingestion process is outlined as follows:

1. **GET Request to APIs:**
    - A GET request is made to the provided API links with specific parameters to maximize data retrieval from the sources. No API registration or key is required for access.

2. **Data Retrieval in JSON Format:**
    - Upon a successful GET request, data is obtained in JSON format.

3. **Data Transformation to CSV:**
    - The JSON data is then transformed into CSV files, which are more suitable for data analysis.

4. **Data Cleaning:**
    - The CSV data is now cleaned by removing invalid secuirties data from the datasets.
### Storage Location
The converted and cleaned CSV files are stored in an Azure Storage account:
- **Azure Storage Account:** f23projectstorageaccount
    - **Root Container:** f23projectcontainer
        - **Datasets Folder:**
            - **processed_data:**
                - Two folders hold data from the two data sources after some cleaning has been done:
                    - **Average-Interest-Rates-on-U.S.-Treasury Securities**
                    - **U.S.Treasury-Monthly-Statement-of-the-Public-Debt-(MSPD)**
            - **serving_data:**
                - This folder stores the processed data, typically after data transformation is done, joining the datasets and this data here will be ready for serving. 

## Transformation
### Design Documentation for Transformation
The data transformation process is outlined as follows:
1. **Using the cleaned data:**
    - In this phase the dataset which were cleaned are used to join the datasets together with the required columns that are needed for analysis which will help in answering the questions.
2. **Joining the datasets:**
    - Joining of the datasets are done using two attributes "record_date" and "security_desc" from dataset1 and "record_date" and "security_class_desc" from dataset2 using inner join.
3. **Storing the files in azure storage:**
    - The joined data is now stored as CSV file in the Azure storage container(as serving_data) and also the same data is inserted in Azure SQL database.

## Serving
### Design Documentation for Serving
The data serving process is outlined as follows:
1. **Using the data for analysis:**
    - The data is fetched for analysis from the Azure SQL database and analysis is performed for getting the answers for the specified **"Business Solution"**.
2. **Using the data for Data Visulaization**
    - The data which is ready to serve is read in Power Bi for data visualization from the Azure SQL database, and some visualizations are performed on the secuirties data, which helps in viewing the data in graphs formats like bar, plot and using filters the data can be viewed more specific to a security and time.

## Technologies Used:
1. **Python**
    - Libraries: pandas, requests, pyodbc, concurrent, configparser, StringIO.

2. **Azure Cloud Services:**
    - Storage account, server, key vault, container, SQL database.

3. **Power BI:**
    - Utilized for creating interactive and insightful data visualizations.

## Tools Required:
1. **IDE**
    - IDE for running the code(Visual studio or Intellij
2. **Azure Data Studio**
    - Used for accessing the Azure SQL database and checking the data tables and data.

## Implementation details
1. **What cloud resources would be required to reproduce the work?**
    - Azure Cloud resources include a resource group, storage account, Azure SQL database, Azure SQL server, and Key Vault.
2. **What steps would be required to refresh the data, perform the transformations, and serve the data for business consumption?**
    - Clone the repository and open it in an integrated development environment (IDE) like Visual Studio or IntelliJ.
    - Configure the Azure storage account connection string and Azure SQL database password by creating an Account_key.config file in the project. Add the following variables:
      - **[CONNECTION-STRING]
        CONNECTION-STRING=** DefaultEndpointsProtocol=https;AccountName="ReplaceWithYourStorageAccount";AccountKey="ReplaceWithYourAccountKey"";EndpointSuffix=core.windows.net
      - **[DB-PASSWORD-CONNECTION-STRING]
        DB-PASSWORD-CONNECTION-STRING=** DRIVER="replace if using a different one {ODBC Driver 18 for SQL Server}";Server="ReplaceWithYourSQLDatabaseServer";Database="ReplaceWithYourDatabaseName";Uid="ReplaceWithYours";Pwd="ReplaceWithYourPasswordFromKeyVault";Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30
      - After doing the above, running the data_engineering_script.py will get the latest data available from the sources, perform the data transformation and get the data ready for serving.
      - After the script got successfully ran, the answers to the questions gets saved in the analysis_answers.txt file.
      - The database server name and other following credentials need to be used to connect the Azure SQL database in Power Bi and the file in the project can be used for getting the visual on the latest data.
3. **An overview of how the code is structured in the repo**
    - The src/ingestion folder contains scripts for data engineering(DataEngineering_Script.py) and exploratory analysis(ExploratoryAnalysis.py).
    - src/project_logs stores error logs in the error.log file.
    - src/project_logs stores the info logs in the scripts.log file.
    - src/project_logs stores the answers for the business problem in analysis_answers.txt file.
    - src/SQL_Transformation includes the DDL_scripts.sql file.
    - The SupplementaryInfo/IngestionAnalysis folder holds evidence related to the project's architecture diagram, serving_data file, exploratory analysis documents, power bi visualization file(U.S.Treasury_Interest_Rates.pbix) and resources.
