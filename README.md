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

## Ingestion
### Design Documentation for Ingestion
The data ingestion process is outlined as follows:

1. **GET Request to APIs:**
    - A GET request is made to the provided API links with specific parameters to maximize data retrieval from the sources. No API registration or key is required for access.

2. **Data Retrieval in JSON Format:**
    - Upon a successful GET request, data is obtained in JSON format.

3. **Data Transformation to CSV:**
    - The JSON data is then transformed into CSV files, which are more suitable for data analysis.

### Storage Location
The converted CSV files are stored in an Azure Storage account:

- **Azure Storage Account:** f23projectstorageaccount
    - **Root Container:** f23projectcontainer
        - **Datasets Container:**
            - **raw_data:**
                - Two folders hold data from the two data sources:
                    - **Average-Interest-Rates-on-U.S.-Treasury Securities**
                    - **U.S.Treasury-Monthly-Statement-of-the-Public-Debt-(MSPD)**
            - **processed_data:**
                - These folder store processed data, typically after some data cleaning has been performed.      