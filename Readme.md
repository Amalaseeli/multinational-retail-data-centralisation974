
### Milestone 1
### Extract and Clean the Data from the Data Sources
In this milestone, we will focus on extracting data from the various sources, cleaning it, and setting up a local PostgreSQL database to store the extracted data for future analysis and centralization.

### Task 1: Set Up a Local PostgreSQL Database
We'll be using PostgreSQL along with pgAdmin4 to create a new database for storing all extracted data.

1. **Install PostgreSQL** and **pgAdmin4**
Ensure you have PostgreSQL and pgAdmin4 installed on your local system. You can download them from:
- [PostgreSQL Downloads](https://www.postgresql.org/download/)
- [Download pgAdmin4](https://www.pgadmin.org/download/)
2. Creating a New Database in pgAdmin4
Follow these steps to set up your new database:
- Open **pgAdmin4** and log in.
- Right-click on **“Databases”** → **“Create”** → **“Database”**.
- Name your database, for example, sales_data.
- Click **“Save”.**

### Task 2: Initialise three project classes.
In this task we will be defining the scripts and Classes use to extract and clean the data from multiple data sources.
The project involves the creation of three Python scripts:
1. **data_extraction.py**
2. **database_utils.py**
3. **data_cleaning.py**

## Project Structure

### 1. data_extraction.py
This script contains the class **DataExtractor**. It is responsible for extracting data from various sources, such as:
- **CSV files**
- **APIs**
- **S3 Buckets**

### 2. database_utils.py
This script defines the class ***DatabaseConnector*** which helps to establish connections to a PostgreSQL database and upload data.

### 3. data_cleaning.py 
This script will contain a class **DataCleaning** which responsible for cleaning data extracted from different sources.

### Task 3: Extract and clean user data
In this task we will **extract user data**  stored in an **AWS RDS database** and clean it for further use.
The process involves reading database credentials, establishing a connection to the database, and listing the available tables.

Database connector class contains three methods.
1. Create db_creds.yaml file which containing database credientials.
2. The DatabaseConnector class in the database_utils.py script contains four key methods:
    - **read_db_creds:** Read credientials and return dictonary of credientials. 
    - **init_db_engine:**  Initializes a connection engine to the **RDS database** using the credentials retrieved from the read_db_creds() method.
    - **list_db_tables:** List all the table in the database.
    - **Upload_to_db:** Stores the cleaned user data in the PostgreSQL database table `dim_users`.

###  Task 4 : Extract and clean card details.
In this task extract users card details stored in a PDF document in an AWS S3 bucket. 
> ``` Install the Python package tabula-py this will help you to extract data from a pdf document```
- Clean the extracted data using the DataCleaning class and upload it to the PostgreSQL database in the `dim_card_details` table.

### Task 5 : Extract and clean the details of each store
In this task the store data using an API.
- The API has two GET methods. 
    1. **list_number_of_stores():** Returns the number of stores in the business.
    2. **retrieve_stores_data():** Retrieves details of each store based on the store number.

- In the DataCleaning class includes called_clean_store_data method to clean the store data.
- Upload the cleaned data to the PostgreSQL database in `dim_store_details` table.

### Task 6 : Extract and clean the product details.
In this task we extract the information for each product the company currently sells stored in CSV format in AWS S3 bucket.
> boto3 package will used to download and extract data from AWS s3 bucket.
After extracting data in the datacleaning class contain two methods.
- **convert_product_weights:** Remove Excess Characters and standardize all values to kilograms.
- **clean_products_data:** Clean extracted products data.
- Upload the cleaned data to the PostgreSQL database in `dim_products` table.

### Task 6 : Retrieve and clean the orders table.
In this task, extract the orders data using the `read_rds_table` method we created earlier, clean it for further use.
- After clean orders data upload the data to PostgreSQL database in `orders_table` table.

### Task 7 : Retrieve and clean the orders date events data.
In this task, we will extract data stored in a JSON format from an AWS S3 bucket, clean it for further use, and then upload the cleaned data to a PostgreSQL table named `dim_date_times`.

### Milestone 2
### Create the database schema
In this task devlop the star-based schema of the database and convert the column in to the correct data types.Create the primary key and foreign key constraint.
