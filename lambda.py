import json
import requests
import os
import snowflake.connector as sf
from dotenv import load_dotenv
import toml

# Function to load environment variables from .env file
def load_environment_variables():
    load_dotenv()

# Function to load configuration variables from config.toml file
def load_config_variables():
    return toml.load("config.toml")

# Function to retrieve data from the given URL
def fetch_data(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.content

# Function to write data to a file
def write_to_file(file_path, data):
    with open(file_path, 'wb') as file:
        file.write(data)

# Function to read data from a file
def read_from_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

# Function to establish connection to Snowflake
def connect_to_snowflake(user, password, account, warehouse, database, schema, role):
    return sf.connect(user=user, password=password, account=account,
                      warehouse=warehouse, database=database, schema=schema, role=role)

# Function to execute Snowflake SQL statements
def execute_sql(cursor, sql_statement):
    cursor.execute(sql_statement)

# Lambda handler function
def lambda_handler(event, context):
    # Load environment variables
    load_environment_variables()
    
    # Load configuration variables
    config = load_config_variables()
    
    # Retrieve variables from config.toml
    url = config['url']['inventory_url']
    destination_folder = config['url']['destination_folder']
    file_name = config['url']['file_name']
    
    # Retrieve sensitive variables from environment variables
    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    account = os.getenv('ACCOUNT')
    warehouse = os.getenv('WAREHOUSE')
    database = os.getenv('DATABASE')
    schema = os.getenv('SCHEMA')
    table = os.getenv('TABLE')
    role = os.getenv('ROLE')
    file_format_name = os.getenv('FILE_FORMAT_NAME')
    stage_name = os.getenv('STAGE_NAME')
    
    # Fetch data from the URL
    data = fetch_data(url)
    
    # Write data to a file
    file_path = os.path.join(destination_folder, file_name)
    write_to_file(file_path, data)
    
    # Print file content
    file_content = read_from_file(file_path)
    print(file_content)
    
    # Connect to Snowflake
    conn = connect_to_snowflake(user, password, account, warehouse, database, schema, role)
    cursor = conn.cursor()
    
    # Execute SQL statements
    execute_sql(cursor, f"use warehouse {warehouse};")
    execute_sql(cursor, f"use schema {schema};")
    execute_sql(cursor, f"create or replace file format {file_format_name} type ='CSV' field_delimiter=',';")
    execute_sql(cursor, f"create or replace stage {stage_name} file_format ={file_format_name};")
    execute_sql(cursor, f"put file://{file_path} @{stage_name};")
    execute_sql(cursor, f"list @{stage_name};")
    execute_sql(cursor, f"truncate table {schema}.{table};")
    execute_sql(cursor, f"copy into {schema}.{table} FROM @{stage_name}/{file_name} file_format={file_format_name} on_error='continue';")
    
    # Close Snowflake connection
    conn.close()
    
    print("Success!!!")
    return {
        'statusCode': 200,
        'body': "Success!!!"
    }
