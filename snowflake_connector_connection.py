from snowflake.connector import connect
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

# Connection variable definition
password = '<Your_Password>'
database = '<Your_Database>'
schema = '<Your_Schema>'
warehouse = '<Your_Warehouse>'
role = '<Your_Role>'

# Connect to Snowflake
conn = connect(
    account='<Your_Account>',
    user='<Your_User>',
    password=password,
    role=role,
)


# Create a cursor object using the connection
cursor = conn.cursor()

# Validate connection
try:
    cursor.execute('SELECT 1')  # Simple query to test the connection
    result = cursor.fetchone()  # Retrieve the result of the query
    if result[0] == 1:
        print('Successfully connected to Snowflake!')  # Connection validation successful
    else:
        print('Connection to Snowflake failed.')  # Connection validation failed
except Exception as e:
    print(f'An error occurred: {e}')  # Print out any error that occurs during the connection test
# Note: Do not close the cursor here.


# ---- Resource management statements ----- #

# Changing role
cursor.execute(f'USE ROLE {role}')
print(f'Role changed to {role} successfully.')

# Creating warehouse
cursor.execute(f'CREATE WAREHOUSE IF NOT EXISTS {warehouse};')
print(f'Warehouse {warehouse} created successfully.')
# Setting warehouse
cursor.execute(f'USE WAREHOUSE {warehouse};')
print(f'Warehouse {warehouse} set successfully.')

# ---- DDL statements ----- #

# Creating database
cursor.execute(f'CREATE DATABASE IF NOT EXISTS {database};')
print(f'Database {database} created successfully.')
# Setting database
cursor.execute(f'USE DATABASE {database};')
print(f'Database {database} set successfully.')

# Creating schema
cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {schema};')
print(f'Schema {schema} created successfully.')
# Setting schema
cursor.execute(f'USE SCHEMA {database}.{schema};')
print(f'Schema {schema} set successfully.')

# Creating table
table_name = 'TEST_TB'
table_structure = 'COL1 STRING, COL2 STRING'
cursor.execute(f'CREATE OR REPLACE TABLE {schema}.{table_name} ({table_structure});')
print(f'Table {table_name} created successfully.')


# ---- DML statements ----- #

# Populate table
cursor.execute(
    f'INSERT INTO {table_name}(COL1, COL2)'
    'VALUES(%s,%s)', (
     'A', 'B'
    ))
print(f'{table_name} successfully populated.')

# Query a table
cursor.execute(f'SELECT * FROM {database}.{schema}.{table_name};')
df = cursor.fetch_pandas_all()
print(df.head())

# Create a DataFrame containing data
df2 = pd.DataFrame([('Mark', 10), ('Luke', 20)], columns=['COL1', 'COL2'])

# Write the data from the DataFrame to the table named "{table_name}".
success, nchunks, nrows, _ = write_pandas(conn, df2, table_name)

# Fetch all data and print df.
cursor.execute(f'SELECT * FROM {database}.{schema}.{table_name};')
df3 = cursor.fetch_pandas_all()
print(df3.head())

# Clean up: closing cursor and connection
cursor.close()
conn.close()

