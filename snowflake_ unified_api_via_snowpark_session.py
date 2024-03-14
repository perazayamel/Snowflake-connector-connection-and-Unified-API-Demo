from snowflake.core import Root
from snowflake.snowpark import Session
from snowflake.connector import connect
from snowflake.core.warehouse import Warehouse
from snowflake.core.database import Database
from snowflake.core.schema import Schema
from snowflake.core.table import Table, TableColumn
import pandas as pd

# Connection variable definition
password = '<Your_Password>'
database = '<Your_Database>'
schema = '<Your_Schema>'
warehouse = '<Your_Warehouse>'
role = '<Your_Role>'

# Connect to Snowflake
CONNECTION_PARAMETERS = {
    "account": '<Your_Account>',
    "user": '<Your_User>',
    "password": password,
    "database": database,
    "warehouse": warehouse,
    "schema": schema,
    "role": role,
}

session = Session.builder.configs(CONNECTION_PARAMETERS).create()
root = Root(session)

# ---- Resource management statements ----- #

# Creating a warehouse
my_wh = Warehouse(
  name=warehouse,
  warehouse_size="SMALL",
  auto_suspend=600,
)
root.warehouses[f'{warehouse}'].create_or_update(my_wh)
print(f'Warehouse {warehouse} created successfully.')

# Get warehouse details
warehouse_info = root.warehouses[f'{warehouse}'].fetch()
print(f"Warehouse's info {warehouse_info.to_dict()}")

# root.warehouses[f'{warehouse}'].resume()

# ---- DDL statements ----- #

# Creating a Db.
my_db = Database(name=f'{database}')
root.databases[f'{database}'].create_or_update(my_db)
print(f'Database {database} created successfully.')

# Get Db. info
db_info = root.databases[f'{database}'].fetch()
print(f'db info {db_info.to_dict()}')

# Creating a Schema
my_schema = Schema(name=f'{schema}')
root.databases[f'{database}'].schemas[f'{schema}'].create_or_update(my_schema)
print(f'Schema {schema} created successfully.')

# Get Schema info
schema_info = root.databases[f'{database}'].schemas[f'{schema}'].fetch()
print(f'Schema  info {schema_info.to_dict()}')

# Creating a table
table_name = 'TEST_TB'
my_table = Table(
  name=f"{table_name}",
  columns=[TableColumn("COL1", "string", nullable=False)
      , TableColumn("COL2", "string")]
)
root.databases[f"{database}"].schemas[f"{schema}"].tables[f"{table_name}"].create_or_update(my_table)
print(f"Table {table_name} created successfully.")

# Get table info
table_info = root.databases[f'{database}'].schemas[f'{schema}'].tables[f'{table_name}'].fetch()
print(f'{table_info.to_dict()}')


# ---- DML statements ----- #

# Populate table
query = f"INSERT INTO {database}.{schema}.{table_name}(COL1, COL2) VALUES ('A', 'B')"
session.sql(query).collect()
print(f'{table_name} successfully populated.')

# Query a table
df = session.sql(f'SELECT * FROM {database}.{schema}.{table_name}')
print(f'df count {df.count()} df show\n{df.show()}')

df2 = session.table(f'{database}.{schema}.{table_name}')
print(df2.show())

# Create a DataFrame containing data
df3 = session.create_dataframe([('Mark', 10), ('Luke', 20)], schema=['COL1', 'COL2'])
print(df3.show())

# Write the data from the DataFrame to the table named "{table_name}".
df3.write.mode('append').save_as_table(f'{database}.{schema}.{table_name}')
df4 = root.session.table(f'{database}.{schema}.{table_name}')
print(f'df count: {df4.count()}')

# Clean up: closing session
session.close()
