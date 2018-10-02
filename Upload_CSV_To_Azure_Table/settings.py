# Script settings
drop_table_if_existing = False
upload_blobs = True

# SQL Server info
server_name = 'gathianalyticserver'
database = 'gathianalyticSQL'
username = 'gathianalyticadmin'
password = 'password@1'
schema_name = 'dbo'
warehouse = False
upload_directory = 'upload/'
output_file_directory = 'done/'
driver = '{ODBC Driver 17 for SQL Server}'
server = server_name+'.database.windows.net'

# Blob Service info
blob_account_name = 'gathianalyticcod'
blob_account_key = 'AIKeaTT2vx3CvRTme3Zw1HyrSP2t9US51U1UNKQz8GZ6GwAno0XR5lhXTPzZFIRJpk7Sg5JMGy/a1f/bfMKPdQ=='
container_name = 'blobstaging'

# Azure info
subscription_id = 'e8833533-a491-4f0a-bcf3-10c53c6811d1'
AD_client_id = '32b40222-1b74-4ad3-8dc4-f3e85d96771d'
AD_client_secret = '6290l3Fvp5popI5/3Rm7L+9QpP03M8FMeJiXq+IfzL0='
AD_tenant_id = '65447364-7b96-4331-b0d6-77f228d073d2'
rg_name = 'gathianalyticcod'
rg_params = {'location':'eastus'}

# Data Factory info
df_name = 'gathianalyticFactory'
df_params = {'location':'eastus'}

# CSV info
# Leave as None for default values
column_delimiter = ','
row_delimiter = '\n'
first_row_as_header = True
skip_line_count = 0
quote_character = '\"'
escape_character = None
null_value = None
encoding_name = 'ISO-8859-1'
