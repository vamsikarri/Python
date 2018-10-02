import glob
import pyodbc
import csv
import os
import settings
import create_table
import blob
import factory
import initialization_exception as ie


con = pyodbc.connect('DRIVER=' + settings.driver + ';SERVER=' + settings.server + ';PORT=1443;DATABASE=' + settings.database +
                     ';UID=' + settings.username + ';PWD=' + settings.password)
cursor = con.cursor()
con.autocommit = True

try:
    if settings.upload_blobs:
        blob.initialize()
except ie.InitializationException:
    print("Could not finish initialization.  Aborting.")
    exit(1)

for file in glob.glob(settings.upload_directory + '*.csv'):
    success = False
    with open(file, 'r', encoding=settings.encoding_name) as f:
        reader = csv.reader(f, delimiter=settings.column_delimiter)
        blob_name_index = file.rfind('\\')
        blob_name = file if blob_name_index == -1 else file[len(settings.upload_directory):]
        create_table.create_table(cursor, blob_name, reader)
        qualified_table_name = '['+settings.schema_name+'].['+create_table.filename_to_tablename(blob_name)+']'
        try:
            if settings.upload_blobs:
                blob.upload_blob(settings.container_name, file, blob_name)
            factory_manager = factory.FactoryManager()
            success = factory_manager.copy(blob_name, qualified_table_name)
        except Exception as e:
            print(str(e))
            print('Aborting copying of ' + blob_name)
    if success:
        print('Successfully copied '+blob_name+' to '+qualified_table_name)
        out = settings.output_file_directory + blob_name
        try:
            os.rename(file, out)
            print('Moved '+file+' to '+out)
        except Exception as e:
            print(str(e))
            print('Could not move ' + file + ' to ' + out)
