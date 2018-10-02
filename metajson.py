import os
import numpy as np
import pandas as pd
import re
import argparse
import os.path, time

import urllib.request, json

parser = argparse.ArgumentParser()

parser.add_argument('-in1', required=True)

args= parser.parse_args()

input_file = args.in1

exc = pd.read_csv(input_file)

filename_re=re.search(r'^(/[^/ ]*)+/?$',input_file).group(1)

file_name = filename_re.strip("/")
filename=file_name.split(".")[0]

#writing Objects sheet
obj = pd.DataFrame(data = {'Database name':['mdr2'],'Schema name':['cod'],'Object name':[filename],'Object Creation time':[time.ctime(os.path.getctime(input_file))],'Object modified time':[time.ctime(os.path.getmtime(input_file))]})
print(obj)



json_data=open(input_file).read()
fields = json.loads(json_data)

df = pd.DataFrame.from_dict(fields, orient='columns')
df['Object Property Number']=df.index+1
df['object']=filename
df['data_type']=0
df['Is Nullable']=""
df['Is Primary Key']=""
df['Is Foriegn Key']=""
df['Description text']=""
df['Data Type precision Number']=""
df['Data Type Scale Number']=""
df['Object Property Creation Date']=time.ctime(os.path.getctime(input_file))
df['Object Property Modification Date']=time.ctime(os.path.getmtime(input_file))

#reading types.csv for mapping datatypes of Esri
types = pd.read_csv("/home/hduser/python/types.csv")
types.columns=['type','data_type']

#Mapping both data frames and cleaning it
df['data_type']=df[['type']].merge(types,how='left').data_type
df = df.replace(np.nan, '', regex=True)
df['index']=df.index+1
del df['alias']
del df['type']
print(df)

#output of template as csv
#df.to_csv('meta_template.csv')

#print object_property
writer = pd.ExcelWriter('MetaDatafromJson.xlsx', engine='xlsxwriter')

# Write each dataframe to a different worksheet.
obj.to_excel(writer, sheet_name='Objects')
df.to_excel(writer, sheet_name='Object Property')

# Close the Pandas Excel writer and output the Excel file.
writer.save()
