import os
import numpy as np
import pandas as pd
import re
import argparse
import xlsxwriter
import os.path, time

parser = argparse.ArgumentParser()

parser.add_argument('-in1', required=True)


args= parser.parse_args()

input_file = args.in1

exc = pd.read_csv(input_file)

filename_re=re.search(r'^(/[^/ ]*)+/?$',input_file).group(1)

file_name = filename_re.strip("/")
filename=file_name.split(".")[0]

#writing Objects sheet
colname=['Database name','Schema name','Object name','object creation date','object modification date']
s1=pd.Series(["mdr2","cod",input_file,time.ctime(os.path.getctime(input_file)),time.ctime(os.path.getmtime(input_file))])
obj = pd.DataFrame(data = {'Database name':['mdr2'],'Schema name':['cod'],'Object name':[filename],'Object Creation time':[time.ctime(os.path.getctime(input_file))],'Object modified time':[time.ctime(os.path.getmtime(input_file))]})
print(obj)

#Writing Object_Property sheet
colnames=['object name','object property','object number','object property created date','object property modified date']
object_property=pd.DataFrame(columns=colnames)
object_property['object property']=list(exc)
object_property['object name']=filename
object_property['object number']=object_property.index+1
object_property['object property created date']=time.ctime(os.path.getctime(input_file))
object_property['object property modified date']=time.ctime(os.path.getmtime(input_file))
object_property['Is Nullable']=""
object_property['Is Primary Key']=""
object_property['Is Foriegn Key']=""
object_property['Description text']=""
object_property['Data Type precision Number']=""
object_property['Data Type Scale Number']=""

print(object_property)

#print object_property
writer = pd.ExcelWriter('MetaDatafromCsv.xlsx', engine='xlsxwriter')

# Write each dataframe to a different worksheet.
obj.to_excel(writer, sheet_name='Objects')
object_property.to_excel(writer, sheet_name='Object Property')

# Close the Pandas Excel writer and output the Excel file.
writer.save()
