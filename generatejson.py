import os
import numpy as np
import pandas as pd
import re
import argparse
import urllib.request, json

parser = argparse.ArgumentParser()

parser.add_argument('-link', required=True)


args= parser.parse_args()

input_link = args.link

#importing cod json url and loading it
with urllib.request.urlopen(input_link) as url:
    data = json.loads(url.read().decode())



#Storing fields object in an  variable for writing in seperate file
if 'fields' in data:
    fields=data['fields']
    field_json=json.dumps(fields)

print(fields)

#For features Class
if 'features' in data:
    features=data['features']
    features_json=json.dumps(features)
print(features)


"""

# Writing fields as json source file
with open("/home/hduser/cod/fields.json","w") as f:
  f.write(field_json)

# Writing fields as json source file
with open("/home/hduser/cod/features.json","w") as f:
  f.write(features_json)
"""