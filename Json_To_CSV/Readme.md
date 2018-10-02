# Overview
This script was written using python 3.7.  While it should work on earlier versions of python, only version 3.7 has been tested.

This python script will convert a given JSON file to a CSV.  Because JSON is a nested data format, and CSV is a flat data format, the process must follow a strict invariant in order to perform a lossless conversion. 

There are three types of data that can be stored in a JSON object: data, objects, and arrays.  The conversions are performed as follows:

##### Data: 
JSON data is a name/value pair, and so each unique instance of data is given a new column of the same name.

![](https://i.imgur.com/5baif7v.png)



#### Objects: 
JSON objects are containers for more nested JSON objects.  Hence, each value within an object will be one of the same three types (data, object, array), and will be handled accordingly.  Each object will be collapsed to the same level as the siblings of an object, recursively if necessary.  The names of the values within an object will also be prefixed with their parent's name.

##### Without Nesting:
![](https://i.imgur.com/Eq4Spfg.png)

##### With Nesting and Sibling Object:
![](https://i.imgur.com/OXTZRjM.png)

#### Arrays:
To preserve a CSV's flat format, and in order to keep the CSV in 1NF, each array element will result in a new row.  The row's contents are derived by creating a new table from the array and taking the Cartesian product of this new table and the table built from the array's siblings.

##### Without Nesting:
![](https://i.imgur.com/aRm6qLL.png)

##### With Nesting:
![](https://i.imgur.com/8y2apRn.png)


# Usage
Usage of this script requires a minimum of two arguments: the input and the output file. An example execution of this script with minimal arguments is as follows:

`python json_to_csv.py -in1 input.json -out1 result.csv`

Note that if the input file does not have the `.json` extension, it will still be parsed so long as its contents are that of a properly formatted JSON file.  Additionally, if the output file is not indicated to be a CSV, it will have the `.csv` extension appended upon creation.

This script can also take an optional `-filter` argument, which will filter the resultant CSV by the base-level value of the same name:

`python json_to_csv.py -in1 input.json -out1 result.csv -filter color`

![](https://i.imgur.com/GJX2G3P.png)

The resultant CSV has been filtered by the `color` array.  The `id` column was not created, and the names of the columns that *are* present are not prefixed by the array's name.
