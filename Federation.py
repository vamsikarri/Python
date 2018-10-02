
from flask_cors import CORS
from flask import Flask, jsonify, abort , make_response, request
from sqlalchemy import create_engine
import psycopg2
import ConfigParser as CP
import MySQLdb
import os
from os.path import expanduser
import re
import subprocess
from subprocess import Popen, PIPE
import time
import json
from datetime import datetime
from dateutil import relativedelta
import shlex
from collections import OrderedDict
import csv



app = Flask(__name__)
CORS(app, supports_credentials=True)
app.config['JSON_SORT_KEYS'] = False






config = CP.RawConfigParser()
home = expanduser("~")
config_file="%s/config.repo" %home
config.read(config_file)



mysql_db = {
    "host": "52.203.109.83",
    "user": "staging",
    "passwd": "StgUsr05@",
    "db": "staging"
}





pg_connection = {
    "database":"metarepo",
    "host": "gathimdm.cjtsyol1af3p.us-east-1.rds.amazonaws.com",
    "port":5432,
    "user":"gathiadmin",
    "password": "Admin56Gathi9156",
}


def tables_in_query(sql_str):

    # split on blanks, parens and semicolons
    tokens = re.split(r"[\s)(;]+", sql_str)

    # scan the tokens. if we see a FROM or JOIN, we set the get_next
    # flag, and grab the next one (unless it's SELECT).
    result = set()
    get_next = False
    for tok in tokens:
        if get_next:
            if tok.lower() not in ["", "select"]:
               if "." in tok:
                   result.add(tok)
            get_next = False
        get_next = tok.lower() in ["from", "join"]

    return result



@app.route('/dbnames')
def DB():
    return jsonify({'dbname':'MySQL','id':'dbMySQL'},{'dbname':'Postgres','id':'dbPostgres'},{'dbname':'MongoDB','id':'dbMongoDB'})


@app.route('/MySQL')
def mysql(function_called=0):
    try:
        connection = MySQLdb.connect(**mysql_db)
    except BaseException:
        abort(make_response(jsonify(code=400, success= "False", message = "Failed to establish MySQL connection")))
    cursor = connection.cursor()
    databases = {}
    cursor.execute("SHOW DATABASES")
    result = []
    for db in cursor.fetchall():
        databases[db[0]] = {'tables': []}

    for db in databases.keys():
        cursor.execute("SHOW TABLES FROM `%s`" % db)
        for table in cursor.fetchall():
            databases[db]['tables'].append(
                {'name': table[0], 'columns': [], 'query': None, 'columns_types': []})

    for db in databases.keys():
        for index, table in enumerate(databases[db]['tables']):
            sql = "SHOW COLUMNS FROM `%s`.`%s`" % (db, table['name'])
            try:
                cursor.execute(sql)
                for column in cursor.fetchall():
                    databases[db]['tables'][index]['columns'].append(column[0])
                    databases[db]['tables'][index]['columns_types'].append("%s(%s)" % (column[0], column[1].replace("unsigned", "").strip()))

                query = "SELECT * FROM mysql.`%s`.`%s`" % (db, table['name'])
                databases[db]['tables'][index]['query'] = query
            except BaseException:
                print "Error fetching the table\n%s" % sql
    cursor.close()
    connection.close()

    for db in databases.keys():
        r = {
            'db': db,
            'table': []
        }
        for table in databases[db]['tables']:
            r['table'].append({'table_name': table['name'],
                               'select_query': table['query'],
                               'columns': table['columns_types']})
        result.append(r)

    data = {'dbname': 'MySQL', 'dbinfo': result}
    if not function_called:
        return jsonify(data)
    else:
        return data



@app.route('/Postgres')
def postgres(function_called=0):
    connection = psycopg2.connect(**pg_connection)
    cursor = connection.cursor()
    databases = {}
    cursor.execute(
        "SELECT datname from pg_database WHERE datistemplate = false;")
    result = []
    for db in cursor.fetchall():
        databases[db[0]] = {'schemas': []}
    cursor.close()
    connection.close()

    for db in databases.keys():
        pg_connection["database"] = db
        try:
            connection = psycopg2.connect(**pg_connection)
            cursor = connection.cursor()
            cursor.execute(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT LIKE 'pg%%' AND schema_name NOT LIKE 'information_schema' AND schema_name NOT LIKE 'public' AND catalog_name LIKE '%s'" %
                db)
            for schema in cursor.fetchall():
                databases[db]['schemas'].append(
                    {'name': schema[0], 'tables': []})
            cursor.close()
            connection.close()
        except BaseException:
            print "1. Database failed %s\n" % db
            databases[db]['failed'] = True

    for db in databases.keys():
        if databases[db].get("failed"):
            continue
        pg_connection["database"] = db
        try:
            connection = psycopg2.connect(**pg_connection)
            cursor = connection.cursor()

            for index, schema in enumerate(databases[db]['schemas']):
                schema_name = schema['name']
                cursor.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = '%s' ORDER BY table_type, table_name;" %
                    schema_name)
                for table in cursor.fetchall():
                    databases[db]['schemas'][index]['tables'].append(
                        {'name': table[0], 'columns': [], 'query': None, 'columns_types': []})

            cursor.close()
            connection.close()
        except OSError:
            print "2. Database failed %s\n" % db
            databases[db]['failed'] = True

    for db in databases.keys():
        if databases[db].get("failed"):
            continue
        pg_connection["database"] = db
        try:
            connection = psycopg2.connect(**pg_connection)
            cursor = connection.cursor()

            for index, schema in enumerate(databases[db]['schemas']):
                schema_name = schema['name']
                schema_tables = schema['tables']

                for subindex, table in enumerate(schema_tables):
                    sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s'" % (
                        table['name'], schema_name)
                    try:
                        cursor.execute(sql)
                        for column in cursor.fetchall():
                            databases[db]['schemas'][index]['tables'][subindex]['columns'].append(
                                column[0])
                            databases[db]['schemas'][index]['tables'][subindex]['columns_types'].append(
                                "%s(%s)" % (column[0], column[1].strip()))

                        query = "SELECT * FROM `%s`" % table['name']
                        databases[db]['schemas'][index]['tables'][subindex]['query'] = query
                    except BaseException:
                        print "Error fetching the table\n%s" % sql

            cursor.close()
            connection.close()
        except BaseException:
            print "3. Database failed %s\n" % db
            databases[db]['failed'] = True

    for db in databases.keys():
        r = {
            'db': db
        }
        if not databases[db].get('failed'):
            r['dbschemas'] = []
            for index, schema in enumerate(databases[db]['schemas']):
                schema_name = schema['name']
                schema_tables = schema['tables']
                data = {
                    "dbschemanm": schema_name,
                    "tables": []
                }

                for subindex, table in enumerate(schema_tables):
                    data['tables'].append(
                        {'table_name': table['name'],
                         'select_query': table['query'],
                         'columns': table['columns_types']
                         }
                    )

                r['dbschemas'].append(data)
        else:
            r['dbschemas'] =[]
            r['error'] = 'permission denied'

        result.append(r)

    data = {'dbname': 'Postgres', 'dbinfo': result}
    if not function_called:
        return jsonify(data)
    else:
        return data


@app.route('/files')
def files(function_called=0):
    result = {
        "dbname": "files",
        "dbinfo": [
            {
                "table": [],
                "db": "csv"
            },
            {
                "table": [],
                "db": "JSON"
            },
        ]
    }

    for r in handle_csv(1):
        for table in r["dbinfo"][0]["table"]:
            result["dbinfo"][0]["table"].append(table)

    for r in handle_json(1):
        for table in r["dbinfo"][0]["table"]:
            result["dbinfo"][1]["table"].append(table)


    if function_called:
        return result

    return jsonify(result)


@app.route('/csv')
def handle_csv(function_called=0):
    files = os.listdir("/home/hduser/federation/Files/CSV")
    results = []
    for file in files:
        filename = "/home/hduser/federation/Files/CSV/%s" % file

        f = open(filename, 'r')
        reader = csv.reader(f)

        columns = reader.next()
        f.close()

        result = {
            "dbinfo": [{
                "table": [{
                    "table_name": file,
                    "select_query": "SELECT * FROM local.csv.%s" % file,
                    "columns": columns
                }],
                "db": "csv"
            }
            ],
            "dbname": "files"
        }

        results.append(result)
        print results

    if function_called:
        return results
    else:
        return jsonify(results)

@app.route('/parquet')
def handle_parquet(function_called=0):
    files = os.listdir("/home/hduser/federation/Files/PARQUET")
    results = []
    for file in files:
        filename = "/home/hduser/federation/Files/PARQUET/%s" % file

        f = pandas.read_parquet(filename)
        columns = list(f.columns)

        result = {
            "dbinfo": [{
                "table": [{
                    "table_name": file,
                    "select_query": "SELECT * FROM local.parquet.%s" % file,
                    "columns": columns
                }],
                "db": "parquet"
            }
            ],
            "dbname": "Files"
        }

        results.append(result)

    if function_called:
        return results
    else:
        return jsonify(results)

@app.route('/json')
def handle_json(function_called=0):
    files = os.listdir("/home/hduser/federation/Files/JSON")
    results = []
    for file in files:
        filename = "/home/hduser/federation/Files/JSON/%s" % file

        f = open(filename)
        data = json.load(f)
        print data
        columns = data[0].keys()
        f.close()

        result = {
            "dbinfo": [{
                "table": [{
                    "table_name": file,
                    "select_query": "SELECT * FROM local.json.%s" % file,
                    "columns": columns
                }],
                "db": "json"
            }
            ],
            "dbname": "files"
        }

        results.append(result)

    if function_called:
        return results
    else:
        return jsonify(results)


@app.route('/excel')
def handle_excel(function_called=0):
    files = os.listdir("/home/hduser/federation/Files/EXCEL")
    results = []
    for file in files:
        filename = "/home/hduser/federation/Files/EXCEL/%s" % file

        f = pandas.read_excel(filename)
        columns = list(f.columns)

        result = {
            "dbinfo": [{
                "table": [{
                    "table_name": file,
                    "select_query": "SELECT * FROM local.excel.%s" % file,
                    "columns": columns
                }],
                "db": "excel"
            }
            ],
            "dbname": "files"
        }

        results.append(result)

    if function_called:
        return results
    else:
        return jsonify(results)

@app.route('/databases')
def databases():
    data = [mysql(1), postgres(1), files(1)]
    return jsonify(data)


@app.route('/query')
def query():
    try:
        count=[]
        sql_str = request.args.get("sql")
        sql_str = sql_str.replace(u"\u00A0", " ")
        print sql_str
        params = request.args.get("conditions")


        if not sql_str or not len(sql_str):
            return 'error'
        # split on blanks, parens and semicolons
        tokens = re.split(r"[\s)(;]+", sql_str)
        result = []
        get_next = False
        for tok in tokens:
            if get_next:
                if tok.lower() not in ["", "select"]:
                    if "." in tok:
                        result.append(tok)
                get_next = False
            get_next = tok.lower() in ["from", "join"]
        timestamp = time.strftime("%Y%m%d%H%M%S")
        filename_output = "output_%d%s.json" % (os.getpid(), timestamp)
        json_result = {
            "sql": sql_str,
            "outputs": [{"datasink": "HDFS","file-type": "json", "file-path": "hdfs://ip-10-11-9-196.ec2.internal:8020/user/hduser/federation/" + filename_output, "append": False, "user":"NA"}],
            "inputs": [],
            "dynamic-conditions": [params]
        }
        
        
        for entry in result:
            type = entry.split(".")[0]
            if type=="PostgreSQL":
                user = pg_connection["user"]
                json_result["inputs"].append({
                    "table": entry.split(".")[3],
                    "schema": entry.split(".")[2],
                    "datasource": entry.split(".")[0],
                    "database": entry.split(".")[1],
                    "user": "%s"%user
                    })
            elif type=="MySQL":
                user = mysql_db["user"]
                json_result["inputs"].append({
                    "table": entry.split(".")[2],
                    "datasource": entry.split(".")[0],
                    "database": entry.split(".")[1],
                    "user": "%s"%user
                    })
        print json_result
        timestamp = time.strftime("%Y%m%d%H%M%S")
        filename = "/home/hduser/dropbox/%d_%s.json" % (os.getpid(), timestamp)
        f = open(filename, "w")
        json.dump(json_result, f)
        f.close()
        query_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        strip_time_1= query_start_time[10:][:9].strip()
        strip_time_2 = datetime.strptime(strip_time_1,"%H:%M:%S")
        subprocess_fail1= False
        run_cmd="spark-submit --master local[*] --class com.gathi.mdm.federation.Federation --jars /home/hduser/federation/log4j-1.2.17.jar,/home/hduser/federation/mysql-connector-java-5.1.6.jar,/home/hduser/federation/postgresql-42.2.2.jre7.jar,/home/hduser/federation/spark-csv_2.11-1.2.0.jar,/home/hduser/federation/commons-csv-1.4.jar,/home/hduser/federation/utils-0.1.jar /home/hduser/federation/federation-0.1.jar -APP_NAME test -CONFIG_FILE_PATH %s -ENV dev -LOG_PATH PostgresMysqlParquetJSON_BigJoin_2_CSV.log " % filename
        print run_cmd
        run_cmd1= shlex.split(run_cmd)
        p= subprocess.Popen(run_cmd1,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        print p
        stdout, stderr = p.communicate() 


        print stderr

            
 


        query_run_time_1 = datetime.now().strftime("%H:%M:%S")
        query_run_time_2 = datetime.strptime(query_run_time_1, "%H:%M:%S")

        diff = relativedelta.relativedelta(query_run_time_2,strip_time_2)
        hours = diff.hours
        minutes= diff.minutes
        seconds = diff.seconds
        duration= "%s:%s:%s"%(hours,minutes,seconds)
        print stderr
        if stderr:
           status = "error"
           return stderr
        

        
        try:
            cmd_val = "hadoop fs -getmerge hdfs://ip-10-11-9-196.ec2.internal:8020/user/hduser/federation/%s /home/hduser/dropbox/r_%s.txt" %(filename_output,timestamp)
            print cmd_val
            cmd = shlex.split(cmd_val) 
            print cmd
            cmd2 = subprocess.call(cmd, shell =False)


            
        except Exception as e:
            status= str(e).replace("'","")
            return status
        count = sum(1 for line in open("/home/hduser/dropbox/r_%s.txt"%(timestamp)))
        data= open("/home/hduser/dropbox/r_%s.txt"%(timestamp),"r").readlines()
        
 
        result = []
        for line in data:
            line = line.strip()
            if not len(line):
                continue
            result.append(json.loads(line))
        status = "SUCCESS"    
        return jsonify(result)
    except Exception as e:
        status = str(e).replace("'","")
        return jsonify({'error':status})

"""
    finally:
        connection = psycopg2.connect(**pg_connection)
        cursor = connection.cursor()
        q = "%s" %sql_str
        query = "SELECT count(*) FROM \"FEDERATION_T\".qry_history WHERE qry_id=MD5('%s')"%q.replace("'", "\'")
        print query
        cursor.execute(query)
        count_result = cursor.fetchall()[0][0]
        insert_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        if count_result==0:
            if not count:
                count = "null"
            cursor.execute("INSERT INTO \"FEDERATION_T\".qry_history(qry_id,qry_exe_strt_ts,qry_txt,qry_run_drn_tm,qry_run_sta_txt,qry_rslt_rows_cnt,usr_id,rec_insrt_ts) VALUES (MD5('%s'),'%s','%s','%s','%s',%s, null, '%s')"%(sql_str,query_start_time,sql_str,duration,status, count, insert_timestamp))
           
        else:
            update_query= "UPDATE \"FEDERATION_T\".qry_history SET rec_insrt_ts='%s' WHERE qry_id=MD5('%s')"%(insert_timestamp, sql_str)
            cursor.execute(update_query)
        connection.commit()
        connection.close()
"""


@app.route('/api/history', methods=['GET'])
def query_history():
    connection = psycopg2.connect(**pg_connection)
    conn = connection.cursor()
    results = []
    history_query= "SELECT qry_exe_strt_ts , qry_txt , qry_run_drn_tm , qry_rslt_rows_cnt, rec_insrt_ts, qry_run_sta_txt , qry_id FROM \"FEDERATION_T\".qry_history;"
    conn.execute(history_query)
    results_select = conn.fetchall()
    results = []
    connection.close()
    for result_select in results_select:
        
        LAST_RUN= str(result_select[4])
        QUERY_START_TIME = (result_select[0])
        QUERY = str(result_select[1].strip())
        QUERY_RUN_TIME = str(result_select[2])
        RESULT_COUNT = "%s rows"%str(result_select[3])
        QUERY_STATUS = str(result_select[5])
        QUERY_ID = str(result_select[6])
        

        if "SUCCESS" in QUERY_STATUS:
            QUERY_STATUS = "Success"
        else:
            QUERY_STATUS = "Failed"

        if "None rows" in RESULT_COUNT:
            RESULT_COUNT= "None"
        

        
        LAST_RUN_strip = LAST_RUN[0:][:10].strip()
        LAST_RUN_FORMAT= datetime.strptime(LAST_RUN_strip, "%Y-%m-%d")
        LAST_RUN_DAY = LAST_RUN_FORMAT.day
        LAST_RUN_MONTH = LAST_RUN_FORMAT.month
        LAST_RUN_YEAR = LAST_RUN_FORMAT.year

        Today_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        date_strip = Today_datetime[0:][:10].strip()
        date_format= datetime.strptime(date_strip, "%Y-%m-%d")
        day_today = date_format.day
        day_month = date_format.month
        day_year = date_format.year
        


        LAST_RUN_DAY_DIFF = int(day_today) - int(LAST_RUN_DAY)
        LAST_RUN_MONTH_DIFF = int(day_month) - int(LAST_RUN_MONTH)
        LAST_RUN_YEAR_DIFF = int(day_year) - int(LAST_RUN_YEAR)
        
        DIFF = (LAST_RUN_YEAR_DIFF, LAST_RUN_MONTH_DIFF, LAST_RUN_DAY_DIFF)

        if DIFF == (0,0,0):
            LAST_RUN = "Today"

        elif DIFF[0]== 1:
            LAST_RUN = "%s year ago"%(DIFF[0])

        elif DIFF[0]> 1:
            LAST_RUN = "%s years ago"%(DIFF[0])

        elif DIFF[1]== 1:
            LAST_RUN = "%s month ago"%(DIFF[1])

        elif DIFF[1]>1:
            LAST_RUN = "%s months ago"%(DIFF[1])

        elif DIFF[2] ==1:
            LAST_RUN = "%s day ago"%(DIFF[2])
  
        elif DIFF[2] > 1:
            LAST_RUN = "%s days ago"%(DIFF[2])
      

        results.append({'QUERY_START_TIME':QUERY_START_TIME,'QUERY':QUERY, 'QUERY_RUN_TIME':QUERY_RUN_TIME, 'RESULT_COUNT':RESULT_COUNT, 'LAST_RUN':LAST_RUN, 'QUERY_STATUS':QUERY_STATUS, 'QUERY_ID':QUERY_ID})

    return jsonify(results)


    
    





@app.route('/api/help', methods=['GET'])
def help():
    endpoints = [rule.rule for rule in app.url_map.iter_rules() 
                 if rule.endpoint !='static']
    return jsonify(dict(api_endpoints=endpoints))





@app.route('/api/profiling', methods=['GET'])
def dp():
    connection = psycopg2.connect(**pg_connection)
    cursor = connection.cursor()
    query = "SELECT S.SITE_ID ,S.db_pltfm_nm ,O.OBJ_SCHM_NM ,O.OBJ_PHY_NM ,R1.RUL_FN_NM as TBL_RULE,OP.OBJ_PRPT_NM ,OPD.ORD_NBR ,OPD.DAT_TYPE ,R.RUL_FN_NM as COL_RULE,O.OBJ_ID ,R1.RUL_ID as rule_id1 ,OP.OBJ_PRPT_ID ,R.RUL_ID as rule_id2,case when QR.RUL_ID IS NOT NULL THEN 'Y' ELSE 'N' END as \"TABLE_RUL_IND\",case when PR.RUL_ID IS NOT NULL THEN 'Y' ELSE 'N' end as \"COL_RUL_IND\", R.RUL_TYPE_NM FROM mdr_t2.obj_prpt_dat_prflg_qlty_rul_ma PR  LEFT JOIN mdr_t2.dat_prflg_qlty_and_bsn_rul R ON PR.RUL_ID = R.RUL_ID LEFT JOIN mdr_t2.obj_prpt OP ON PR.OBJ_PRPT_ID = OP.OBJ_PRPT_ID LEFT JOIN mdr_t2.obj_prpt_dtl OPD ON OP.OBJ_PRPT_ID = OPD.OBJ_PRPT_ID LEFT JOIN mdr_t2.obj O ON OP.OBJ_ID = O.OBJ_ID LEFT JOIN mdr_t2.obj_dat_prflg_qlty_rul_map QR ON O.OBJ_ID = QR.OBJ_ID LEFT JOIN mdr_t2.dat_prflg_qlty_and_bsn_rul R1 ON QR.RUL_ID = R1.RUL_ID LEFT JOIN mdr_t2.SITE_INFMN S ON O.SITE_ID = S.SITE_ID where O.OBJ_SCHM_NM='dvdrental.public';"
    print query
    cursor.execute(query)
    allrows = cursor.fetchall()
    print allrows

    #allrows = [('S04', 'PostgreSQL', 'mdr_t2', 'JOB', None, 'obj_dat_stwd_id', 17, 'varchar(35)', 'Statistical Data Profiling', '2', None, '17', '1'), ('S04', 'PostgreSQL', 'mdr_t2', 'OBJ', None, 'src_apn_identifer', 18, 'varchar(35)', 'Statistical Data Profiling', '2', None, '18', '1'), ('S04', 'PostgreSQL', 'mdr_t2', 'OBJ', None, 'obj_dat_dmn_nm', 15, 'varchar(128)', 'Statistical Data Profiling', '2', None, '15', '1'), ('S04', 'PostgreSQL', 'mdr_t2', 'OBJ', None, 'obj_fnl_dmn_nm', 16, 'varchar(128)', 'Statistical Data Profiling', '2', None, '16', '1'), ('S04', 'PostgreSQL', 'mdr_t2', 'OBJ', None, 'obj_loc_path_txt', 13, 'varchar(2000)', 'Statistical Data Profiling', '2', None, '13', '1'), ('S04', 'PostgreSQL', 'mdr_t2', 'OBJ', None, 'obj_ownr_id', 14, 'varchar(35)', 'Statistical Data Profiling', '2', None, '14', '1'), ('S04', 'PostgreSQL', 'mdr_t2', 'OBJ', None, 'obj_phy_nm', 11, 'varchar(128)', 'Null Check', '2', None, '11', '3'), ('S04', 'PostgreSQL', 'mdr_t2', 'OBJ', None, 'obj_phy_nm', 11, 'varchar(128)', 'Statistical Data Profiling', '2', None, '11', '1'), ('S04', 'PostgreSQL', 'mdr_t2', 'OBJ', None, 'obj_lyr_nm', 12, 'varchar(128)', 'Statistical Data Profiling', '2', None, '12', '1'), ('S04', 'PostgreSQL', 'mdr_t2', 'OBJ', None, 'obj_id', 1, 'varchar(35)', 'Statistical Data Profiling', '2', None, '\xef\xbb\xbf1', '1'), ('S04', 'PostgreSQL', 'mdr_t2', 'OBJ', None, 'obj_schm_nm', 10, 'varchar(128)', 'Statistical Data Profiling', '2', None, '10', '1'), ('S04', 'PostgreSQL', 'mdr_t2', 'OBJ', None, 'obj_db_pltfm_nm', 8, 'varchar(128)', 'Statistical Data Profiling', '2', None, '8', '1'), ('S04', 'PostgreSQL', 'mdr_t2', 'OBJ', None, 'obj_type_nm', 9, 'varchar(128)', 'Valid Values Check', '2', None, '9', '2'), ('S04', 'PostgreSQL', 'mdr_t2', 'OBJ', None, 'obj_type_nm', 9, 'varchar(128)', 'Statistical Data Profiling', '2', None, '9', '1'), ('S04', 'PostgreSQL', 'mdr_t2', 'OBJ', None, 'sub_apn_id', 6, 'varchar(35)', 'Statistical Data Profiling', '2', None, '6', '1'), ('S04', 'PostgreSQL', 'mdr_t2', 'OBJ', None, 'site_id', 7, 'varchar(35)', 'Statistical Data Profiling', '2', None, '7', '1'), ('S04', 'PostgreSQL', 'mdr_t2', 'OBJ', None, 'prj_id', 4, 'varchar(35)', 'Statistical Data Profiling', '2', None, '4', '1'), ('S04', 'PostgreSQL', 'mdr_t2', 'OBJ', None, 'apn_id', 5, 'varchar(35)', 'Statistical Data Profiling', '2', None, '5', '1'), ('S04', 'PostgreSQL', 'mdr_t2', 'OBJ', None, 'prgm_id', 3, 'varchar(35)', 'Statistical Data Profiling', '2', None, '3', '1'), ('S04', 'PostgreSQL', 'mdr_t2', 'OBJ', None, 'rec_lst_updt_ts', 19, 'timestamp', 'Statistical Data Profiling', '2', None, '19', '1'), ('S04', 'PostgreSQL', 'mdr_t2', 'OBJ', None, 'ln_of_bsn_id', 2, 'varchar(35)', 'Statistical Data Profiling', '2', None, '2', '1')]
    r_columns = {}
    tables = {}

    for i, row in enumerate(allrows):
        table_profiling = False
        if row[13] == "Y":
            table_profiling = True

        column_profiling = False
        if row[14] == "Y":
            column_profiling = True

        if not tables.get(row[3]):
            tables[row[3]] = {
                "input_type": "database",
                "table_profiling": table_profiling,
                "column_profiling": column_profiling,
                "db-details": {
                },
                "columns": []
            }
            tables[row[3]]['db-details'] = {
                'datasource': row[1],
                'database': row[2].split(".")[0],
                'table': row[3],
                'schema':row[2].split(".")[1],
                'user': "gathiadmin"
            }
            r_columns[row[3]] = {}

        if not r_columns[row[3]].get(row[5]):
            r_columns[row[3]][row[5]] = {
                "input_column": row[5].lower(),
                "obj_prpt_id": row[11].decode('ascii', 'ignore'),
                "datatype": row[7],
                "rule_names": []
            }



        if column_profiling:
            if row[8]:
                r_columns[row[3]][row[5]]["rule_names"].append({row[15]:row[8]})

    for k in r_columns:
        t = r_columns[k]

        for r in t:
            r = t[r]
            tables[k]["columns"].append({
                "input_column": r['input_column'],
                "obj_prpt_id": r['obj_prpt_id'],
                "datatype": r['datatype'],
                "rule_name": r['rule_names']
            })

    filenames = []
    for table in tables:
        timestamp = time.strftime("%Y%m%d%H%M%S")
        f = open("%s-%s.json" % (table, timestamp), "w")
        filenames.append("%s-%s.json" % (table, timestamp))
        json.dump(tables[table], f)
        f.close()

    result = []
    for file in filenames:
        r = {
            'output': '',
            'error': False,
            'filename': file
        }

        output = subprocess.call('spark-submit --master local[2] --class com.gathi.dp.Profiling  --jars   /home/hduser/common/lib/postgresql-42.2.2.jre7.jar,/home/hduser/common/lib/spark-csv_2.10-1.5.0.jar,/home/hduser/common/lib/commons-csv-1.4.jar,/home/hduser/common/lib/mysql-connector-java-5.1.6.jar,/home/hduser/common/lib/log4j-api-2.11.0.jar,/home/hduser/common/lib/log4j-core-2.11.0.jar,/home/hduser/common/jar/utils-0.1.jar  /home/hduser/profiling/Profiling.jar  -APP_NAME  PROFILING  -CONFIG_FILE_PATH "%s" -STATS_PROF_DB /home/hduser/profiling/prof_stats_and_status_db.json' % file, shell=True)

        result.append(r)

    return jsonify(result)

if __name__ == "__main__":
    app.run(host='10.11.9.230',threaded=True)

