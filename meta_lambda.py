from rets import Session
from datetime import datetime
import json
import boto3
# import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
from base64 import b64decode

def lambda_handler(event, context):
     # credentials for RDS Database
    db_username = os.environ['db_username']
    db_name = os.environ['db_name']
    db_password=os.environ['db_password']
    db_host =os.environ['db_host']
    db_port = os.environ['db_port']
    
    print("db_username Type:", type(db_username))
    print("db_username are:", db_username)
    
    print("DataBase Name Type:", type(db_name))
    print("DataBase Name are:", db_name)
   
    
    print("Type of password",type(db_password))
    print("Password are:", db_password)
    
    
    print("Type of db_host are:",type(db_host))
    print("db_host name are:",db_host)
    
    print("Type of db_port are:",type(db_port))
    print("db_port are:",db_port)
    
    conn_api = psycopg2.connect(database=db_name, user=db_username, password=db_password,
                            host=db_host, port=int(db_port))
                            
    print("connection:", conn_api)
    cur_api = conn_api.cursor()
    cur_api.execute("select auth,name from source where name ~* 'cwtar'")
    mydic={}
    
    def add_value(dict_obj, key, value):
        if key not in dict_obj:
                dict_obj[key] = value
        elif isinstance(dict_obj[key], list):
            dict_obj[key].append(value)
        else:
            dict_obj[key] = [dict_obj[key], value]
            
    for i in cur_api.fetchall():
        print("TYPE:",type(i))
        print("data are:",i)
        print("hello type",type(i[0]))
        print("hello",i[0])
        
        alldetails = json.loads(i[0])
        print("RES type",type(alldetails))
        print("RES ARE",alldetails)
        
        user_key_to_lookup = 'user'
        if user_key_to_lookup in alldetails:
            user = {user_key_to_lookup: alldetails[user_key_to_lookup]}
            print("user_key_to_lookup Key exists",user)
            add_value(mydic, user_key_to_lookup, alldetails[user_key_to_lookup])

        else:
            print("user_key_to_lookup Key does not exist")

        loginUrl_key_to_lookup = 'loginUrl'
        if loginUrl_key_to_lookup in alldetails:
            loginurl={loginUrl_key_to_lookup:alldetails[loginUrl_key_to_lookup]}
            # mydic.update(loginurl)
            add_value(mydic, loginUrl_key_to_lookup, alldetails[loginUrl_key_to_lookup])
            print("loginUrl_key_to_lookup Key exists",loginurl)
        else:
            print("loginUrl_key_to_lookup Key does not exist")
        
        password_key_to_lookup = 'password'
        if password_key_to_lookup in alldetails:
            password={password_key_to_lookup:alldetails[password_key_to_lookup]}
            # mydic.update(password)
            add_value(mydic, password_key_to_lookup, alldetails[password_key_to_lookup])
            print("password_key_to_lookup Key exists",password )
        else:
            print("password_key_to_lookup Key does not exist")
            
    
    print("MYDIC Users",mydic['user'])
    print("MYDIC loginUrl",mydic['loginUrl'])
    print("MYDIC password",mydic['password'])


    print("____")
    print("My dict",mydic)
    df=pd.DataFrame(mydic)
    print(df.head(1))
    print("Starting:___________")
    
    first_row = df.iloc[1]
    print("First Row Select:",first_row)
    username_R=first_row[0]
    loginUrl_R=first_row[1]
    password_R=first_row[2]
    
    
    print("dONE USER:",username_R)
    print("dONE loginUrl",loginUrl_R)
    print("ONE password",password_R)

    
    # credentials for ylopo2
    # rets_login_url=os.environ['rets_login_url']
    rets_login_url=loginUrl_R
    # rets_username = os.environ['rets_username']
    rets_username = username_R
    # rets_password = os.environ['rets_password']
    rets_password = password_R
    
    print("Type of rest login url:",type(rets_login_url))
    print("rets login url:",rets_login_url)
   
    print("Type of Rets username are:",type(rets_username))
    print("Rets username are:",rets_username)
    
    print("Type of rets_password",type(rets_password))
    print("rets_password",rets_password)

    rets_client = Session(rets_login_url, rets_username, rets_password)
    rets_client.login()
    source_name = "cwatr"
    # conn = psycopg2.connect(database="homelistings", user="homedbuser", password="brUDra8h", host="pg-dev.alpha.ylopo", port="5443")
    conn = psycopg2.connect(database=db_name, user=db_username, password=db_password,
                            host=db_host, port=int(db_port))
    cursor = conn.cursor()

    ###Resource MetaData
    resource = ["Property", "Media", "OpenHouse", "ActiveAgent", "ActiveOffice"]
    l = []
    for item in resource:
        metadata = ''
        metadata = rets_client.get_resource_metadata(resource=item)
        metadata.update({"source_name": source_name})
        l.append(metadata)
    df = pd.DataFrame(l)

    df['source_name'] = ''
    # df['resource_name']=''
    columns = (list(df.columns))
    columns = json.dumps(columns)
    columns = columns.replace('[', ' ')
    columns = columns.replace(']', ' ')
    columns = columns.replace('"', '')
    columns = columns.replace('Description', 'Description_r')

    query = "INSERT INTO stage.ps_resource_metadata ({}) VALUES %s".format((columns))
    values = [[value for value in project.values()] for project in l]
    execute_values(cursor, query, values)
    # ###Class metaData
    for c in resource:
        class_metadata = ''
        l = []
        class_metadata = rets_client.get_class_metadata(resource=c)
        for v in class_metadata:
            v.update({"source_name": source_name})
            v.update({"resource_name": c})
            l.append(v)
        dframe = ''
        dframe = pd.DataFrame(class_metadata)
        dframe['source_name'] = ''
        dframe['resource_name'] = ''
        class_columns = dframe.columns.values.tolist()
        class_columns = json.dumps(class_columns)
        class_columns = class_columns.replace('[', ' ')
        class_columns = class_columns.replace(']', ' ')
        class_columns = class_columns.replace('"', '')
        class_columns = class_columns.replace('Description', 'Description_r')
        # with open ('class_metadata','w') as w:
        #     json.dump(l,w)
        query1 = ''
        query1 = "INSERT INTO stage.ps_class_metadata ({}) VALUES %s".format((class_columns))
        metadata_values = [[value for value in project.values()] for project in l]

        execute_values(cursor, query1, metadata_values)
        res = []
        res = [d['ClassName'] for d in l]

        ##Table MetaData
        for d in res:
            system_data = ''
            system_data = rets_client.get_table_metadata(resource=c, resource_class=d)
            df1 = ''
            df1 = pd.DataFrame(system_data)
            df1['source_name'] = ''
            df1['resource_name'] = ''
            for item in system_data:
                item.update({"source_name": source_name})
                item.update({"resource_name": c})

            # with open('system_data.json', 'w') as outfile:
            #     json.dump(system_data, outfile)

            table_columns = (list(df1.columns))
            table_columns = json.dumps(table_columns)
            table_columns = table_columns.replace('[', ' ')
            table_columns = table_columns.replace(']', ' ')
            table_columns = table_columns.replace('"', '')
            table_columns = table_columns.replace('DataType', 'DataType_r')
            table_columns = table_columns.replace('Precision', 'Precision_r')
            table_columns = table_columns.replace('Default', 'Default_r')
            table_columns = table_columns.replace('Unique', 'Unique_r')
            table_columns = table_columns.replace('Index', 'Index_r')
            table_columns = table_columns.replace('Case', 'Case_r')
            table_columns = table_columns.replace('InKeyIndex_r', 'InKeyIndex')
            table_columns = table_columns.replace('Default_rSearchOrder', 'DefaultSearchOrder')
            query = ''
            query = "INSERT INTO stage.ps_system_metadata ({}) VALUES %s".format((table_columns))
            values = [[value for value in project1.values()] for project1 in system_data]
            execute_values(cursor, query, values)

    cursor.execute("select * from stage.CheckMetaDataEssentials()")
    result = cursor.fetchall()

    for row in result:
        print(row)
        if row[0] != '1':
            raise Exception(row)
        else:
            pass
            # lambda_client = boto3.client('lambda')
            # lambda_payload = {'rets_login_url':rets_login_url,'rets_username':rets_username,'rets_password':rets_password}
            # lambda_client.invoke(FunctionName='cwatr', 
            #          InvocationType='Event',
            #          Payload=json.dumps(lambda_payload))
     
    conn.commit()
    conn.close()
    print("Upload succeeded:has been uploaded to Amazon S3 in bucket")
    print("connection status", conn.closed)
    if conn.closed==1:
        print("connection was closed")
       
    # return {
    #     'statusCode': 200,
    #     'body': f"Upload succeeded:has been uploaded to Amazon S3 in bucket"
    # }