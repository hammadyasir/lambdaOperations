import pandas as pd
import awswrangler as wr
import csv
import boto3
from rets import Session
import asyncio
import time
import os


def lambda_handler(event,context):
    print ('i am here')
    print ("Event are:",event)
    login_url_R=event['rets_login_url']
    print("login_url_R",login_url_R)
    
    rets_username_R=event['rets_username']
    print("rets_username_R",rets_username_R)
    
    rets_password_R=event['rets_password']
    print("rets_password_R",rets_password_R)
    
    
    
    # return '0';
    start = time.time()
    login_url =login_url_R
    username =rets_username_R
    password = rets_password_R
    bucket_name = 'almsbucket'
    key='csvfiles'
    subfolder='cwatr'
    rets_client =Session(login_url, username, password)
    print (rets_client)
    s3=boto3.client("s3")
    rets_client.login()
    database='default'
    
    str_list=["RE_1","LD_2","CI_3","MF_4"]
    resource=["Property","Media","OpenHouse"]
    class_names=["RESIDENTIAL","LOTS/LAND","COMMERCIAL/INDUSTRIAL","MULTI-FAMILY"]
    i=0
    j=0
    sizeoflist=len(str_list)
    sizeofresource=len(resource)
    print (sizeoflist)
    while j < sizeofresource:
        i=0
        print("Athena access start time to read tables and LMD for ",resource[j])
        while i < sizeoflist:
            if resource[j]=="Property" :
                dq = wr.athena.read_sql_query(sql="SELECT table_name FROM information_schema.columns WHERE table_name = 'property' LIMIT 1", database="default")
                print (dq)
                if not dq.empty:
                    sql_query=''
                    sql_query='SELECT l_updatedate FROM property Where l_class=\''+class_names[i]+'\' order by 1 desc limit 1'
                    print(sql_query)
                    dq = wr.athena.read_sql_query(sql=sql_query, database="default")
                    print('Dq value is',dq)
                    if not dq.empty:
                        dq=dq['l_updatedate'].iloc[0]
                        # dq=dq.get('l_updatedate').iloc[0]
                        query="(L_UpdateDate="+dq+"+)"
                    
                if len(dq)==0:
                    dq='2022-01-01'
                    print(dq)
                    query="(L_UpdateDate="+dq+"+)"
            print('if loop',dq)    
            if resource[j]=="Media" :
                dq = wr.athena.read_sql_query(sql="SELECT table_name FROM information_schema.columns WHERE table_name = 'Media' LIMIT 1", database="default")
                print (dq)
                if not dq.empty:
                    sql_query=''
                    sql_query='SELECT l_updatedate FROM Media Where l_class=\''+class_names[i]+'\' order by 1 desc limit 1'
                    dq = wr.athena.read_sql_query(sql=sql_query, database="default")
                    if not dq.empty:
                        dq=dq.get('l_updatedate').iloc[0]
                    # dq=dq['l_updatedate'].iloc[0]
                        query="(L_UpdateDate="+dq+"+)"
                    print(dq)
                if len(dq)==0:
                    dq='2022-01-01'
                    print(dq)
                    query="(L_UpdateDate="+dq+"+)"
            if resource[j]=="OpenHouse" :
                dq = wr.athena.read_sql_query(sql="SELECT table_name FROM information_schema.columns WHERE table_name = 'OpenHouse' LIMIT 1", database="default")
                print (dq)
                if not dq.empty:
                    sql_query=''
                    sql_query='SELECT OH_UpdateDateTime FROM OpenHouse Where l_class= '+class_names[i]+'order by 1 desc limit 1'
                    dq = wr.athena.read_sql_query(sql=sql_query, database="default")
                    if not dq.empty:
                        dq=dq['OH_UpdateDateTime'].iloc[0]
                        query="(OH_UpdateDateTime="+dq+"+)"
                    print(dq)
                if len(dq)==0:
                    dq='2022-05-01'
                    print(dq)
                    query="(OH_UpdateDateTime="+dq+"+)"
            #query='(L_UpdateDate=2022-01-01+)'
            print("Athena access end time to read tables and LMD for ",resource[j])
            print("data download start time for ",str_list[i])
            system_data = rets_client.search(resource=resource[j], resource_class=str_list[i], limit = 100, dmql_query=query)
            df=pd.DataFrame(system_data)
            # print (df)
            if not df.empty :
                #print (df['L_UpdateDate'].str[0:10])
                current_dat=[]
                if resource[j]=="Property" or  resource[j]=="Media"  :
                    current_dat=df['L_UpdateDate'].str[0:10]
                if resource[j]=="OpenHouse" :
                    current_dat=df['OH_UpdateDateTime'].str[0:10]
                df['current_date']=current_dat#.drop_duplicates()
                #print(df['current_dat'])
                print ('curent date is ',df['current_date'])
            print("data download end time for ",str_list[i])
            print("S3 writing start time for class",str_list[i])
            s3=boto3.resource("s3")
            bucket = s3.Bucket(bucket_name)
            Path='s3://almsbucket/cwatr/'+resource[j]+'/'
            new_folder=resource[j]
            Table=resource[j].lower()
            default='default'
            length=len(df)
            a1= set((list(df.columns)))
            dtype=dict.fromkeys(a1,'string')
            print("S3 writing end time for class",str_list[i])
            print("Athena writing end time for class",str_list[i])
            if length!=0 :
            # and Table=='property' and str_list[i]=="RE_1":
                wr.s3.to_parquet(
                df=df,
                path=Path,
                mode='append',
                database=default,
                table=Table,
                filename_prefix=resource[j],
                dtype=dtype,
                # boto3_session=s3.session,
                partition_cols=["current_date"],
                 dataset=True
                # use_threads=True

                )
            print("Athena writing end time for class",str_list[i])
            i=i+1
        print("data download end time for ",resource[j])
        j=j+1
    print("Data download end time for rets")
    end = time.time()
    print(f"Runtime of the program is {end - start}")

    # c=0
   
    # while c < sizeoflist2:
    #     print ('awais')
    #     if str_list2[c]=="ActiveAgent" :
    #             query='(U_UpdateDate=2022-05-23+)'
    #     if str_list2[c]=="ActiveOffice" :
    #             query='(O_UpdateDate=2022-05-23+)'
    #     system_data = rets_client.search(resource=str_list2[c], resource_class=str_list2[c],dmql_query=query)
    #     print (system_data)
    #     file_name = str_list2[c]+'.csv'
    #     df = pd.DataFrame(system_data)
    #     print (df)
    #     s3=boto3.resource("s3")
    #     bucket = s3.Bucket(bucket_name)
    #         # bucket.upload_fileobj(
    #         # file,
    #         # f"{subfolder}/{key}/{str_list[i]}")
    #     Path1='s3://almsbucket/cwatr/'+str_list2[c]+'/'+str_list2[c]+'.parquet'
    #     len1=len(df)
    #     if len1!=0 :
    #         wr.s3.to_parquet(
    #         df=df,
    #         path=Path1
    #             # path=f"{subfolder}/{key}/{str_list[i]}"
    #         )
    #     i=i+1
    print("Done Work by cwatr Lambda")    
    return {
        'statusCode': 200,
        'body': f"Upload succeeded:has been uploaded to Amazon S3 in bucket"
    }
# df = pd.DataFrame(system_data)
# df.to_csv("E:/Python/rets/rets.csv")
