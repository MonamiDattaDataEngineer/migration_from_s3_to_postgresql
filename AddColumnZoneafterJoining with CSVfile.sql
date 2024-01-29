import pandas as pd
import psycopg2
from io import StringIO
from sqlalchemy import create_engine
import boto3
import random


db_cred = {
    'host':'localhost',
    'port':'5432',
    'database':'postgres',
    'user':'postgres',
    'password':'Jimny*123'}

bucket_name = 'np-dev-rawdata'
filePath = 'MMI Data/Admin_Boundary_Latest.csv'
s3_path = f's3://{bucket_name}/{filePath}'

client = boto3.client('s3')

csv_obj = client.get_object(Bucket=bucket_name, Key=filePath)
body = csv_obj['Body']
csv_string = body.read().decode('utf-8')

df = pd.read_csv(StringIO(csv_string))


df.columns


import numpy as np
df_final = df[df['TYPE'].isin(['Subdistrict'])]
df_final = df_final[['ST_CODE','STATE_NAME','DIST_CODE','DIST_NAME','SUBDIST_CODE','SUBDIST_NAME']]


df_final['Fin_yr'] = np.random.randint(2020,2024,size = len(df_final))
df_final['Potential'] = np.random.randint(1,1001,size = len(df_final))


csv_string = body.read().decode('utf-8')
dff = pd.read_csv("C:/Users/kpmg_bhalder1/Downloads/Zone_Info.csv")
dff.head(25)

dff.state_desc.unique()


df_final.STATE_NAME.unique()

df_final['STATE_NAME'] = df_final['STATE_NAME'].str.upper() 
  
df_final.head() 

df_final.STATE_NAME.unique()


# Importing the libraries
import pandas as pd
import numpy as np
dff.loc[dff["state_desc"] == "JAMMU AND KASHMIR", "state_desc"] = 'JAMMU & KASHMIR'
dff.loc[dff["state_desc"] == "ANDAMAN AND NICOBAR ISLANDS", "state_desc"] = 'ANDAMAN & NICOBAR ISLANDS'
dff.loc[dff["state_desc"] == "PONDICHERRY", "state_desc"] = 'PUDUCHERRY'
dff.loc[dff["state_desc"] == "LAKSHDWEEP", "state_desc"] = 'LAKSHADWEEP'
dff.head(15)




df3=pd.merge(df_final,dff, left_on='STATE_NAME', right_on='state_desc', how='left')
#df3.drop('state_desc','region_desc','zone_cd','Already Defined Zone for Clustering')
df3.head(5)



df_zone = df3.drop(['state_desc','region_desc','zone_cd','Already Defined Zone for Clustering'], axis = 1)
df_zone.head(10)



# engine = create_engine(f'postgresql://{db_cred["user"]}:{db_cred["password"]}@{db_cred["host"]}:{db_cred["port"]}/{db_cred["database"]}')

conn_str = f'postgresql://{db_cred["user"]}:{db_cred["password"]}@{db_cred["host"]}:{db_cred["port"]}/{db_cred["database"]}'
engine = create_engine(conn_str)
conn = engine.connect() 

df_zone.to_sql('np-sales-protential-zone-wist-demo', engine, schema='dev', if_exists='replace', index=False)


---------------xxxxxxxxxxxxxxxxxxxxxxxxx-------------------------------


MODIFIED
no uppercase>>>>>>>>>>>>>>





import pandas as pd
import psycopg2
from io import StringIO
from sqlalchemy import create_engine
import boto3
import random


db_cred = {
    'host':'localhost',
    'port':'5432',
    'database':'postgres',
    'user':'postgres',
    'password':'Jimny*123'}

bucket_name = 'np-dev-rawdata'
filePath = 'MMI Data/Admin_Boundary_Latest.csv'
s3_path = f's3://{bucket_name}/{filePath}'


client = boto3.client('s3')

csv_obj = client.get_object(Bucket=bucket_name, Key=filePath)
body = csv_obj['Body']
csv_string = body.read().decode('utf-8')

df = pd.read_csv(StringIO(csv_string))



df_final['Fin_yr'] = np.random.randint(2020,2024,size = len(df_final))
df_final['Potential'] = np.random.randint(1,1001,size = len(df_final))



csv_string = body.read().decode('utf-8')
dff = pd.read_csv("C:/Users/kpmg_bhalder1/Downloads/Zone_Info.csv")
dff.head(25)


#df_final['STATE_NAME'] = df_final['STATE_NAME'].str.upper() 
df_final = df_final.drop(['Fin_yr'], axis = 1)
  
df_final.head() 



# Importing the libraries
import pandas as pd
import numpy as np
dff.loc[dff["state_desc"] == "JAMMU AND KASHMIR", "state_desc"] = 'JAMMU & KASHMIR'
dff.loc[dff["state_desc"] == "ANDAMAN AND NICOBAR ISLANDS", "state_desc"] = 'ANDAMAN & NICOBAR ISLANDS'
dff.loc[dff["state_desc"] == "PONDICHERRY", "state_desc"] = 'PUDUCHERRY'
dff.loc[dff["state_desc"] == "LAKSHDWEEP", "state_desc"] = 'LAKSHADWEEP'
dff.head(15)




df3 = pd.merge(df_final, dff, left_on=df_final["STATE_NAME"].str.lower(),
                      right_on=dff["state_desc"].str.lower(), how="left")
#df3=pd.merge(df_final,dff, left_on='STATE_NAME', right_on='state_desc', how='left')
#df3.drop('state_desc','region_desc','zone_cd','Already Defined Zone for Clustering')
df3.head(5)





df_zone = df3.drop(['state_desc','region_desc','zone_cd','Already Defined Zone for Clustering'], axis = 1)
df_zone = df3[['ST_CODE','STATE_NAME','DIST_CODE','DIST_NAME','SUBDIST_CODE','SUBDIST_NAME','ZONE']]
df_zone.head(10)


df_region = df3.drop(['state_desc','ZONE','zone_cd','Already Defined Zone for Clustering'], axis = 1)
df_region = df3[['ST_CODE','STATE_NAME','DIST_CODE','DIST_NAME','SUBDIST_CODE','SUBDIST_NAME','region_desc','Potential']]
df_region.head(10)


# engine = create_engine(f'postgresql://{db_cred["user"]}:{db_cred["password"]}@{db_cred["host"]}:{db_cred["port"]}/{db_cred["database"]}')

conn_str = f'postgresql://{db_cred["user"]}:{db_cred["password"]}@{db_cred["host"]}:{db_cred["port"]}/{db_cred["database"]}'
engine = create_engine(conn_str)
conn = engine.connect() 



df_zone.to_sql('np_sales_protential_zone_wise_demo', engine, schema='dev', if_exists='replace', index=False)
df_region.to_sql('np_sales_protential_region_wise_demo', engine, schema='dev', if_exists='replace', index=False)
df_final.to_sql('np_sales_protential_allindia_demo', engine, schema='dev', if_exists='replace', index=False)