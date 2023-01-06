# -*- coding: utf-8 -*-

!pip install pyodbc   # install library for MSSQL and import necessary libraries
import requests
import pandas as pd
import numpy as np
import json
import pyodbc

# Method of Fatching Data from Json 

dataList=[]

def fatch_data(row):
  base_url = "https://goadmin.ifrc.org/api/v2/project/"
  result = requests.get(base_url).json()
  for i in result['results']:
    dataList.append(i)

  for j in range(1,int(row/50),1):
    if int(row/50) > 43:
      break
    base_url=result['next']
    result = requests.get(base_url).json()
    for x in result['results']:
      dataList.append(x)

# Fatch 1000 row and convert it to DataFrame
fatch_data(1000)
dataFList = pd.DataFrame(dataList)

# Create project_details DataFrame from Fatched DataFrame

project_details = pd.DataFrame()
rowsCount = len(dataFList)
for i in range(0,rowsCount,1):
    projectCell = dataFList.loc[i]
    buffer = pd.DataFrame()
    data = [[projectCell['id'], projectCell['name'], projectCell['primary_sector'],projectCell['programme_type'],projectCell['operation_type'],
             projectCell['status'],projectCell['visibility'],projectCell['reporting_ns_contact_name'], projectCell['reporting_ns_contact_role'],projectCell['reporting_ns_contact_email'],
             projectCell['start_date'],projectCell['end_date'],projectCell['budget_amount'],projectCell['actual_expenditure'], projectCell['project_country_detail']['name'], projectCell['target_male'], 
             projectCell['target_female'],projectCell['target_other'],projectCell['target_total'],projectCell['reached_male'],projectCell['reached_female'],
             projectCell['reached_other'],projectCell['reached_total']
             
            ]]
    
    
    
    buffer =pd.DataFrame(data)
    project_details = project_details.append(buffer,ignore_index=True)

project_details.columns = ['id', 'name','primary_sector','programme_type','operation_type',
                             'status','visibility','reporting_ns_contact_name', 'reporting_ns_contact_role','reporting_ns_contact_email',
                             'start_date','end_date','budget_amount','actual_expenditure', 'country', 'target_male', 
                             'target_female','target_other','target_total','reached_male','reached_female',
                             'reached_other','reached_total']

project_details

# Prepare the project_details DataFrame. Convert None and NaN to other variables (integeger or float to zero and string varialbles to white space

project_details.replace(to_replace=[None], value=np.nan, inplace=True) # None to NaN

project_details['reporting_ns_contact_name'] = project_details['reporting_ns_contact_name'].fillna("")
project_details['reporting_ns_contact_role'] = project_details['reporting_ns_contact_role'].fillna("")
project_details['reporting_ns_contact_email'] = project_details['reporting_ns_contact_email'].fillna("")

project_details['budget_amount'] = project_details['budget_amount'].fillna(0)
project_details['actual_expenditure'] = project_details['actual_expenditure'].fillna(0)
project_details['target_male'] = project_details['target_male'].fillna(0)
project_details['target_female'] = project_details['target_female'].fillna(0)
project_details['target_other'] = project_details['target_other'].fillna(0)
project_details['target_total'] = project_details['target_total'].fillna(0)
project_details['reached_male'] = project_details['reached_male'].fillna(0)
project_details['reached_female'] = project_details['reached_female'].fillna(0)
project_details['reached_other'] = project_details['reached_other'].fillna(0)
project_details['reached_total'] = project_details['reached_total'].fillna(0)

# Create project_districts_details DataFrame from Fatched DataFrame

project_districts_detail = pd.DataFrame()
rowsCount = len(dataFList)
for i in range(0,rowsCount-1,1):
  distinctCount = len(dataFList.loc[i]['project_districts_detail'])
  country = dataFList.loc[i]['project_country_detail']
  for j in range(0,distinctCount-1,1):
    buffer = pd.DataFrame()
    distinct = dataFList.loc[i]['project_districts_detail'][j]
    data = [[distinct['id'], distinct['name'], distinct['code'],distinct['is_enclave'],  
             dataFList.loc[i]['operation_type'], dataFList.loc[i]['status'], country['name']
            ]]
    buffer =pd.DataFrame(data)
    project_districts_detail = project_districts_detail.append(buffer,ignore_index=True)

project_districts_detail.columns = ['id', 'name', 'code', 'is_enclave', 'operation_type', 'status', 'country']

project_districts_detail

# Create reporting_ns_detail DataFrame from Fatched DataFrame

reporting_ns_detail = pd.DataFrame()
rowsCount = len(dataFList)
for i in range(0,rowsCount,1):
  buffer = pd.DataFrame()
  reporting = dataFList.loc[i]['reporting_ns_detail']
  data = [[reporting['id'], reporting['iso'], reporting['iso3'],reporting['name'], reporting['society_name'],reporting['fdrs']]]
  buffer =pd.DataFrame(data)
  reporting_ns_detail = reporting_ns_detail.append(buffer,ignore_index=True)

reporting_ns_detail.columns = ['id', 'iso', 'iso3', 'name', 'society_name', 'fdrs']

reporting_ns_detail

# Create reporting_ns_detail(users) DataFrame from Fatched DataFrame

modified_by_detail = pd.DataFrame()
rowsCount = len(dataFList)
for i in range(0,rowsCount-1,1):
  buffer = pd.DataFrame()
  users = dataFList.loc[i]['modified_by_detail']
  try:
    data = [[ users['id'], users['username'], users['email'], users['first_name'], users['last_name']]]
    buffer =pd.DataFrame(data)
    modified_by_detail = modified_by_detail.append(buffer,ignore_index=True)
  except:
    pass

modified_by_detail.columns = ['id', 'username', 'email', 'first_name', 'last_name']

modified_by_detail

# Connection string. Please change the value of Servername variable before running this cell

ServerName = "KOTANPC\KOTANSQLSERVER"
def connectDB():
    conn = pyodbc.connect(
    "Driver={SQL Server Native Client 11.0};"
    "Server="+ServerName+";"
    "Trusted_Connection=yes;"
    )
    return conn

# Create Database

conn= connectDB()
conn.autocommit = True
cursor = conn.cursor()
cursor.execute("CREATE DATABASE projectDB;")
conn.close()

# Create Database Tables

conn= connectDB()
conn.autocommit = True
cursor = conn.cursor()

cursor.execute("Use projectDB;")
cursor.execute(
                  " CREATE TABLE project_details ( id INT, name varchar(500), primary_sector INT, programme_type INT, operation_type INT, " +
                  " status INT, visibility varchar(100), reporting_ns_contact_name varchar(100), reporting_ns_contact_role varchar(100), " + 
                  " reporting_ns_contact_email varchar(100), start_date date, end_date date, budget_amount money, " +
                  " actual_expenditure money,country varchar(100), target_male INT, " +
                  " target_female INT, target_other INT, target_total INT, " +
                  " reached_male INT, reached_female INT, reached_other INT, reached_total INT ); " 


                  + " CREATE TABLE project_districts_details ( "+
                      "id INT, name varchar(100), code varchar(10), is_enclave bit, operation_type INT," +
                      "status INT, country varchar(100) );"


                  + " CREATE TABLE reporting_ns_detail ( "+
                      "id INT, iso varchar(5), iso3 varchar(5), name varchar(100)," +
                      "society_name  varchar(100), fdrs varchar(10) );"

                  +  "CREATE TABLE users ( "+
                      "id INT, username varchar(100), email varchar(100), first_name varchar(100), last_name varchar(100));"
              )
conn.commit()
conn.close()

# Fill project_details database table with project_details DataFrame

conn= connectDB()
conn.autocommit = True
cursor = conn.cursor()


cursor.execute(" Use projectDB; ")

project_details.fillna(0)

for index,row in project_details.iterrows():
    cursor.execute(
                  " INSERT INTO  project_details ([id], [name], [primary_sector], [programme_type],[operation_type]," +
                  " [status],[visibility],[reporting_ns_contact_name], [reporting_ns_contact_role],[reporting_ns_contact_email]," +
                  " [start_date],[end_date],[budget_amount],[actual_expenditure],[country],[target_male]," +
                  " [target_female],[target_other],[target_total],[reached_male],[reached_female]," +
                  " [reached_other],[reached_total]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" 

                 , row['id'], row['name'], row['primary_sector'], row['programme_type'], row['operation_type'],
                  row ['status'], row['visibility'], row['reporting_ns_contact_name'], row['reporting_ns_contact_role'], row['reporting_ns_contact_email'],
                  row['start_date'], row['end_date'], row['budget_amount'], row['actual_expenditure'], row['country'], row['target_male'],
                  row['target_female'], row['target_other'], row['target_total'], row['reached_male'], row['reached_female'],
                  row['reached_other'], row['reached_total'] 

                )


conn.commit()
conn.close()

# Fill project_districts_details database table with project_districts_detail DataFrame

conn= connectDB()
conn.autocommit = True
cursor = conn.cursor()


cursor.execute(" Use projectDB; ")
for index,row in project_districts_detail.iterrows():
    cursor.execute(
                    " INSERT INTO  project_districts_details ( [id] , [name], [code], [is_enclave], [operation_type],  [status], [country] ) VALUES (?,?,?,?,?,?,?)"
                    , row['id'], row['name'], row['code'],row['is_enclave'],  row['operation_type'],row['status'], row['country']  

                  )

conn.commit()
conn.close()

# Fill reporting_ns_detail database table with reporting_ns_detail DataFrame

conn= connectDB()
conn.autocommit = True
cursor = conn.cursor()


cursor.execute(" Use projectDB; ")
for index,row in reporting_ns_detail.iterrows():
    cursor.execute(
                    " INSERT INTO  reporting_ns_detail ( [id] , [iso], [iso3], [name], [society_name],  [fdrs] ) VALUES (?,?,?,?,?,?)"
                    , row['id'], row['iso'], row['iso3'],row['name'],  row['society_name'],row['fdrs']  

                  )


conn.commit()
conn.close()

# Fill users database table with modified_by_detail DataFrame

conn= connectDB()
conn.autocommit = True
cursor = conn.cursor()


cursor.execute(" Use projectDB; ")
for index,row in modified_by_detail.iterrows():
        cursor.execute(
                    " INSERT INTO  users ( [id] , [username], [email], [first_name], [last_name] ) VALUES (?,?,?,?,?)"
                    , row['id'], row['username'], row['email'],row['first_name'],  row['last_name']  

                  )

conn.commit()
conn.close()

# Fill fetch data from the created database, display the top 10 districts in terms of the target population (target_total)

conn= connectDB()
conn.autocommit = True
cursor = conn.cursor()


cursor.execute(" Use projectDB; ")
cursor.execute( " SELECT DISTINCT TOP 10 project_districts_details.name as 'district name', " +
                       " project_details.country 'coutry', "+
                       " reporting_ns_detail.society_name as 'national society', " +
                       " project_details.target_total as 'target_total', "+
                       " project_details.target_male as 'target_male', "+
                       " project_details.target_female as 'target_female', "+
                       " project_details.primary_sector as 'primary_sector', "+
                       " project_details.budget_amount as 'budget', "+
                       " project_details.actual_expenditure as 'expenditure', " +
                       " users.first_name as 'name of the user' "+
               " FROM project_details " +
                      "   LEFT JOIN  users ON project_details.reporting_ns_contact_email = users.email " +
                       "  INNER JOIN ( project_districts_details  INNER JOIN reporting_ns_detail ON reporting_ns_detail.name =  project_districts_details.country) "+
                       "     ON project_details.country =  project_districts_details.name "+
              "  ORDER BY target_total DESC "
              )
for row in cursor:
    print(f'{row}')
print()               
conn.commit()
conn.close()

