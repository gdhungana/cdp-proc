import pyodbc
import numpy as np
import pandas as pd
import sys

def connect_sqlserver(server,dbname,uid=None,pwd=None,port=1433,driver='/usr/local/lib/libmsodbcsql.17.dylib'):
    #- connects to the sql server using pyodbc and returns a cursor object
    if uid is None:
        try: #- windows authentication:
            print("trying windows authentication")
            cnxn=pyodbc.connect(driver=driver,server=server,database=dbname,Trusted_Connection='yes')
        except:
            print("Windows authentication could not be established. Provide uid/pwd for sql driver connection")
            sys.exit(0)
    else:
        print("using sql driver-- non-windows authentication")
        if pwd is None:
            print("Must also provide pwd")
            sys.exit(0)
        else:  
            cnxn=pyodbc.connect(driver=driver,server=server,database=dbname,uid=uid,pwd=pwd,port=port)
    cursor=cnxn.cursor()
    return cursor

def load_data(cursor,sqlquery):
    """
    cursor: pyodbc.cursor object
    sqlquery: sql query string
    returns pandas dataframe from the sqlquery. 
    Use only for small databases if running from stanalone node-- to make efficient 
    need distributed architecture for larger databases 
    """
    data=pd.read_sql(sqlquery,cursor.connection)
    return data


