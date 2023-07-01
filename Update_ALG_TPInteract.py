
import urllib
import pandas as pd
import sqlalchemy as sa
import pyodbc
import numpy as np
import os
import win32com.client
from datetime import datetime, timedelta

def preprocessing(date):
    date=date.replace("/","-")



    cats_needed=[   "Eureka ID","Agent","Campaign Name","Channel", "Date/Time","Location","Recorder ID","Skill Name",
                    "SourceID",
                    'Categories.ALG Complaint Analysis.Extreme',
                    'Categories.ALG Complaint Analysis.Mild',
                    'Categories.ALG Complaint Analysis.Moderate',
                    'Categories.ALG Dissatisfaction Breakdown.ALG Agent',
                    'Categories.ALG Dissatisfaction Breakdown.ALG Payment Related',
                    'Categories.ALG Dissatisfaction Breakdown.ALG Service',
                    'Categories.ALG Dissatisfaction Breakdown.ALG Technology Related',
                    'Categories.ALG General Customer Experience.Dissatisfaction',
                    'Categories.ALG General Customer Experience.Escalation',
                    'Categories.ALG General Customer Experience.Neutral',
                    'Categories.ALG General Customer Experience.Positive Sentiments',
                    'Categories.ALG General Customer Experience.Repeat Contact',
                    'Categories.ALG General Customer Experience.Transfer',
                    'Categories.ALG TCD.ALG All TCD.Billing Issue',
                    'Categories.ALG TCD.ALG All TCD.Cancellation',
                    'Categories.ALG TCD.ALG All TCD.Change Reservation',
                    'Categories.ALG TCD.ALG All TCD.Document Request',
                    'Categories.ALG TCD.ALG All TCD.Inquiries',
                    'Categories.ALG TCD.ALG All TCD.Insurance',
                    'Categories.ALG TCD.ALG All TCD.Payment']

    dfcm=pd.read_excel(os.getcwd()+f"\\Download folder\\Downloads Callminer_v2\\{date}\SearchReport{date}.xlsx", header=4, usecols=cats_needed)


    dfcm = dfcm.replace(['Miss'],0)
    dfcm = dfcm.replace(['Hit'],1)

    try: 
        dfcm.drop(columns=["Contact Link"], inplace=True)
        dfcm.drop(columns=["Hits"], inplace=True)
    except:
        pass

    dfcm["Date/Time"]=pd.to_datetime(dfcm["Date/Time"], format="%Y-%m-%d %H:%M:%S").dt.strftime("%d/%m/%Y")
    dfcm.rename(columns={'Date/Time':'Date'}, inplace=True)

    dfcm.columns=[category.replace('Categories.','') for category in dfcm.columns]
    dfcm.columns = [category.replace('ALG All TCD.', '') if 'ALG All TCD.' in category else category for category in dfcm.columns]
    
    return dfcm


def connect_DB():
    connection_string = (
        'Driver={ODBC Driver 17 for SQL Server};'
        'SERVER=TPCCP-DB09\SCNEAR;'
        'Database=Analytics;'
        'Trusted_Connection=yes;'
    )
    connection_uri = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_string)}"
    engine = sa.create_engine(connection_uri, fast_executemany=True)
    return engine


def upload_dfs_tosql(data):
    engine=connect_DB()
    data.to_sql("ALG_TPInteract_CM", if_exists="append", con=engine, index=False)



def remove_duplicates_SQL():

    connection_string = (
        'Driver={ODBC Driver 17 for SQL Server};'
        'SERVER=TPCCP-DB09\SCNEAR;'
        'Database=Analytics;'
        'Trusted_Connection=yes;'
    )


    connection_uri = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_string)}"
    engine = sa.create_engine(connection_uri, fast_executemany=True)



    cnxn = pyodbc.connect(connection_string)
    cursor = cnxn.cursor()
    remove_dup=f"""
                WITH CTE AS (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY [Eureka ID] ORDER BY (SELECT NULL)) AS RowNum
                    FROM [Analytics].[dbo].[ALG_TPInteract_CM]
                )
                DELETE FROM CTE WHERE RowNum > 1;
                """
    cursor.execute(remove_dup)
    cnxn.commit()
    print(f"removed duplicates from: ALG_TPInteract_CM")

def main(date):
    data=preprocessing(date)
    upload_dfs_tosql(data)
    remove_duplicates_SQL()

if __name__=="__main__":
    main(date)