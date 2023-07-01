# %%
import urllib
import pandas as pd
import sqlalchemy as sa
import pyodbc
import numpy as np
import os
import win32com.client
from datetime import datetime, timedelta


# %%
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

# %%
def preprocessing_dfcm(dfcm):

    dfcm = dfcm.replace(['Miss'],0)
    dfcm = dfcm.replace(['Hit'],1)

    try: 
        dfcm.drop(columns=["Contact Link"], inplace=True)
        dfcm.drop(columns=["Hits"], inplace=True)
    except:
        pass

    
    dur_id=dfcm.columns.to_list().index("Duration")
    id_vars=dfcm.columns[:dur_id+1]
    #Remove Categories
    dfcm.columns=[item[11:] if "CJ Categories" in item else item.replace("Categories.","") for item in dfcm.columns]

    Componentes= [f"{item}" for item in dfcm.columns if item.count(".")>1]
    Categorias= [f"{item}" for item in dfcm.columns if item.count(".")==1]

    #dfcm=dfcm[dfcm["ANI"].isin(["Restricted","anonymous","Anonymous"])]
    dfcm_tosql=dfcm.copy()

    dfcm_tosql["Session"]=dfcm_tosql["Recorder ID"].str.replace("@","_").str.replace(".","_").str.split("_").str[0]
    dfcm_tosql["Session"]

    #Create Date and time columns

    dfcm_tosql["Date"]=pd.to_datetime(dfcm_tosql["Call Start Time"], format="%Y-%m-%d %H:%M:%S").dt.strftime("%m/%d/%Y")
    dfcm_tosql["Time"]=pd.to_datetime(dfcm_tosql["Call Start Time"], format="%Y-%m-%d %H:%M:%S").dt.strftime("%H:%M:%S")

    dfcm_tosql["Rebuttals Total"]=(dfcm_tosql['Rebuttals handling.Rebuttal-Customer already has a provider']+
                                    dfcm_tosql['Rebuttals handling.Rebuttal-Customer is busy']+
                                    dfcm_tosql["Rebuttals handling.Rebuttal-Customer is not interested"]+
                                    dfcm_tosql['Rebuttals handling.Rebuttal-Dialed number is wrong']+
                                    dfcm_tosql['Rebuttals handling.Rebuttal-Moving-selling home']+
                                    dfcm_tosql["Rebuttals handling.Rebuttal-Not authorized to make decisions"]+
                                    dfcm_tosql['Rebuttals handling.Rebuttal-Price or More benefits']+
                                    dfcm_tosql['Rebuttals handling.Rebuttal-Send info over']+
                                    dfcm_tosql['Rebuttals handling.Rebuttal-Speak to spouse|I need to think about it'])
                                 

    dfcm_tosql["Objections Total"]=(dfcm_tosql['Objection handling.Refusal-Customer already has a provider']+
                                    dfcm_tosql['Objection handling.Refusal-Customer is busy']+
                                    dfcm_tosql['Objection handling.Refusal-Customer is not interested']+
                                    dfcm_tosql['Objection handling.Refusal-Dialed number is wrong']+
                                    dfcm_tosql['Objection handling.Refusal-Moving-selling home']+
                                    dfcm_tosql['Objection handling.Refusal-Not authorized to make decisions']+
                                    dfcm_tosql['Objection handling.Refusal-Price or More benefits']+
                                    dfcm_tosql['Objection handling.Refusal-Send info over']+
                                    dfcm_tosql['Objection handling.Refusal-Speak to spouse|I need to think about it'])

    dfcm_tosql["CX-Attentiveness Total"]=(dfcm_tosql["CX-Attentiveness.Brand apologies"]+
                                          dfcm_tosql["CX-Attentiveness.Building interaction"]+
                                          dfcm_tosql["CX-Attentiveness.Confidence"]+
                                          dfcm_tosql["CX-Attentiveness.Detection"]+
                                          dfcm_tosql["CX-Attentiveness.Focuses the call`s objective"]+
                                          dfcm_tosql["CX-Attentiveness.Offering further assitance"]+
                                          dfcm_tosql["CX-Attentiveness.Personalization"]+
                                          dfcm_tosql["CX-Attentiveness.Support customer`s needs"])

    dfcm_tosql["CX-Non negotiable Total"]=(dfcm_tosql["CX-Non negotiable.Agent Profanity"]+
                                          dfcm_tosql["CX-Non negotiable.Hang up in previous call"])


    dfcm_tosql["CX-Process Total"]=(dfcm_tosql["CX-Process.Customer Identity Verification"]+
                                    dfcm_tosql["CX-Process.Guides with areas in charge"]+
                                    dfcm_tosql["CX-Process.Slowness of tools"]+
                                    dfcm_tosql["CX-Process.Tool malfunctions"]+
                                    dfcm_tosql["CX-Process.Working deadline for response-resolution"])

    dfcm_tosql["CX-Service Total"]=(dfcm_tosql["CX-Service.Able to respond to various situations"]+
                                    dfcm_tosql["CX-Service.Look for all alternatives"]+
                                    dfcm_tosql["CX-Service.Need to reconduct the call"])

    dfcm_tosql["CX-WAHA Attributes Total"]=(dfcm_tosql["CX-WAHA Attributes.Background sounds"]+
                                            dfcm_tosql["CX-WAHA Attributes.Connection inconveniences"]+
                                            dfcm_tosql["CX-WAHA Attributes.Voice pitch volume"])                                    

    dfcm_tosql["CX Total"]=(dfcm_tosql["CX-Attentiveness Total"]+
                            dfcm_tosql["CX-Non negotiable Total"]+
                            dfcm_tosql["CX-Process Total"]+
                            dfcm_tosql["CX-Service Total"]+
                            dfcm_tosql["CX-WAHA Attributes Total"]+
                            dfcm_tosql["CX-Efforts.Makes to repeat information"]
                            )


    dfcm_tosql["Autofail Total"]=(dfcm_tosql["Autofail.Agent does not retake the lead"]+
                                dfcm_tosql["Autofail.Agent is not doing active listening"]+
                                dfcm_tosql["Autofail.Dead air"]+
                                dfcm_tosql["Autofail.Displays hesitation to keep the call going"]+
                                dfcm_tosql["Autofail.Incorrect tone of voice"]+
                                dfcm_tosql["Autofail.Knowledge gaps"]+
                                #dfcm_tosql["Autofail.Language not supported by the campagin"]+
                                dfcm_tosql["Autofail.Respect"]+
                                #dfcm_tosql["Autofail.Slaming evidence"]+
                                dfcm_tosql["Autofail.Talking over customer"]+
                                dfcm_tosql["Autofail.Transfer to supervisor"]
                                    )

    dfcm_tosql["Beneficios Total"]=(dfcm_tosql["Beneficios.Price protection"]+
                                    dfcm_tosql["Beneficios.Rewards program"]
                                        )

    dfcm_tosql["Branding and introduction Total"]=(dfcm_tosql["Branding and Introduction.Branding - Agent name"]+
                                                    dfcm_tosql["Branding and Introduction.Branding - Company name"]+
                                                    dfcm_tosql["Branding and Introduction.Correct expectations"]+
                                                    #dfcm_tosql["Branding and Introduction.Independent Industry"]+
                                                    dfcm_tosql["Branding and Introduction.Recording disclosure"]+
                                                    dfcm_tosql["Branding and Introduction.Statements to approach customer"]
                                                        )

    dfcm_tosql["Closing Total"]=(#dfcm_tosql["Closing.Indra does not represent Utility"]+
                                dfcm_tosql["Closing.Transfer Language"]
                                    )

    dfcm_tosql["Qualifying Total"]=(dfcm_tosql["Qualifying.Bill information"]+
                                    dfcm_tosql["Qualifying.Confirm utility company"]+
                                    dfcm_tosql["Qualifying.Find DM - FFQ"]+
                                    #dfcm_tosql["Qualifying.Get copy of the bill"]+
                                    dfcm_tosql["Qualifying.Get copy of the bill"]+
                                    dfcm_tosql["Qualifying.Government assistance"]+
                                    #dfcm_tosql["Qualifying.Looking for lead"]+
                                    dfcm_tosql["Qualifying.Looking for lead"]+
                                    dfcm_tosql["Qualifying.Payment status"]
                                        )

    dfcm=dfcm[id_vars.to_list()+Componentes]

    dfcm=dfcm.dropna(subset=["Eureka ID"])

    dfcm = dfcm.replace(['Miss'],0)
    dfcm = dfcm.replace(['Hit'],1)
    df2=dfcm.melt(id_vars=id_vars)
    df3=df2[(df2["value"]!=0)]
    df3=df3.rename(columns={'variable': 'Category', 'value': 'Component'})

    #Separate into Category and component
    df3[["Folder",'Category','Component']] = df3['Category'].str.split('.', expand = True)

    return df3, dfcm_tosql


# %%

def upload_dfs_tosql(df3, engine):
    engine=connect_DB()
    #df_tcd=df3[df3["Folder"].isin(["Cleaning services","General Handyman","Installation Services","Outdoor Projects",
     #               "Renovation or Remodel Services","Repair Services"])]
    #df_tcd.to_sql("tbIndraTCD", if_exists="append", con=engine, index=False)


    df_sales_obj=df3[df3["Folder"]=="Objection handling"]
    df_sales_obj.to_sql("tbIndraSalesObjections", if_exists="append", con=engine, index=False)

    df_rebuttals=df3[df3["Folder"]=="Rebuttals handling"]
    df_rebuttals.to_sql("tbIndraRebuttals", if_exists="append", con=engine, index=False)

    df_cx=df3[df3["Folder"].isin(["CX-Attentiveness","CX-Efforts","CX-Non negotiable","CX-Process","CX-Service","CX-WAHA Attributes"])]
    df_cx.to_sql("tbIndraCX", if_exists="append", con=engine, index=False)

    #return df_tcd, df_sales_obj, df_rebuttals, df_informative


# %%
def scores_indra(dfcm_tosql):
    dfcm_tosql["Branding_score"]=(dfcm_tosql["Branding and Introduction.Branding - Agent name"]+
                                dfcm_tosql["Branding and Introduction.Branding - Company name"]+
                                dfcm_tosql["Branding and Introduction.Correct expectations"]*3+
                                dfcm_tosql["Branding and Introduction.Recording disclosure"]+
                                dfcm_tosql["Branding and Introduction.Statements to approach customer"]*3)
                                

    dfcm_tosql["closing_score"]=(dfcm_tosql["Closing.Disclosure"]*2+
                                dfcm_tosql["Closing.Transfer Language"]*2)


    dfcm_tosql["Qualifying_score"]=(dfcm_tosql["Qualifying.Bill information"]*1.5+
                                dfcm_tosql["Qualifying.Confirm utility company"]*0.2+
                                dfcm_tosql["Qualifying.Find DM - FFQ"]*0.2+
                                dfcm_tosql["Qualifying.Get copy of the bill"]*1.5+
                                dfcm_tosql["Qualifying.Government assistance"]*0.2+
                                dfcm_tosql["Qualifying.Looking for lead"]*0.2+
                                dfcm_tosql["Qualifying.Payment status"]*0.2)

    dfcm_tosql["process_score"]=dfcm_tosql["closing_score"]+dfcm_tosql["Branding_score"]+dfcm_tosql["Qualifying_score"]

    conditions=[(dfcm_tosql["Rebuttals handling.Rebuttal-Customer already has a provider"]==0)&
                (dfcm_tosql["Objection handling.Refusal-Customer already has a provider"]==1),
                
                (dfcm_tosql["Rebuttals handling.Rebuttal-Customer is busy"]==0)&
                (dfcm_tosql["Objection handling.Refusal-Customer is busy"]==1),

                (dfcm_tosql["Rebuttals handling.Rebuttal-Customer is not interested"]==0)&
                (dfcm_tosql["Objection handling.Refusal-Customer is not interested"]==1),

                (dfcm_tosql["Rebuttals handling.Rebuttal-Dialed number is wrong"]==0)&
                (dfcm_tosql["Objection handling.Refusal-Dialed number is wrong"]==1),

                (dfcm_tosql["Rebuttals handling.Rebuttal-Moving-selling home"]==0)&
                (dfcm_tosql["Objection handling.Refusal-Moving-selling home"]==1),

                (dfcm_tosql["Rebuttals handling.Rebuttal-Not authorized to make decisions"]==0)&
                (dfcm_tosql["Objection handling.Refusal-Not authorized to make decisions"]==1),

                (dfcm_tosql["Rebuttals handling.Rebuttal-Price or More benefits"]==0)&
                (dfcm_tosql["Objection handling.Refusal-Price or More benefits"]==1),

                (dfcm_tosql["Rebuttals handling.Rebuttal-Send info over"]==0)&
                (dfcm_tosql["Objection handling.Refusal-Send info over"]==1),

                (dfcm_tosql["Rebuttals handling.Rebuttal-Speak to spouse|I need to think about it"]==0)&
                (dfcm_tosql["Objection handling.Refusal-Speak to spouse|I need to think about it"]==1),
                ]
    choices=[0,0,0,0,0,0,0,0,0]
    dfcm_tosql["Objections_score"]=np.select(conditions, choices, default=40)


    dfcm_tosql["SalesAttributes_score"]=np.where((dfcm_tosql["Autofail.Agent does not retake the lead"]==1)|
                                                (dfcm_tosql["Autofail.Agent is not doing active listening"]==1)|
                                                (dfcm_tosql["Autofail.Dead air"]==1)|
                                                (dfcm_tosql["Autofail.Displays hesitation to keep the call going"]==1)|
                                                (dfcm_tosql["Autofail.Incorrect tone of voice"]==1)|
                                                (dfcm_tosql["Autofail.Knowledge gaps"]==1)|
                                                #(dfcm_tosql["Autofail.Language not supported by the campagin"]==1)|
                                                #(dfcm_tosql["Autofail.Respect"]==1)|
                                                #(dfcm_tosql["Autofail.Slaming evidence"]==1)|
                                                (dfcm_tosql["Autofail.Talking over customer"]==1)|
                                                (dfcm_tosql["Autofail.Transfer to supervisor"])==1,0,40)


    #dfcm_tosql["Autofail_score"]=np.where(#(dfcm_tosql["Autofail.Language not supported by the campagin"]==1)|
    #                                    (dfcm_tosql["Autofail.Respect"]==1)|
                                        #(dfcm_tosql["Autofail.Slaming evidence"]==1)
    #                                    ,0,40)

    
    dfcm_tosql["Total_score"]=(dfcm_tosql["SalesAttributes_score"]+dfcm_tosql["Objections_score"]+dfcm_tosql["Qualifying_score"]+dfcm_tosql["Branding_score"])

    return dfcm_tosql

# %%
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

    for table in ["tbIndraRebuttals","tbIndraSalesObjections","tbIndraCX"]:
        remove_dup=f"""
                    WITH cte AS (
                                SELECT * ,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY 
                                            [Eureka ID],
                                            [Recorder ID],
                                            [Category],
                                            [Component],
                                            [Folder]
                                        ORDER BY 
                                            [Eureka ID],
                                            [Recorder ID],
                                            [Category],
                                            [Component],
                                            [Folder]
                                    ) row_num
                                
                                    FROM Analytics.dbo.{table}
                            )
                            DELETE FROM cte
                            WHERE row_num > 1;
                    """
        cursor.execute(remove_dup)
        cnxn.commit()
        print(f"removed duplicates from: {table}")

    for table in ["tbIndraLivevoxData"]:
        remove_dup=f"""
                    WITH cte AS (
                                SELECT * ,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY 
                                            [Session ID]
                                            
                                        ORDER BY 
                                            [Session ID]
                                            
                                    ) row_num
                                
                                    FROM Analytics.dbo.{table}
                            )
                            DELETE FROM cte
                            WHERE row_num > 1;
                    """
        cursor.execute(remove_dup)
        cnxn.commit()
        print(f"removed duplicates from: {table}")

    for table in ["tbIndraDFCallminer"]:
        remove_dup=f"""
                    WITH cte AS (
                                SELECT * ,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY 
                                            [Eureka ID]
                                        ORDER BY 
                                            [Eureka ID]
                                    ) row_num
                                
                                    FROM Analytics.dbo.{table}
                            )
                            DELETE FROM cte
                            WHERE row_num > 1;
                    """
        cursor.execute(remove_dup)
        cnxn.commit()
        print(f"removed duplicates from: {table} by EurekaID")

    for table in ["tbIndraDFCallminer"]:
        remove_dup=f"""
                    WITH cte AS (
                                SELECT * ,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY 
                                            [Session]
                                        ORDER BY 
                                            [Session]
                                    ) row_num
                                
                                    FROM Analytics.dbo.{table}
                            )
                            DELETE FROM cte
                            WHERE row_num > 1;
                    """
        cursor.execute(remove_dup)
        cnxn.commit()
        print(f"removed duplicates from: {table} by Session")


# %%
def update_IndraCM(date):
    date=date.replace("/","-")
    engine=connect_DB()
    #dfcm=pd.read_excel(r"C:\Users\fernandeztovar.7\Downloads\Intra tradicional sep5-8.xlsx", header=4)
    dfcm=pd.read_excel(os.getcwd()+f"\\Download folder\\Downloads Callminer_v2\\{date}\SearchReport{date}.xlsx", header=4)
    
    if len(dfcm.columns)==474:
        print("We have 474 columns, we can continue")
        df3,dfcm_tosql=preprocessing_dfcm(dfcm)

        df_indra_scores=scores_indra(dfcm_tosql)
        #return df_indra_scores
        df_indra_scores.to_sql("tbIndraDFCallminer",chunksize=5000,if_exists="append", con=engine, index=False)
        print("done IndraDFCallminer")
        upload_dfs_tosql(df3, engine)
        print("done uploads")
        remove_duplicates_SQL()
    else:
        print("Missing columns, download again")
        
    


# %%



# %%
#Update Sales Report
import imaplib
import email

# Connect to an IMAP server
def connect(server, user, password):
    m = imaplib.IMAP4_SSL(server)
    m.login(user, password)
    m.select()
    return m

# Download all attachment files for a given email
def downloaAttachmentsInEmail(m, emailid, outputdir):
    resp, data = m.fetch(emailid, "(BODY.PEEK[])")
    email_body = data[0][1]
    mail = email.message_from_bytes(email_body)
    if mail.get_content_maintype() != 'multipart':
        return
    for part in mail.walk():
        if part.get_content_maintype() != 'multipart' and part.get('Content-Disposition') is not None:
            open(outputdir + '/' + part.get_filename(), 'wb').write(part.get_payload(decode=True))

def downloadLastAttachmentsInInbox(server, user, password, outputdir,mail_from, report_type):
    m = connect(server, user, password)
    resp, items = m.search(None, f'(FROM "{mail_from}" HEADER SUBJECT "{report_type}")')
    items = items[0].split()
    downloaAttachmentsInEmail(m, items[-1], outputdir)

# %%

def downloadAllAttachments(subject):

    #date must be in format %Y-%m-%d

    Outlook = win32com.client.Dispatch("Outlook.Application")
    olNs = Outlook.GetNamespace("MAPI")
    Inbox = olNs.GetDefaultFolder(6)
    
    
    Filter = (f"@SQL=(urn:schemas:httpmail:subject LIKE '%{subject}%')")

    Items = Inbox.Items.Restrict(Filter)
    #Inbox = Inbox.Items.Restrict("[ReceivedTime] >= '" + date)
    
    Items.Sort('[ReceivedTime]', False)
    #Item = Items.GetLast()
    for Item in Items:
        for attachment in Item.Attachments:
            #print(attachment.FileName)
            attachment.SaveAsFile(f"C:\\Users\\bustossanchez.9\\OneDrive - Teleperformance\\Procesos\\Indra\\Indra\\Sales\\Indra\\{attachment.filename}")



def update_sales_table():
        
    #Download report from Lucy porto Castilla
    subject="INDRA Call list"
    
    #downloadAllAttachments(subject)
    #Download report from Victor sales

    #downloadLastAttachmentsInInbox("imap.outlook.com","fernandeztovar.7@nlsa.teleperformance.com", "Admin2020*",f"Sales/Indra","Victor.MartinezMoreno@teleperformance.com", "Energy Historical Sales Report")
    
    subject="Energy Historical Sales Report"
    
    #downloadAllAttachments(subject)
    
    #Read files
    indra_sales=pd.read_excel(r"Sales\Indra\Recordings.xlsx")
    indra_sales=indra_sales.iloc[:,:9]

    sales_victor=pd.read_excel(r"Sales\Indra\Sales historical Report updated - Finances 2023.xlsx", sheet_name="Data")

    #keep indra and year 2022

    sales_victor=sales_victor[(sales_victor["Year"]==2023) &(sales_victor["Program"]=="Indra")]

    #merge two dataframes by Phone and TPV
    crossed_with_victor=indra_sales.merge(sales_victor[["CCMS","Agent Name","Phone","State - location","Status","Zip code","City",
                                    "livevox ID ","Type of Sale","Customer name","TPV Number"]],
                                how="left", left_on="TPV", right_on="Phone")

    crossed_with_victor=crossed_with_victor.rename(columns={"PHONE":"Phone_recordings"})

    crossed_with_victor=crossed_with_victor[(~crossed_with_victor["DATE"].isin(["1 CORTE","DATE"]))
                                        ]
    crossed_with_victor=crossed_with_victor.dropna(subset=["DATE"])

    #upload to SQL
    engine=connect_DB()
    crossed_with_victor.to_sql("tbIndraSales",chunksize=5000,if_exists="replace", con=engine, index=False)

from sqlalchemy.types import BigInteger
def clean_Phone(x):
    try: 
        
        if "-" in x:
            x=x.split("-")[0]
            x=x.replace(" ","")

        return int(x)
    
    except:
        return x

def clean_TPV(x):
        try: 
            x=x.replace(" ","")
            return int(x)
        except:
            return x
def update_sales_table_v2():
    subject="Indra Energy Call list"
    
    downloadAllAttachments(subject)
    print ("Indra Energy Call list downloaded")

    subject="Energy Historical Sales Report"

    downloadAllAttachments(subject)
    print ("Energy Historical Sales Report downloaded")
    
    #Read files
    indra_sales=pd.read_excel(r"Sales\Indra\Recordings.xlsx")
    indra_sales=indra_sales.iloc[:,:9]
    indra_sales=indra_sales.rename(columns={"Unnamed: 6":"LOB", "Unnamed: 7":"agent_id","Unnamed: 8":"Customer Name"})
    indra_sales=indra_sales[(~indra_sales["DATE"].isin(["1 CORTE","DATE"]))]
    indra_sales=indra_sales.dropna(subset=["DATE"])
    indra_sales=indra_sales[1762:]

    indra_sales=indra_sales.drop_duplicates(subset=["TPV"], keep="last")

    
    indra_sales["TPV"]=indra_sales["TPV"].apply(lambda x: clean_TPV(x))
    indra_sales["TPV"]=pd.to_numeric(indra_sales["TPV"])
    #Dejar el TPV como int

    sales_victor=pd.read_excel(r"Sales\Indra\Sales historical Report updated - Finances 2023.xlsx", sheet_name="Data")

    #keep indra and year 2022
    

    sales_victor=sales_victor[(sales_victor["Year"].isin([2022,2023])) &(sales_victor["Program"]=="Indra")]

    #Limpiar phone y solo dejar un telefono, quitar guiones y espacios en blanco, y dejar como int también
    
    sales_victor["Phone"]=sales_victor['Phone'].apply(lambda x: clean_Phone(x))
    sales_victor["Phone"]=pd.to_numeric(sales_victor["Phone"])
    
    engine=connect_DB()

    indra_sales.to_sql("tbIndraSalesRecordings", con=engine, index=False, if_exists="replace", dtype={"TPV": BigInteger()})

    sales_victor.to_sql("tbIndraSalesFinancial", con=engine, index=False, if_exists="replace", dtype={"Phone": BigInteger()})






if __name__=="__main__":
    update_IndraCM(date)
    update_sales_table_v2()