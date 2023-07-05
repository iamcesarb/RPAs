# %%
import pyodbc
import pymssql
import pandas as pd
import glob
import sqlalchemy as sa
import urllib
import os
from datetime import datetime, timedelta
import time
#from imapclient import IMAPClient
import re
import imaplib
from imaplib import IMAP4_SSL
import email
from email.header import make_header, decode_header
import email.policy as epolicy
import pyodbc
import imaplib
import email
from sqlalchemy.types import BigInteger, String
import numpy as np



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
    dfcm_tosql["Date_only"]=dfcm_tosql["Date/Time"].dt.date
    dfcm_tosql["Time"]=dfcm_tosql["Date"].dt.time



    #-------------------------
    outliers = {"Outliers":["Do not contact alert", "Not interaction", "Spanish calls", "Voicemail", "Wrong Number"],
                            "Outcomes":['Do Not Contact', "Excessive Silence","Hang Up", "incoming calls", "Mono calls", "Voicemail-"]}

    list_outliers_cat=[]

    for key, values in outliers.items():
        for value in values:
            name=(key + '.' + value)
            list_outliers_cat.append(name)
            
    # Create the new ouliers dataframe 
    outliers_df = dfcm_tosql.loc[(dfcm_tosql[list_outliers_cat] == 1).any(axis=1)]

    #Removes outliers from the original dataframe
    dfcm_tosql = dfcm_tosql.loc[~(dfcm_tosql[list_outliers_cat] == 1).any(axis=1)]
    

    # Drop outliers
    dfcm_tosql = dfcm_tosql.drop(dfcm_tosql.columns[dfcm_tosql.columns.str.contains("outliers", case=False)], axis=1)
    dfcm_tosql = dfcm_tosql.drop(dfcm_tosql.columns[dfcm_tosql.columns.str.contains("outcomes", case=False)], axis=1)

    #metadata
    meta = ["Eureka ID", "Agent","Agent Group","ANI","Average Confidence","contact_id","Date","Date/Time","Direction","Disp_Name","Duration",
                "Hold Time","Longest Silence","Percent Silence",
                "Real Direction","Recorder ID","Silence Time","Skill name","skill_no","Tempo","To"]

    outliers_cats_tosql = meta+list_outliers_cat

    outliers_df = outliers_df[outliers_cats_tosql]

    scores_values = {'Positive':{
                        '2' : ['Product Knowledge.Negative Phrasing'],
                        '3' :['Acknowledge Statement.Info - CUS about customer service',
                                'Acknowledge Statement.Info - CUS about happiness guarantee',
                                'Acknowledge Statement.Info - CUS about membership',
                                'Acknowledge Statement.Info - CUS about pre-priced guarantee',
                                'Acknowledge Statement.Info - CUS about professionals',
                                'Active Listening.Recap',
                                'Closing QA.Assumptive Language & Urgency',
                                'Closing QA.Call Control',
                                'Closing QA.Set a call back',
                                'Tone Rapport.Did we greet the call properly',
                                'Tone Rapport.Set the agenda',
                                'Tone Rapport.Confident tone',
                                'Objection Handling.Not Lead by offering a discount',
                                'Objection Handling.Sell On Cancellation',
                                'Objection Handling.Reasoning for objections'],
                        '4':["Compliance.Cross Selling & Up Selling",
                                "Tone Rapport.Build Rapport"],
                        '5':["Compliance.Legal Terms",
                                "Compliance.Post Booking Script"],
                        '6':["Compliance.Recorded line"]
                        
                                                      
                },
                'Critical errors':[ 'Critical Errors.Language Around Pre-priced Pros and Pro Behavior',
                                        'Critical Errors.Other language',
                                        'Critical Errors.Payment Language',
                                        'Critical Fail.Dispo CallBack',
                                        'Critical Fail.Dispo connection issue',
                                        'Critical Fail.Dispo legal terms',
                                        'Critical Fail.Professionalism throughout the call']
                }





    dfcm_tosql['Product Knowledge Score']=0
    dfcm_tosql['Tone/ Rapport Score']=0
    dfcm_tosql['Active Listening Score']=0
    dfcm_tosql['Objection Handling Score']=0
    dfcm_tosql['Closing Score']=0
    dfcm_tosql['Compliance Score']=0
    
    #Positive cats
    for i in range (2,7):
            
        for category in scores_values['Positive'][str(i)]:
            #Tone / Rapport
            if 'Tone Rapport' in category:
                dfcm_tosql['Tone/ Rapport Score']=dfcm_tosql['Tone/ Rapport Score']+(dfcm_tosql[category]*i)
            if 'Active Listening' in category:
                dfcm_tosql['Active Listening Score']=dfcm_tosql['Active Listening Score']+(dfcm_tosql[category]*i)
            if 'Acknowledge Statement' in category:  #ojo on esta, porque el componente principal es en realidad Product knowledge
                dfcm_tosql['Product Knowledge Score']=dfcm_tosql['Product Knowledge Score']+(dfcm_tosql[category]*i)
            if 'Objection Handling' in category:
                dfcm_tosql['Objection Handling Score']=dfcm_tosql['Objection Handling Score']+(dfcm_tosql[category]*i)
            if 'Closing QA' in category:
                #Aplicamos filtro de sales para la categoria correspondiente
                if category == "Closing QA.Set a call back":
                    dfcm_tosql['Closing Score']=dfcm_tosql['Closing Score']+(dfcm_tosql[category]*i).where(dfcm_tosql['Informative.Not sales']==1, other=dfcm_tosql['Closing Score']+i)
                else:
                    dfcm_tosql['Closing Score']=dfcm_tosql['Closing Score']+(dfcm_tosql[category]*i)
            if 'Compliance' in category:
                if (category == "Compliance.Cross Selling & Up Selling" or 
                    category == "Compliance.Legal Terms" or 
                    category == "Compliance.Post Booking Script"):
                    
                    
                    dfcm_tosql['Compliance Score']=(dfcm_tosql['Compliance Score']+dfcm_tosql[category]*i).where(dfcm_tosql['Informative.Sales Angi']==1, other=dfcm_tosql['Compliance Score']+i)
                
                if category == "Compliance.Recorded line": #just for outbound skill name  it can be outbound or OB 
                    dfcm_tosql['Compliance Score']=(dfcm_tosql['Compliance Score']+dfcm_tosql[category]*i).where(dfcm_tosql['Skill name'].str.contains(r'out|OB', flags=re.IGNORECASE, regex=True), other=dfcm_tosql['Compliance Score']+i)


    #Negative cats
    #for i in range (2,4):
        
    #    for category in scores_values['Negative'][str(i)]:
    #        if 'Tone Rapport' in category:
    #            dfcm_tosql['Tone/ Rapport Score']=dfcm_tosql['Tone/ Rapport Score']-(dfcm_tosql[category]*i)
    #        if 'Objection Handling' in category:
    #            dfcm_tosql['Objection Handling Score']=dfcm_tosql['Objection Handling Score']-(dfcm_tosql[category]*i)
    #        if "Product Knowledge" in category:
    #            dfcm_tosql['Product Knowledge Score']=dfcm_tosql['Product Knowledge Score']-(dfcm_tosql[category]*i)



    dfcm_tosql['Product Knowledge %']=dfcm_tosql['Product Knowledge Score']/17*100
    dfcm_tosql['Tone/ Rapport %']=dfcm_tosql['Tone/ Rapport Score']/13*100
    dfcm_tosql['Active Listening %']=dfcm_tosql['Active Listening Score']/3*100
    dfcm_tosql['Objection Handling %']=dfcm_tosql['Objection Handling Score']/9*100
    dfcm_tosql['Closing %']=dfcm_tosql['Closing Score']/9*100
    dfcm_tosql['Compliance %']=dfcm_tosql['Compliance Score']/20*100


    dfcm_tosql['Total Score']= (    dfcm_tosql['Product Knowledge Score']+
                                    dfcm_tosql['Tone/ Rapport Score']+
                                    dfcm_tosql['Active Listening Score']+
                                    dfcm_tosql['Objection Handling Score']+
                                    dfcm_tosql['Closing Score']+
                                    dfcm_tosql['Compliance Score'])


    for category in scores_values['Critical errors']:
       dfcm_tosql['Total Score']=np.where(dfcm_tosql[category]==1,0,dfcm_tosql['Total Score'])
       
    dfcm_tosql['Total Score']=dfcm_tosql['Total Score'].apply(lambda x: float(x)/71*100)



    #-------------------------

    dfcm_tosql["Rebuttals Total"]=(dfcm_tosql["Rebuttals 02.Cancellation 2"]+
                                dfcm_tosql["Rebuttals 02.Customer Service 2"]+
                                dfcm_tosql["Rebuttals 02.Discount 2"]+
                                dfcm_tosql["Rebuttals 02.Email after booking 2"]+
                                dfcm_tosql["Rebuttals 02.Handyman 2"]+
                                dfcm_tosql["Rebuttals 02.Happiness Guarantee 2"]+
                                dfcm_tosql["Rebuttals 02.I can talk to xxxxx 2"]+
                                dfcm_tosql["Rebuttals 02.I can wait 2"]+
                                dfcm_tosql["Rebuttals 02.Membership 2"]+
                                dfcm_tosql["Rebuttals 02.No booking Windows 2"]+
                                dfcm_tosql["Rebuttals 02.Other 2"]+
                                dfcm_tosql["Rebuttals 02.Pre-Priced Guarantee 2"]+
                                dfcm_tosql["Rebuttals 02.Professionals 2"]+
                                dfcm_tosql["Rebuttals 02.Re schedule 2"]+
                                dfcm_tosql["Rebuttals 02.Security 2"]+
                                dfcm_tosql["Rebuttals 02.Urgency2"])
                                 

    dfcm_tosql["Objections Total"]=(dfcm_tosql["Sales objections.Can`t give card number"]+
                                dfcm_tosql["Sales objections.Dont want to pay until speak to pro"]+
                                dfcm_tosql["Sales objections.I don`t have my card handy"]+
                                dfcm_tosql["Sales objections.I don`t want to provide my own materials"]+
                                dfcm_tosql["Sales objections.I dont have a card to put on file"]+
                                dfcm_tosql["Sales objections.I dont want to pay ahead of time"]+
                                dfcm_tosql["Sales objections.I need the service sooner"]+
                                dfcm_tosql["Sales objections.I need time to think"]+
                                dfcm_tosql["Sales objections.I need to consult before I make a decision"]+
                                dfcm_tosql["Sales objections.I need to work out scheduling"]+
                                dfcm_tosql["Sales objections.I want to get other quotes before deciding"]+
                                dfcm_tosql["Sales objections.I want to pay cash only"]+
                                dfcm_tosql["Sales objections.Need a better price"]+
                                dfcm_tosql["Sales objections.Someone available"]+
                                dfcm_tosql["Sales objections.Talk to Pro"]+
                                dfcm_tosql["Sales objections.The customer wants to know who the pro is"]+
                                dfcm_tosql["Sales objections.This price is too high"]
    )


    
    dfcm_tosql["Powerful_phrase_total"]=(dfcm_tosql["Powerful Phrases.Powerful_Phrases.Absolutely agree with you"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.Aim to provide a friendly service"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.Anything Else"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.Be ideal"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.Because you`re a valued customer"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.Can certainly help you"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.Favorite Option"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.Good News"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.I can highly recommend"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.I hope you enjoy your"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.I would be more than happy"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.I`ll do"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.If you can, then I can"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.Is going to be absolutely awesome!"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.Looking best price"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.People prefer"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.Quick Review"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.Splendid! All that is left to do now"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.that is an interesting idea"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.That is exaclty right"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.Thats a great question"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.This is going to be an ideal choice"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.Understand more about it"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.We are going to enhance your life"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.Yes, it is essential that you"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.You are going to feel the quality"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.Your house is worth it"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.Your house will gain an exceptional value"]+
                                        dfcm_tosql["Powerful Phrases.Powerful_Phrases.Your house will never looked so good"])

    dfcm_tosql["Total Ack"]=(dfcm_tosql["Acknowledge Statement.Info - CUS about cancellation"]+
                            dfcm_tosql["Acknowledge Statement.Info - CUS about customer service"]+
                            dfcm_tosql["Acknowledge Statement.Info - CUS about discount"]+
                            dfcm_tosql["Acknowledge Statement.Info - CUS about email after booking"]+
                            dfcm_tosql["Acknowledge Statement.Info - CUS about handyman"]+
                            dfcm_tosql["Acknowledge Statement.Info - CUS about happiness guarantee"]+
                            dfcm_tosql["Acknowledge Statement.Info - CUS about I can talk to xxxxx"]+
                            dfcm_tosql["Acknowledge Statement.Info - CUS about I can wait"]+
                            dfcm_tosql["Acknowledge Statement.Info - CUS about membership"]+
                            dfcm_tosql["Acknowledge Statement.Info - CUS about No booking Windows"]+
                            dfcm_tosql["Acknowledge Statement.Info - CUS about other"]+
                            dfcm_tosql["Acknowledge Statement.Info - CUS about payment secur"]+
                            dfcm_tosql["Acknowledge Statement.Info - CUS about pre-priced guarantee"]+
                            dfcm_tosql["Acknowledge Statement.Info - CUS about professionals"]+
                            dfcm_tosql["Acknowledge Statement.Info - CUS about re schedule"]+
                            dfcm_tosql["Acknowledge Statement.Info - CUS about security"]+
                            dfcm_tosql["Acknowledge Statement.Info - CUS about urgency"]
                            )

    dfcm_tosql["Total TCD"]=(dfcm_tosql['TCD.Cleaning_services']+
                            dfcm_tosql['TCD.General_Handyman']+
                            dfcm_tosql['TCD.Handyman_service']+
                            dfcm_tosql['TCD.Installation_Services']+
                            dfcm_tosql['TCD.Outdoor_Projects']+
                            dfcm_tosql['TCD.Renovation or Remodel Services_']+
                            dfcm_tosql['TCD.Repair_Services'])        


    dfcm_tosql["Total Critical_errors"]=(dfcm_tosql['Critical Errors.Language Around Pre-priced Pros and Pro Behavior']+
                                            dfcm_tosql['Critical Errors.Payment Language'])



    dfcm_tosql["Sale"]=np.where(dfcm_tosql["Disp_Name"].isin(['Sale','Sale - One Time',
                                                            'Sale - Recurring/Commit',
                                                            'Sale- Membership + Service']),1,0)


    dfcm_tosql["NoSale"]=np.where(dfcm_tosql["Disp_Name"].isin(["No Sale","Retry - No Schedule","Retry - Schedule callback","Retry - Schedule callback from me",
                                                        "Busy","Call back / needs more time","Credit card declined","DNC List","Do Not Call (DNC)",
                                                        "Looking For Quote Only","Out of scope (Not a service we provide)","Pricing too high","Already booked"]),1,0)
   


    dfcm_tosql["Other"]=np.where(dfcm_tosql["Disp_Name"].isin(["Answering Machine","Answering Machine - Agent left VM","Answering Machine Left Message",
                                                                "Abandon","Hung up","Invalid Number","No Answer","Called Party Hang Up","Handover",
                                                                "Transfer to Customer Service","Agent Abandon","Agent Override Answering Machine - System Left Mes",
                                                                "Connection Issue","Disconnect","Error","Force Agent Disconnect","Forced Remove Call","System Failure"]),1,0)

    dfcm_tosql["RetentionLOB"]=np.where(dfcm_tosql["Disp_Name"].isin(["Cancelled Booking","Customer service issue","Pause Service","Save sale- Credit","No Save",
                                                                    "Save Sale- Customer Education","save sale- Partial Refund","Save Sale- Price Match","Save Sale- Rescheduled"]),1,0)

    dfcm_tosql["CallsInteraction"]=np.where((dfcm_tosql["Objections Total"]+dfcm_tosql["Rebuttals Total"]+dfcm_tosql["Total TCD"]+
                                            dfcm_tosql["Informative.Call Back Suggestions.Call Back"]+dfcm_tosql["Informative.Positive Sentiment"]+
                                            dfcm_tosql["Soft Skills.Empathy_"]+dfcm_tosql["Powerful Phrases.Powerful_Phrases"]+
                                            dfcm_tosql["Total Ack"]+dfcm_tosql["Total Critical_errors"])>=1,1,0)

    dfcm=dfcm[id_vars.to_list()+Componentes]

    dfcm=dfcm.dropna(subset=["Eureka ID"])

    dfcm = dfcm.replace(['Miss'],0)
    dfcm = dfcm.replace(['Hit'],1)
    df2=dfcm.melt(id_vars=id_vars)
    df3=df2[(df2["value"]!=0)]
    df3=df3.rename(columns={'variable': 'Category', 'value': 'Component'})

    #Separate into Category and component
    df3[["Folder",'Category','Component']] = df3['Category'].str.split('.', expand = True)
    
    return df3, dfcm_tosql, outliers_df


# %%
def remove_duplicates_SQL():

    connection_string = (
        'Driver={ODBC Driver 17 for SQL Server};'
        'SERVER=TPCCP-DB09\SCNEAR;'
        'Database=Analytics;'
        'Trusted_Connection=yes;'
    )
    cnxn = pyodbc.connect(connection_string)
    cursor = cnxn.cursor()

    for table in ["tbAngiSale","tbAngiNoSale"]:
        remove_dup=f"""
                    WITH cte AS (
                                SELECT * ,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY 
                                            [Master Contact ID],
                                            [contact_id]
                                        ORDER BY 
                                            [Master Contact ID],
                                            [contact_id]
                                    ) row_num
                                
                                    FROM Analytics.dbo.{table}
                            )
                            DELETE FROM cte
                            WHERE row_num > 1;
                    """
        #print(remove_dup)
        #cursor.execute(remove_dup)
        #cnxn.commit()
        #print(f"removed duplicates from: {table}")

    for table in ["tbAngiRebuttals","tbAngiTCD","tbAngiSalesObjections","tbAngiInformative" ]:
        remove_dup=f"""
                    WITH cte AS (
                                SELECT * ,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY 
                                            [Eureka ID],
                                            [contact_id],
                                            [Category],
                                            [Component],
                                            [Folder]
                                        ORDER BY 
                                            [Eureka ID],
                                            [contact_id],
                                            [Category],
                                            [Component],
                                            [Folder]
                                    ) row_num
                                
                                    FROM Analytics.dbo.{table}
                            )
                            DELETE FROM cte
                            WHERE row_num > 1;
                    """
        #cursor.execute(remove_dup)
        #cnxn.commit()
        #print(f"removed duplicates from: {table}")



    for table in ["tbAngiDFCallminer","tbAngiOutliers","tbAngiMelted"]:
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
        print(f"removed duplicates from: {table}")


# %%

def read_dfcm2(date_dfcm:str):
    
    print(f"updating: {date_dfcm}")
    dfcm=pd.read_excel(f"Download folder\\Downloads Callminer_v2\\{date_dfcm}\\SearchReport{date_dfcm}.xlsx", header=4)
    try:
        test_cols = [col for col in dfcm.columns if 'Test' in col]
        dfcm.drop(columns=test_cols, axis=1,inplace=True)
    except: 
        pass
    try:
        dfcm.drop(columns=['Categories.TCD.Repair_Services.Repair'], axis=1,inplace=True)
    except: 
        pass
    
    return dfcm


# %%

def main(date_dfcm:str):
    """
        days_to_update: Use "1" to get yesterday or last available report
    """
    date_dfcm=date_dfcm.replace("/","-")
    engine=connect_DB()
    
    #report_links=get_all_links_from_mail(days_to_update)

    #download_report(r"\Data\callminer_report", report_links)
    #downloadAllAttachmentsInInbox("imap.outlook.com",'fernandeztovar.7@nlsa.teleperformance.com', 'Admin2020*', "Data/contacthistory")
    
    
    dfcm=read_dfcm2(date_dfcm)
    #contact_history=upload_contact_history(day+1)
    df3,dfcm_tosql, dfcm_tosql_ouliers=preprocessing_dfcm(dfcm)
    dfcm_tosql.to_sql("tbAngiDFCallminer",chunksize=5000,if_exists="append", con=engine, index=False)
    print("done AngiDFCallminer")
    df3.to_sql("tbAngiMelted",chunksize=5000,if_exists="append", con=engine, index=False)
    print("done AngiMelted")
    dfcm_tosql_ouliers.to_sql("tbAngiOutliers",chunksize=5000,if_exists="append", con=engine, index=False)
    print("done AngiOutliers")
    remove_duplicates_SQL()
        
if __name__=="__main__":
    main(date_dfcm)
