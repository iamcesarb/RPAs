
# %% [markdown]
# HC Cleaning

# %%
from enum import auto
import pandas as pd
import numpy as np
import urllib
import sqlalchemy as sa
import warnings
from unidecode import unidecode

warnings.filterwarnings("ignore")

# %%
#Read and clean Head_count


def remove_accents(x):

    #convert plain text to utf-8
    u = unidecode(x, "utf-8")
    #convert utf-8 to normal text
    return (unidecode(u))

def HC_cleaning():
    HC=pd.read_excel(r"Data\Most Recent HC.xlsx", sheet_name="CX", index_col=None)
    #HC=pd.read_excel(r"Data\Breakdown DD 07042022 Final.xlsx", sheet_name="CX", index_col=None)
    HC_rel=HC.iloc[:,[0,3,4,5,7,9,10,11,12,13,14]]
    HC_rel.columns=HC_rel.columns.str.lower()
    
    HC_rel.rename(columns={HC_rel.columns[0]:"ccmsid",
                            HC_rel.columns[1]:"agentname",
                            HC_rel.columns[2]:"agentsf",
                            HC_rel.columns[3]:"email",
                            HC_rel.columns[4]:"supervisor",
                            HC_rel.columns[5]:"accm",
                            HC_rel.columns[6]:"lob",
                            HC_rel.columns[7]:"site",
                            HC_rel.columns[8]:"wave",
                            HC_rel.columns[9]:"tenure",
                            HC_rel.columns[10]:"tenure days"
                            
                                              }, inplace=True)
        
    HC_rel=HC_rel.replace(",","", regex=True)
    HC_rel=HC_rel.apply(lambda x: x.astype(str).str.lower())

    HC_rel["agentname"]=HC_rel["agentname"].apply(lambda x: remove_accents(x))

    return HC_rel

# %% [markdown]
# RawData cleaning

# %%
#RawData


def main(date):
    date=date.replace("/","-")
    HC_rel=HC_cleaning()
    RawData=pd.read_excel(f"Download folder\\Downloads Callminer_v2\\{date}\\SearchReport{date}.xlsx", header=4)


    RawData=RawData.drop(RawData.columns[[1,13,-1]], axis=1)

    RawData.rename(columns={RawData.columns[7]:"datetime",
                            RawData.columns[1]:"email"},
                            inplace=True
    )

    #Column names to lowercase and replace names
    RawData.columns=RawData.columns.str.lower()

    RawData.columns=[name.replace("categories.","").replace("`","") for name in RawData.columns]

    #merge and create new table
    RawUpdated=pd.merge(HC_rel, RawData, on="email", how="right")


    #Replacing hit and miss with 1's and 0's and leaving the columns as factors
    RawUpdated.iloc[:,24:]=RawUpdated.iloc[:,24:].replace("Hit",1, regex=True).replace("Miss",0, regex=True)

    #Rename columns by component name
    RawUpdated.columns=RawUpdated.columns[:24].to_list()+[name.split(".")[2] if len(name.split("."))==3 else name.replace("."," ") for name in RawUpdated.columns[24:]]

    #Drop duplicates by EurekaID and columns duplicated
    RawUpdated.drop_duplicates(subset=["eureka id"], inplace=True)
    RawUpdated=RawUpdated.loc[:,~RawUpdated.columns.duplicated()]

    #Filtering by conditions
    #RawUpdated=RawUpdated[(RawUpdated["higher percentage silence"]!=0) 
    #                    &(RawUpdated["average confidence"]!=0)
    #                    &(RawUpdated["word count"]!=0)]

    def swap_columns(df, col1, col2):
        col_list = list(df.columns)
        x, y = col_list.index(col1), col_list.index(col2)
        col_list[y], col_list[x] = col_list[x], col_list[y]
        df = df[col_list]
        return df

    #Swap email and agentname columns
    RawUpdated=swap_columns(RawUpdated, "email","agentname")

    #Sort and reset_index
    RawUpdated=RawUpdated.sort_values(by="eureka id").reset_index(drop=True)

    auto_fail=RawUpdated[["datetime", "eureka id","delivery misbehavior", "health", "legal", "blame customer", "describes doordash in a derogatory manner", 
            "profanity from agent to customer", "rude/arrogant", "agent is yelling and screaming", "confidential information", "covid 19"]]

    auto_fail.columns=[name.replace(" ","_").replace("/","").replace("^_","").replace("_$","") for name in auto_fail.columns]

    auto_fail=auto_fail.melt(id_vars=["datetime","eureka_id"])

    auto_fail=auto_fail[auto_fail["value"]>0]
    auto_fail["variable"]=auto_fail["variable"].str.replace("_", " ").str.replace("&"," ").str.capitalize()


    auto_fail=auto_fail.sort_values(by=["datetime","eureka_id","variable"]).reset_index(drop=True)

    #Security Riders/Informative 
    sec_riders=RawUpdated[["datetime","eureka id","call avoidance","call unexpetedly disconnected",
            "higher percentage silence", "sil hold", "sil transfer","lower avg confidence"]]

    sec_riders.columns=[name.replace(" ","_").replace("/","").replace("^_","").replace("_$","") for name in sec_riders.columns]

    sec_riders=sec_riders.melt(id_vars=["datetime","eureka_id"])

    sec_riders=sec_riders[sec_riders["value"]>0]
    sec_riders["variable"]=sec_riders["variable"].str.replace("_", " ").str.replace("&"," ").str.capitalize()


    sec_riders=sec_riders.sort_values(by=["datetime","eureka_id","variable"]).reset_index(drop=True)


    # %%
    #Channel
    #3. Channel 
    channel=RawUpdated[["datetime", "eureka id", "app not working", "chat not working", "system is not running",
            "website not working","been on hold or waiting", "sorry for delay", "bad connection", 
            "call is breaking up", "echo sound or interference","trouble hearing", "youre cutting out"]]

    channel.columns=[name.replace(" ","_").replace("/","").replace("^_","").replace("_$","") for name in channel.columns]

    channel=channel.melt(id_vars=["datetime","eureka_id"])

    channel=channel[channel["value"]>0]
    channel["variable"]=channel["variable"].str.replace("_", " ").str.replace("&"," ").str.capitalize()


    channel=channel.sort_values(by=["datetime","eureka_id","variable"]).reset_index(drop=True)
    # %%
    #TCD

    TCD=RawUpdated[["datetime", "eureka id","tcddd app feedback and troubleshooting", "tcddd availability dashers folder", 
            "tcddd c and r compensation denied", "tcddd cancel my order", 
            "tcddd change delivery address date time", "tcddd delivery too late", 
            "tcddd feedback about dasher merchant", "tcddd food quality issue", "tcddd help placing an order",
            "tcddd missing or incorrect items", "tcddd never delivered", "tcddd order cart adjustment", 
            "tcddd order status inquiries", "tcddd refund credit inquiry", "tcddd report unauthorized charges",
            "tcddd update account information", "tcddd why was my order cancelled", "tcddd wrong order received"]]

    TCD.columns=[name.replace(" ","_").replace("/","").replace("^_","").replace("_$","") for name in TCD.columns]

    TCD=TCD.melt(id_vars=["datetime","eureka_id"])

    TCD=TCD[TCD["value"]>0]
    TCD["variable"]=TCD["variable"].str.replace("_", " ").str.replace("&"," ").str.capitalize()


    TCD=TCD.sort_values(by=["datetime","eureka_id","variable"]).reset_index(drop=True)


    # %%
    #Dissatisfactions
    #Dsat1
    RawUpdated["attonery_mention"]=RawUpdated["attorney mentions"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["bbb"]=RawUpdated["better business bureau"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["file_complaint"]=RawUpdated["file complaint"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["going_sue"]=RawUpdated["im going to sue you"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["social_media"]=RawUpdated["social media"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["fraud_d"]=RawUpdated["this is a fraud"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["robery_d"]=RawUpdated["this is a robery"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["illegal"]=RawUpdated["this is illegal"].apply(lambda x: 0 if x==1 else (100))

    RawUpdated["dsat1"]=np.where((RawUpdated["attonery_mention"]==0) |
                                    (RawUpdated["bbb"]==0) | 
                                    (RawUpdated["file_complaint"]==0) | 
                                    (RawUpdated["going_sue"]==0) |
                                    (RawUpdated["social_media"]==0)|
                                    (RawUpdated["fraud_d"]==0)|
                                    (RawUpdated["robery_d"]==0)|
                                    (RawUpdated["illegal"]==0), 0,100)/8

    dissat_trans1=RawUpdated[["eureka id","datetime","agentname", "attonery_mention", "bbb", 
                            "file_complaint","going_sue", "social_media", "fraud_d", "robery_d", "illegal"]]

    dissat_trans1=dissat_trans1.melt(id_vars=["agentname","eureka id","datetime"])
    dissat_trans1=dissat_trans1[dissat_trans1["value"]==0]
    dissat_trans1=dissat_trans1.sort_values(by=["agentname","eureka id","datetime"]).reset_index(drop=True)


    #Dsat2
    RawUpdated["angry_d"]=RawUpdated["angry"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["annoyded_frustration"]=RawUpdated["annoyed  frustrated"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["kidding"]=RawUpdated["are you kidding"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["disappointed_d"]=RawUpdated["disappointed"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["ignoring_d"]=RawUpdated["ignoring"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["shame_d"]=RawUpdated["shame"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["tired_d"]=RawUpdated["tired"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["wasted_time"]=RawUpdated["wasted time"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["profanity_d"]=RawUpdated["profanity"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["refund_d"]=RawUpdated["refund request"].apply(lambda x: 0 if x==1 else (100))

    RawUpdated["dsat2"]=np.where((RawUpdated["angry_d"]==0) |
                                    (RawUpdated["annoyded_frustration"]==0) | 
                                    (RawUpdated["kidding"]==0) | 
                                    (RawUpdated["disappointed_d"]==0) |
                                    (RawUpdated["ignoring_d"]==0)|
                                    (RawUpdated["shame_d"]==0)|
                                    (RawUpdated["tired_d"]==0)|
                                    (RawUpdated["wasted_time"]==0)|
                                    (RawUpdated["profanity_d"]==0)|
                                    (RawUpdated["refund_d"]==0)
                                    , 0,100)/10

    dissat_trans2=RawUpdated[["eureka id","datetime","agentname","angry_d","annoyded_frustration","kidding",
                                            "disappointed_d","ignoring_d","shame_d","tired_d","profanity_d",
                                            "wasted_time","refund_d"]]

    dissat_trans2=dissat_trans2.melt(id_vars=["agentname","eureka id","datetime"])
    dissat_trans2=dissat_trans2[dissat_trans2["value"]==0]
    dissat_trans2=dissat_trans2.sort_values(by=["agentname","eureka id","datetime"]).reset_index(drop=True)

    #Dsat3
    RawUpdated["upset"]=RawUpdated["a little upset"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["bad_service"]=RawUpdated["bad service"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["dissatisfied"]=RawUpdated["i am dissatisfied"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["not_accept"]=RawUpdated["i cannot accept this"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["problems_before"]=RawUpdated["i had problems before"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["no_answer"]=RawUpdated["nobody answers"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["no_care"]=RawUpdated["nobody cares"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["unbelievable_d"]=RawUpdated["unbelievable"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["unhappy_d"]=RawUpdated["unhappy"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["no_help"]=RawUpdated["you are not helping"].apply(lambda x: 0 if x==1 else (100))
    RawUpdated["unprofessional_d"]=RawUpdated["unprofessional"].apply(lambda x: 0 if x==1 else (100))


    RawUpdated["dsat3"]=np.where((RawUpdated["upset"]==0) |
                                    (RawUpdated["bad_service"]==0) | 
                                    (RawUpdated["dissatisfied"]==0) | 
                                    (RawUpdated["not_accept"]==0) |
                                    (RawUpdated["problems_before"]==0)|
                                    (RawUpdated["no_answer"]==0)|
                                    (RawUpdated["no_care"]==0)|
                                    (RawUpdated["unbelievable_d"]==0)|
                                    (RawUpdated["unhappy_d"]==0)|
                                    (RawUpdated["no_help"]==0)|
                                    (RawUpdated["unprofessional_d"]==0)
                                    , 0,100)/11

    dissat_trans3=RawUpdated[["eureka id","datetime","agentname","upset","bad_service", "dissatisfied","not_accept",
                                            "problems_before", "unprofessional_d",
                                            "no_answer","no_care","unbelievable_d","unhappy_d","no_help"]]

    dissat_trans3=dissat_trans3.melt(id_vars=["agentname","eureka id","datetime"])
    dissat_trans3=dissat_trans3[dissat_trans3["value"]==0]
    dissat_trans3=dissat_trans3.sort_values(by=["agentname","eureka id","datetime"]).reset_index(drop=True)



    # %%
    #Score Dsat
    RawUpdated["dsat_general"]=(RawUpdated["dsat1"] + RawUpdated["dsat2"] + RawUpdated["dsat3"])/3

    RawUpdated["datetime"]=RawUpdated["datetime"].astype(str)
    #RawUpdated["disconnect time"]=RawUpdated["disconnect time"].astype(str)
    TCD["datetime"]=TCD["datetime"].astype(str)
    sec_riders["datetime"]=sec_riders["datetime"].astype(str)
    dissat_trans1["datetime"]=dissat_trans1["datetime"].astype(str)
    dissat_trans2["datetime"]=dissat_trans2["datetime"].astype(str)
    dissat_trans3["datetime"]=dissat_trans3["datetime"].astype(str)


    #Sentiment
    Sentiment=RawUpdated[["datetime", "eureka id", 'nsf intensifier before negative word','nsf inverter before positive word','nsf pronouns before negative words',
        'nsl intensifier before negative word','nsl inverter before positive word','nsl pronouns before negative words','nsm intensifier before negative word',
        'nsm inverter before positive word','nsm pronouns before negative words','ns intensifier before negative word','ns inverter before positive word',
        'ns pronouns before negative words','psf intensifiers before positive words','psf inverter before negative words','psf pronouns before positive words',
        'psl intensifiers before positive words','psl inverter before negative words','psl pronouns before positive words','psm intensifiers before positive words',
        'psm inverter before negative words','psm pronouns before positive words','ps intensifiers before positive words','ps inverter before negative words',
        'ps pronouns before positive words']]

    Sentiment.columns=[name.replace(" ","_").replace("/","").replace("^_","").replace("_$","") for name in Sentiment.columns]

    Sentiment=Sentiment.melt(id_vars=["datetime","eureka_id"])

    Sentiment=Sentiment[Sentiment["value"]>0]
    Sentiment["variable"]=Sentiment["variable"].str.replace("_", " ").str.replace("&"," ").str.capitalize()


    Sentiment=Sentiment.sort_values(by=["datetime","eureka_id","variable"]).reset_index(drop=True)

    # %%
    def connect_DB():
        connection_string = (
            'Driver={ODBC Driver 17 for SQL Server};'
            'SERVER=TPCCP-DB04\SCBACK;'
            'Database=Analytics;'
            'Trusted_Connection=yes;'
        )
        connection_uri = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_string)}"
        engine = sa.create_engine(connection_uri, fast_executemany=True)
        return engine

    
    # %%
    #Clean column names
    RawUpdated.columns=[name.replace("-","").replace("|","").replace(" ","").replace("&","").replace("/","") for name in RawUpdated.columns]
    dissat_trans1.columns=[name.replace("-","").replace("|","").replace(" ","").replace("&","").replace("/","") for name in dissat_trans1.columns]
    dissat_trans2.columns=[name.replace("-","").replace("|","").replace(" ","").replace("&","").replace("/","") for name in dissat_trans2.columns]
    dissat_trans3.columns=[name.replace("-","").replace("|","").replace(" ","").replace("&","").replace("/","") for name in dissat_trans3.columns]

    engine=connect_DB()
    RawUpdated.to_sql("tbDDall", if_exists="append", con=engine, index=False)

    dissat_trans1.to_sql("tbDDDsatransUNO", if_exists="append", con=engine, index=False)

    dissat_trans2.to_sql("tbDDDsatransDOS", if_exists="append", con=engine, index=False)

    dissat_trans3.to_sql("tbDDDsatransTRES", if_exists="append", con=engine, index=False)

    TCD.to_sql("tbDDtcd", if_exists="append", con=engine, index=False)

    sec_riders.to_sql("tbDDsecriders", if_exists="append", con=engine, index=False)

    channel.to_sql("tbDDchannel", if_exists="append", con=engine, index=False)

    auto_fail.to_sql("tbDDautofail", if_exists="append", con=engine, index=False)

    Sentiment.to_sql("tbDDsentiment", if_exists="append", con=engine, index=False)

if __name__=="__main__":
    
    main(date)

