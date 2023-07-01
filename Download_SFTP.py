# %%
# %%
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
#import win32com.client
import time
from datetime import datetime, timedelta
import os
import pandas as pd
#import numpy as np
import re
import glob
#import paramiko
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from tqdm import tqdm
#from threading import Thread, Barrier
from selenium.webdriver.common.keys import Keys
import rsa
#import Login_CM
#from openpyxl import load_workbook
#import keys

# %%
import sys

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

import paramiko

# %%
def connect_to_callminer_sftp(myHostname, myUsername, myPassword)->paramiko:
        
    paramiko.util.log_to_file("paramiko.log")

    # Open a transport
    host,port = myHostname,22
    transport = paramiko.Transport((host,port))

    # Auth    
    username,password = myUsername,myPassword
    transport.connect(None,username,password)
    # Go!    
    sftp = paramiko.SFTPClient.from_transport(transport)
    
    return sftp

# %%
def connect_to_callminer_sftp(myHostname, myUsername, myPassword)->paramiko:
        
    paramiko.util.log_to_file("paramiko.log")

    # Open a transport
    host,port = myHostname,22
    transport = paramiko.Transport((host,port))

    # Auth    
    username,password = myUsername,myPassword
    transport.connect(None,username,password)
    # Go!    
    sftp = paramiko.SFTPClient.from_transport(transport)
    
    return sftp

def upload_mp3_to_sftp_callminer(filepath, sftp:paramiko,download_file_path:str,yesterday_date_str:str,start):
    # Upload
    
    #sftp.mkdir('/test/' + yesterday_date_str)
    #filepath = "/test/" + yesterday_date_str
    
    #print(filepath)
    ruta = glob.glob(download_file_path + '\\*.mp3')
    #print("ruta:",ruta)
    #for i in range(1,len(ruta)):
    for i in tqdm(range(start,len(ruta))):   
        localpath = ruta[i]
        #
        # print(localpath)
        name_file = ruta[i].split('\\')[-1]
        # print(name_file)
        sftp.put(localpath,filepath + '/' + name_file, confirm=False)
        time.sleep(1)

def upload_xml_to_sftp_callminer(filepath, sftp:paramiko,download_file_path_xml:str,yesterday_date_str:str,start):
    # Upload
    
    #sftp.mkdir('/test/' + yesterday_date_str)
    
    #print(filepath)
    ruta = glob.glob(download_file_path_xml + '\\*.xml')
    #print("ruta:",ruta)

    for i in tqdm(range(start,len(ruta))):
        localpath = ruta[i]
        name_file = ruta[i].split('\\')[-1]
        # print(name_file)
        sftp.put(localpath,filepath + '/' + name_file, confirm=False)
        #print(filepath + '/' + name_file)
        time.sleep(1)   
        
# %%
def read_key(path_privateKey):

    # Read the private_key
    with open(path_privateKey, "rb") as private_file:
        private_key = rsa.PrivateKey.load_pkcs1(private_file.read())

    return private_key

def decrypt_credentials(credentials, private_key):

    encUser = credentials[0]
    encPass = credentials[1]

    user = rsa.decrypt(encUser, private_key).decode()
    password = rsa.decrypt(encPass, private_key).decode()

    return user, password

def upload_files_to_SFTP(date):
    
    myUsername, myPassword = "TPCMSite210_FTP" , "Y@u3@0xrxL5%bv2C"
    myHostname = 'uploads.callminer.net'

    filepath="/Indra_cuentas_digitales"

    date= date.replace("/","-")

    sftp=connect_to_callminer_sftp(myHostname, myUsername, myPassword)
    download_file_path_xml=os.getcwd() +f"\\Download folder\\{date}\\XML {date}"
    #download_file_path_wavs=os.getcwd()+f"\\Download folder\\{date}"
    #download_file_path_wavs=f"//teleperformance.co/tpshares/CampaingsShares/TPCO/Indra Energy/CALLMINER/{date}"
    download_file_path_wavs=f"Call Miner/{date}"

    upload_mp3_to_sftp_callminer(filepath, sftp, download_file_path_wavs, date,0)
    upload_xml_to_sftp_callminer(filepath, sftp, download_file_path_xml, date,1)

    time.sleep(60)
    

# %%
def convert_secs(time_string):
    if len(time_string) in [4,5]:
        date_time = datetime.strptime(time_string, "%M:%S")
        a_timedelta = date_time - datetime(1900, 1, 1)
        seconds = a_timedelta.total_seconds()
        return seconds
    elif len(time_string) in [6,7,8] :
        date_time = datetime.strptime(time_string, "%H:%M:%S")
        a_timedelta = date_time - datetime(1900, 1, 1)
        seconds = a_timedelta.total_seconds()
        return (seconds)
    else:
        return("Unable to convert")

# %%

def final_metadata_creation(date):

    #sftp_metadata=pd.read_excel(f"//teleperformance.co/tpshares/CampaingsShares/TPCO/Indra Energy/CALLMINER/{date}/{date}.xlsx")
    sftp_metadata=pd.read_excel(f"Call Miner/{date}/{date}.xlsx")
    livevox_txt = glob.glob(f'Download folder\\{date}\\*Click2Power*.txt')
    lvx_metadata=pd.read_csv(livevox_txt[0])
    #sftp_metadata["Duration_secs"]=sftp_metadata["Duration"].apply(lambda x: convert_secs(x))
    sftp_digital=sftp_metadata[(sftp_metadata["Channel"]=="Digital")&(sftp_metadata["Duration"]>=60)]
    final_metadata=sftp_digital.merge(lvx_metadata, left_on="Session2", right_on="Session ID", how="left")
    final_metadata["Transaction ID"]=final_metadata["Transaction ID"].apply(lambda x: str(x).split(".")[0])
    final_metadata["Phone Dialed"]=final_metadata["Phone Dialed"].apply(lambda x: str(x).split(".")[0])
    final_metadata["Service ID"]=final_metadata["Service ID"].apply(lambda x: str(x).split(".")[0])
    final_metadata["Session ID"]=final_metadata["Session ID"].str.replace("@","_").str.replace(".","_")
    print(final_metadata.shape)

    return final_metadata


# %%
def convert_row(row):
    return """<?xml version="1.0"?>
<Recording>
    <Data>
       <AudioFileName>%s</AudioFileName>
        <RecorderID>%s</RecorderID>
        <TransactionID>%s</TransactionID>
        <AccountNumber>%s</AccountNumber>
        <AgentID>%s</AgentID>
        <PhoneNumber>%s</PhoneNumber>
        <CallCenterName>%s</CallCenterName>
        <CallCenterID>%s</CallCenterID>
        <CallDirection>%s</CallDirection>
        <CallDuration>%s</CallDuration>
        <CallEndtime>%s</CallEndtime>
        <CallStartTime>%s</CallStartTime>
        <AgentResult>%s</AgentResult>
        <CallResult>%s</CallResult>
        <ResultLivevox>%s</ResultLivevox>
        <AgentDesktopOutcome>%s</AgentDesktopOutcome>
        <CustomOutcome>%s</CustomOutcome>
        <Campaign>%s</Campaign>
        <ServiceName>%s</ServiceName>
        <SkillID>%s</SkillID>
    </Data>
</Recording>""" % (row["Filename_x"], 
                    row["Session ID"], 
                    row["Transaction ID"],
                    row["Account Number"], 
                    row["Agent Logon Id"], 
                    row["Phone Dialed"], 
                    row["Call Center Name"],
                    row["Call Center_ID"], 
                    row["Transaction Type"], 
                    row["Call Duration"], 
                    row["Call End Time"], 
                    row["Time"], 
                    row["AgentResult"], 
                    row["CallResult"], 
                    row["Livevox Result"],
                    row["Agent Desktop Outcome"],
                    row["custom outcome 1"],
                    row["Campaign"],
                    row["Service Name"],
                    row["Service ID"])

def create_XML(date,metadata):
    #ruta_csv = os.getcwd()+f"\\Download folder\\{date}\\metadata\\{date}_30%.csv"

    #metadata=pd.read_csv(ruta_csv)
    
    #metadata["System Phone Number"]= metadata["System Phone Number"].astype(str)
    
    #fakedate= datetime.strftime(datetime.now()-timedelta(1), '%m/%d/%Y')

    #metadata["Call Date"]=date.replace("-","/")

    #metadata["description"]=metadata["description"].fillna("N/A")
    
    try: 
        #os.mkdir(os.getcwd()+f"\\Download folder\\{date}")
        os.mkdir(os.getcwd()+f"\\Download folder\\{date}\\XML {date}")
    except:
        pass
    ruta_salida = os.getcwd()+f"\\Download folder\\{date}\\XML {date}"

    for index,row in metadata.iterrows():
        #file_name = get_filename(row[1])

        file_name=row["Filename_x"]
        file_name=str(file_name).replace(".mp3","")
        #print(file_name,",",row[1])
        with open(f"{ruta_salida}\\{file_name}.xml", "w") as file_xml: 
            file_xml.write(convert_row(row))

# %%
#Read this path
#\\teleperformance.co\tpshares\CampaingsShares\TPCO\Indra Energy\CALLMINER

# %%

#initial settings
def setting_selenium_options(download_file_path:str)->webdriver.ChromeOptions:
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-extensions")
    options.add_argument("--proxy-server='direct://'")
    options.add_argument("--proxy-bypass-list=*")
    options.add_argument("--start-maximized")
    options.add_argument('--disable-gpu')
    #options.add_argument('--headless')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-running-insecure-content')
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
    options.add_argument(f'user-agent={user_agent}')
    prefs = {
        "download.default_directory":download_file_path
        #"download.prompt_for_download": False,
        #"download.directory_upgrade": True
        }

    options.add_experimental_option('prefs', prefs)
    return options


# %%
def Login(driver,client_code, user, passw):
    driver.get('chrome://settings/')
    #driver.execute_script('chrome.settingsPrivate.setDefaultZoom(0.8);')
    driver.get(f'https://portal.na6.livevox.com/Teleperformance_Col#review/1')
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="clientCode"]'))).send_keys(client_code)
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="username"]'))).send_keys(user)
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="password"]'))).send_keys(passw)
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="loginBtn"]'))).click()
    time.sleep(3)
    driver.get(f'https://portal.na6.livevox.com/Teleperformance_Col#review/1')



# %%

def export_report_livevox(date_from, date_to, driver, call_center):

    #WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="react-tabs-1"]/div/ul/li[3]/span[2]/span[2]'))).click()

    #WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="react-tabs-1"]/div/ul/li[3]/ul/li[1]/span[2]/span[2]'))).click()
    
    try: 
        WebDriverWait(driver, 300).until(EC.frame_to_be_available_and_switch_to_it((By.XPATH,'//*[@id="lvshell__iframe"]')))
    except:
        pass

    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="search-panel__start-date"]'))).clear()
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="search-panel__start-date"]'))).send_keys(date_from)

    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="search-panel__end-date"]'))).clear()
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="search-panel__end-date"]'))).send_keys(date_to)

    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="callcenter_combo"]'))).send_keys(call_center)

    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="report_format_combo"]'))).send_keys("CDR_11_Invalid_Not_Made")

    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="search-panel__generate-report-btn"]'))).click()

    time.sleep(10)

# %%


# %%

def full_download_livevox(date, call_center):
    date_to=date.replace("-","/")
    date_from=date.replace("-","/")

    download_file_path=os.getcwd()+f"\\Download folder\\{date}"
    print(download_file_path)
    setting_selenium_options(download_file_path)
    options = setting_selenium_options(download_file_path = download_file_path)
    driver = webdriver.Chrome("chromedriver.exe", options=options)

    Login(driver, "Teleperformance_Col","bustossanchez.9","Password001**")
    
    export_report_livevox(date_from, date_to, driver, call_center)

    
        

# %%
def full_ingestion_digital(date):
    date=date.replace("/","-")
    full_download_livevox(date, "Click2Power")
    final_metadata=final_metadata_creation(date)
    create_XML(date,final_metadata)
    upload_files_to_SFTP(date)

# %%
#use date as day-month-year

if __name__=="__main__":
    full_ingestion_digital(date)

# %%



