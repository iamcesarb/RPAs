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
#LogIn
#LogIn
def Login(driver,numero, user_CM, passw_CM):
    driver.get('chrome://settings/')
    driver.execute_script('chrome.settingsPrivate.setDefaultZoom(0.8);')
    driver.get(f'https://tpcmsite{numero}.callminer.net/Search/Query')
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="submitButton"]'))).click()
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="Email"]'))).send_keys(user_CM)
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="Password"]'))).send_keys(passw_CM)
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="signInBtn"]'))).click()

# %%
def Select_dates(driver, From, To):

    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="dsplyBtn"]'))).click()
    
    #WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="dateSelection"]/div/ul/li[2]'))).click() #yesterday date

    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@name="txtDateFrom"]'))).clear()

    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@name="txtDateFrom"]'))).send_keys(From)

    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@name="txtDateTo"]'))).clear()

    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@name="txtDateTo"]'))).send_keys(To)

    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="dateSelection"]/div/ul/li[9]'))).click() #click before apply button

    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="applyRangeBtn"]'))).click()
    
    #

# %%
#Search and wait


def summary_data_filter(driver):
    
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="displayConfigPanelCollapsedBtn"]'))).click()
    time.sleep(2)
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="speakerSectionCollapsedBtn"]'))).click()
    time.sleep(2)
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="AttributesCollapsedBtn"]'))).click()
    time.sleep(2)
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="Attributes_AgentGroup"]/span[1]'))).click()
    time.sleep(2)

    elem_hover = driver.find_element(By.XPATH,f'//*[@id="Attributes_AgentGroup_Sales-TPC_includeFilterBtn"]')
    elem_click = driver.find_element(By.XPATH,f'//*[@id="Attributes_AgentGroup_Sales-TPC_includeFilterBtn"]')
    actions = ActionChains(driver)
    actions.move_to_element(elem_hover)
    time.sleep(1)
    actions.click(elem_click)
    time.sleep(1)
    actions.perform()

def what_to_download(driver,list_metadata, list_scores, list_Categories):

    for item in list_metadata:   
        try:
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,f"//span[text()='{item}']"))).click()
        except:
            pass
    #Open all active Scores
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,"/html/body/div[1]/div/div/div/div/form/div[1]/div/div[2]/div[1]/i"))).click()

    for item in list_scores:   
        try:
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,f"//span[text()='{item}']"))).click()
        except:
            pass
    #Select Include Score Indicators y Tags
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,f"//span[text()='Include Score Indicators']"))).click()

    #WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,f"//span[text()='Tags']"))).click()

    #Open all Categories
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'/html/body/div[1]/div/div/div/div/form/div[1]/div/div[3]/div/div[1]/i'))).click()



    for item in list_Categories:
        try:
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,f"//span[text()='{item}']"))).click()
        except:
            pass

    #Include category components
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,f"//span[text()='Include Category Components']"))).click()

    #CLick on Next to download
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,"/html/body/div[1]/div/div/div/div/form/div[3]/button[2]"))).click()

    time.sleep(60)

def Search_select_download(driver,list_metadata,list_scores, list_Categories):
    #summary_data_filter(driver)
    time.sleep(5)
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="searchBtn"]'))).click()
    time.sleep(30)
    
    time.sleep(2)
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="toggleLeftPaneCollapseHeader"]/div/button'))).click()

    #WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="toggleLeftPaneCollapseHeader"]/div/ul/li[7]/span/download-results'))).click()
   
    WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH,'//*[(@id="exportLong") and (@data-language-text="ExportToExcel")]'))).click()  
    
    time.sleep(20)

    #Select metadata to download
    what_to_download(driver, list_metadata,list_scores, list_Categories)


def create_list_categories(list_categories,cuenta):
        
    list_cat=[]
    for key in list_categories[cuenta]:
        for key2 in list_categories[cuenta][key]:
            list_cat.append(key2)

    return list_cat


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

def rename_SearchReport(date_folder):
    download_file_path = os.getcwd() + f'\\Download folder\\Downloads Callminer_v2\\{date_folder}'
    filename=os.listdir(download_file_path)[0]
    old_name=r'{}\{}'.format(download_file_path, filename)
    new_name=f"SearchReport{date_folder}.xlsx"
    final_name =r'{}\{}'.format(download_file_path, new_name)
    os.rename(old_name, final_name)

# %%
#Ejecución del script completo
def main(From, To):
    date_folder=To.replace("/","-")
    account=226
    list_scores={"Energy":["New Gathering Information","New Opening","New PTP Score","New Qualifying","Rebuttals"],
                "ALG":[],
                "Doordash":[],
                "Vroom":[],
                "Doordash_v2":[],
                "Angi":[]}

    list_metadata={
        "Angi":["Agent","Agent Group","ANI","Average Confidence","contact_id","Date","Date/Time","Direction","Disp_Name","Duration",
                "Hold Time","Longest Silence","Percent Silence",
                "Real Direction","Recorder ID","Silence Time","Skill name","skill_no","Tempo","To"]
                    
            }

    list_categories={
                        "Angi":{
                            "Acknowledge Statement 2":["Info - CUS about cancellation","Info - CUS about customer service","Info - CUS about discount",
                                        "Info - CUS about email after booking","Info - CUS about handyman","Info - CUS about happiness guarantee",
                                        "Info - CUS about I can talk to xxxxx","Info - CUS about I can wait","Info - CUS about membership",
                                        "Info - CUS about No booking Windows","Info - CUS about other","Info - CUS about payment secur",
                                        "Info - CUS about pre-priced guarantee","Info - CUS about professionals",
                                        "Info - CUS about re schedule","Info - CUS about security","Info - CUS about urgency"],
                            #"Building Rapport":["Building_rapport"],
                            "Coupon and membership offer":["Coupon and membership"],
                            "Critical Errors":["Language Around Pre-priced Pros and Pro Behavior","Other language","Payment Language"],
                            
                            "Informative":["Call Back Suggestions","Positive Sentiment","Season Upselling","Voicemail conversation with client",
                                    "Voicemail more 30 secs non agent message","Voicemail more than 60 secs"],

                            "Outliers":["Do not contact alert","Not interaction","Spanish calls","Voicemail","Wrong Number"],
                            "Powerful Phrases":["Powerful_Phrases"],

                            "Rebuttals 02":["Cancellation 2","Customer Service 2","Discount 2","Email after booking 2",
                                        "Handyman 2","Happiness Guarantee 2","I can talk to xxxxx 2","I can wait 2","Membership 2",
                                        "No booking Windows 2","Other 2","Payment secur 2","Pre-Priced Guarantee 2","Professionals 2","Re schedule 2",
                                        "Security 2","Urgency2"],

                            "Sales objections":["Can`t give card number","Dont want to pay until speak to pro",
                                    "I don`t have my card handy","I don`t want to provide my own materials","I dont have a card to put on file",
                                    "I dont want to pay ahead of time","I need the service sooner","I need time to think",
                                    "I need to consult before I make a decision","I need to work out scheduling","I want to get other quotes before deciding",
                                    "I want to pay cash only","Need a better price","Someone available","Talk to Pro",
                                    "The customer wants to know who the pro is","This price is too high",
                                    ],
                            "Soft Skills":["Empathy_"],
                            "TCD":["Cleaning_services","General_Handyman","Handyman_service","Installation_Services","Outdoor_Projects",
                            "Renovation or Remodel Services_","Repair_Services"],
                            #-------------------------------------------
                            "Active Listening": ["Recap"],
                            "Closing QA": ["Assumptive Language & Urgency","Call Control","Set a call back"],
                            "Compliance": ["Cross Selling & Up Selling","Legal Terms","Post Booking Script","Recorded line"],
                            "Critical Fail":["Dispo CallBack","Dispo connection issue","Dispo legal terms","Professionalism throughout the call"],
                            "Objection Handling": ["Not Lead by offering a discount", "Reasoning for objections"],
                            "Product Knowledge": ["Negative Phrasing"],
                            "Tone Rapport": ["Build Rapport","Confident tone","Did we greet the call properly","Set the agenda"]
                        }
                        
    }


    
    list_Categories=create_list_categories(list_categories, "Angi")

    list_metadata=list_metadata["Angi"]
    list_scores=list_scores["Angi"]
    try:
        os.mkdir(os.getcwd() + f'\\Download folder\\Downloads Callminer_v2\\{date_folder}') # creo una carpeta cuyo nombre es el dia que corresponde a la descarga de la metadata

    except:
        pass
    
    download_file_path = os.getcwd() + f'\\Download folder\\Downloads Callminer_v2\\{date_folder}'
    setting_selenium_options(download_file_path)
    options = setting_selenium_options(download_file_path = download_file_path)
    driver = webdriver.Chrome("chromedriver.exe", options=options)
    
    #private_key = read_key(r"C:\keys\private_key_DD_CM.txt")
    
    #enc_usuario = Login_CM.user
    #enc_contrasenia = Login_CM.passw

    #credentials = [enc_usuario, enc_contrasenia]

    #usuario_CM, contrasenia_CM = decrypt_credentials(credentials, private_key)
    #usuario_CM, contrasenia_CM = "oscar.fernandeztovar@teleperformance.com","Colombiabogota2022*"
    usuario_CM, contrasenia_CM = "cesar.bustossanchez@teleperformance.com","TPColombia2022**"
    Login(driver, account, usuario_CM, contrasenia_CM)

    #Select days
    Select_dates(driver,From,To)
    time.sleep(5)

    #Search and download
    Search_select_download(driver,list_metadata,list_scores, list_Categories)
    
    time.sleep(5)
    print("Download complete")
    
    driver.quit()
    rename_SearchReport(date_folder)
    time.sleep(5)
    
if __name__=="__main__":
    main(From, To)




