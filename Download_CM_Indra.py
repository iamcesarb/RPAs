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

def merge_full_data_from_callminer_with_hold(date, date_folder, download_file_path):

    #leer tabla completa de full_data_from_callminer_with_hold
    full_data_from_callminer_with_hold=pd.read_excel(os.getcwd()+f"\\Data\\full_data_from_callminer_with_hold.xlsx")
    #leer metadata_sample del dia
    new_metadata_sample=pd.read_csv(os.getcwd()+f"\\Download folder\\{date_folder}\\metadata-sample{date_folder}.csv")
    #leer data descargada de Callminer del dia
    files=os.listdir(download_file_path)
    callminer_day=pd.read_csv(os.getcwd()+f"\\Download folder\\Downloads Callminer\\{date_folder}\\"+files[0])
    #merge callminer_day and new_metadata_sample
    new_metadata_sample=new_metadata_sample[["Contact ID","Routing","Active","Hold","Outbound","Transfer","Callback","Post Queue","Script Name","Year","Month","Day","Hour"]]
    new_full_data_from_callminer_with_hold=pd.merge(callminer_day, new_metadata_sample, how="left", left_on="Recorder ID",right_on="Contact ID")
    
    
    #rename columns 
    new_full_data_from_callminer_with_hold=new_full_data_from_callminer_with_hold.rename(columns={"Skill": "Skill_x", "Date/Time": "DT","Duration":"Duration_x","Tags":"Tags_x","Transfer":"Transfer Calls"})
    new_full_data_from_callminer_with_hold=new_full_data_from_callminer_with_hold.drop(["Contact ID"], axis=1)
    new_full_data_from_callminer_with_hold=new_full_data_from_callminer_with_hold.drop_duplicates(subset=["Recorder ID"])

    new_full_data_from_callminer_with_hold.to_excel(os.getcwd()+f"\\Download folder\\{date_folder}\\new_data_from_callminer_with_hold_{date_folder}.xlsx",index=False)
    
    #concat df
    full_data_from_callminer_with_hold=pd.concat([full_data_from_callminer_with_hold, new_full_data_from_callminer_with_hold])

    full_data_from_callminer_with_hold.drop_duplicates(subset=["Recorder ID"], inplace=True)
    full_data_from_callminer_with_hold.dropna(subset=["Month"], inplace=True)
    full_data_from_callminer_with_hold.to_excel(os.getcwd()+f"\\Data\\full_data_from_callminer_with_hold.xlsx", index=False)







# %%
#Lists of fields to download

#dictALG["ALG"]["Brand"]




# %%
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
    account=210

    
    list_scores={"Energy":["New Gathering Information","New Opening","New PTP Score","New Qualifying","Rebuttals"],
                "ALG":[],
                "ALG_2":[],
                "Doordash":[],
                "Vroom":[],
                "Doordash_v2":[] ,
                "Indra":[]  
                    }

    list_metadata={
        "Energy":["Account Number", "Agent ID", "Call Direction", "Call Start Time", "Date/Time", "Duration", 
            "Phone Number", "Result", "Service Name", "State", "Word Count"],
        "ALG":["Agent","Agitation","Average Confidence", "Date/Time","Disposition Result","Duration",
            "Has Negative Sentiment","Has Positive Sentiment","Longest Silence","Overall Sentiment","Percent Silence",
            "Real Date","Recorder ID","Skill","SourceDocumentTypeName","Tempo","Silence Time","Word Count"],
        "ALG_2":["Eureka ID","Recorder ID","Date/Time","Agent","Location","SourceID","Skill Name","Campaign Name"],
        
        "Doordash":['Eureka ID', 'Hits', 'Agent Name', 'Channel', 'Contact Duration', 'Customer Phone Number', 'Disconnect Time',
                'Queue', 'Real date', 'Routing Profile', 'System Phone Number', 'Agitation', 'ANI',
                'Date/Time', 'Recorder ID', 'Average Confidence', 'Customer ID', 'Department', 'Direction',
                'Percent Silence', 'Hold Time', 'Silence Time', 'Duration'],
        "Vroom":["Agent","Agent Group","AGNT","ANI","Average Confidence","CallerID","CallType","Campaing","Date/Time",
                "DISPOSITION","Duration","LastDisposition","Longest Silence","Percent Silence","Recorder ID",
                "Silence Time","Skill"],
        "Doordash_v2":["Agent Name","Average Confidence","Contact Duration","Customer Phone Number","Duration","Has Negative Sentiment",
                "Has Positive Sentiment","Percent Silence","Queue",
                "Real date","Recorder ID","Routing Profile","Silence Time","System Phone Number"],
        "Indra":["Account Number","Agent ID","Call Center ID","Call Center Name","Call Direction","Call Duration Indra","Call End time",
                        "Call Start Time","Phone Number","Result","Service Name","Skill ID","Source","SourceDocumentTypeName","State","Agitation",
                        "Recorder ID","Average Confidence","Direction","Percent Silence","Longest Silence","Silence Time","Duration"]
            }

    list_categories={
        "Energy":["Alert-DNC Rebuttal","Alternate phone number","No interaction","No interaction-Average confidence",
                            "Silence Block","Indra rewards","Rebuttal-Customer already has a provider","Rebuttal-Customer has bad experience",
                            "Rebuttal-Customer is busy","Rebuttal-Customer is not interested","Rebuttal-Dialed number is wrong",
                            "Rebuttal-Does not have a bill handy", "Rebuttal-Early termination Fee","Rebuttal-Not authorized to make decisions",
                            "Rebuttal-Price or More benefits","Rebuttal-Scam call","Refusal-Customer already has a provider","Refusal-Customer does not understand",
                            "Refusal-Customer has bad experience","Refusal-Customer is busy","Refusal-Customer is mentally disabled","Refusal-Customer is not interested",
                            "Refusal-Dialed number is wrong","Refusal-Does not have a bill handy","Refusal-Early termination fee","Refusal-Not authorized to make decisions",
                            "Refusal-Price or More benefits","Refusal-Scam call","Mega energy rewards"],

                    

                    "ALG":{    
                            "Brand":["Charging extra fees - Voucher inquiries","Company is a scam fraud",
                                    "Competitors comparison","Didn`t respond email",
                                    "Do not comply with what they mention on website",
                                    "Do not get answer of a problem","Hotel Dissatisfaction","No availability of dates or hotels",
                                    "Refunds and credits","Services are cancelled without notification"],
                            "Channel":["App or Website","Communication Failures","Communication is difficult"],
                            "Closing":["Booking Recap","Call transfer","Standard closing"],
                            "Customer Education":["Features & benefits of the services"],
                            "Data verification":["Repeat credit card informatio","Verify the caller"],
                            "Dissatisfaction":["Dissatisfaction level 1","Dissatisfaction level 2","Dissatisfaction level 3"],
                            "Hold time parameters":["Dead air","Waiting time"],
                            "Informative":["Calls Disconnected","Cancellation fee","Escalation"],
                            "Legal":["Describes ALG or employees in a derogatory manner","Discloses TP`s or ALG`s confidential information"],
                            "LOB":["ALG Calls","AMR Calls","DT Calls"],
                            "Opening":["Customer service","Greeting","Pleasure to serve"],
                            "Outliers":["Excessive silence","Non Interaction","Outbound Calls","Overtalk","Remove Mono Interaction","Spanish calls",
                                    "Voicemail","Wrong number"],
                            "Ownership folder":["Ownership"],
                            "Positive experiences":["Avoid blaming customer","Empathy","Respect Cat"],
                            "Provide resolution":["Provide resolution","Attempt to overcome objections","Blund cost of travel protection",
                                    "Cross sell and up sell"],
                            "TCD":["Adding a passenger Cat","Cancellation Cat","Change first-last name","Change Hotel Room Cat","Change travel dates Cat",
                                    "Changing flights","Changing hotel Cat","Dropping Passanger Cat","New Reservation Cat","Payment Cat","Quote"],
                            "Understandable and consistent language":["Auditory Understandability","Contextual Understandability"]},
                    "Doordash":{
                                'Auto-Fail':["COVID-19 Case Handling",'HSL Case Handling','Inappropriate Language','Private Information'],
                                'Brand':["Address issues",'Availability dashers Folder','Disadvantages type of payment','Food Safety folder', 
                                        'Issue with a Dasher', 'Item out of Stock', 'Missing or wrong items', 'Order not delivered folder',
                                        'Order taking longer', 'Promo code folder', 'Refund and Credits folder'],
                                "Channel":['APP`s present inconveniences', 'Disagreement when contacting the line', 'Noisy environment'],
                                "Critical Error":['Follow-ups and Escalations','Professionalism'],
                                'Dissatisfaction Level 1':['Dissatisfaction level 1'],
                                'Dissatisfaction Level 2':['Dissatisfaction level 2'],
                                'Dissatisfaction Level 3':['Dissatisfaction level 3'],
                                'Impersonation':['Metadata'],
                                'Informative':['Not interaction','transfer call folder'], 
                                'Security Riders':['Security Riders Folder'],
                                'Soft Skills':['Clarity','Customer Attentiveness','Customer service','Empathy SS',"Excessive hold time",'Issue Type Comprehension',
                                                'Pacing Efficiency','Pleasure  to serve','Willingness to Assist'],
                                "TCD":['Account Settings','Adjust Tip Request','ATO Reported','C&R Compensation Denied','Cancel Order Request',
                                    'Company Inquiry','DashPass Subscription','Edit Order Cart or Special Instructions','Gift card Questions',
                                    'How to Place an Order', 'Missing or incorrect items', 'Reactivation Request', 'Unauthorized charge',
                                    'Where is my order', 'Why was my order cancelled'],
                                "Understandability":['Auditory Understandability','Contextual Understandability','Speech Understandability']

                                },

                    "Vroom":{
                            "ARP_":["ARP Already bought_","ARP Check the car in person_","ARP Condition_","ARP Extenssive Process_","ARP Lacks Interest_","ARP Shipping Fee_","ARP Test Drive_","ARP Wants better price_","ARP Wants to cancel_","ARP Will think_"],
                            "Deposit_":["Deposit money_"],
                            "Digging Deep_":["Current Vehicle_", "Driving Habits_", "Features_", "Finances_","Specific Car_"],
                            "Dissatisfaction_folder": ["Dissatisfaction_"],
                            "Do not contact_":["Dont Contact Anymore_"],
                            "Elegible Calls_":["Eligible Calls_"],
                            "Issue Types_":["Customer issues and concerns_"],
                            "Objections_":["Already bought_","Check the car in person_","Condition_","Extenssive Process_","Lack of Interest_","Shipping Fee_","Test Drive_","Wants Better Price_","Wants to cancel_","Will think_"],
                            "Outliers_":["Mono calls_","Quality call_","Silence_","Voicemail_","Wrong Number_"],
                            "Sales Deposit_":["Ecomms_","Sales During Call_"],
                            "Sales Vroom Protect_":["No reasons Vroom Protect Sales_","Sales - Guaranteed Asset Protection_","Sales - Multi Coverage Protection_","Sales - Vroom Finance_","Sales - Vroom Service Contract_"],
                            "Sales_":["Customer will confirm later_","Ensuring by congrats_","Ensuring by contract department_"],
                            "Soft Skills_":["Emphaty_","Willingness to assit_"],
                            "Type of welcome_":["Following calls_","Website - First Contact_"],
                            "Vroom Protect_":["Guaranteed asset protection_","Multi-Coverage Protection_","Vehicle Service Contract_","Vroom Finance_"]

                            },
                    "Doordash_v2":{
                        'Auto-Fail':["COVID-19 Case Handling",'HSL Case Handling','Inappropriate Language','Private Information'],
                        #'Brand':["Address issues",'Availability dashers Folder','Disadvantages type of payment','Food Safety folder', 
                        #        'Issue with a Dasher', 'Item out of Stock', 'Missing or wrong items', 'Order not delivered folder',
                        #        'Order taking longer', 'Promo code folder', 'Refund and Credits folder'],
                        "Channel":['APP`s present inconveniences', 'Disagreement when contacting the line', 'Noisy environment'],
                        "Critical Error":['Follow-ups and Escalations','Professionalism'],
                        "CJ Categories":["CJ Reiteratividad"],
                        'Dissatisfaction Level 1':['Dissatisfaction level 1'],
                        'Dissatisfaction Level 2':['Dissatisfaction level 2'],
                        'Dissatisfaction Level 3':['Dissatisfaction level 3'],
                        #'Impersonation':['Metadata'],
                        'Informative':['Not interaction',"Silence"], 
                        'Security Riders':['Security Riders Folder'],
                        "Sentiment DD":["Negative Sentiment","Negative Sentiment - First 30% of Call","Negative Sentiment - Last 30% of Call",
                                        "Negative Sentiment Middle call","Positive Sentiment","Positive Sentiment - First 30% of Call",
                                        "Positive Sentiment - Last 30% of Call","Positive Sentiment - Middle of Call"],
                        'Soft Skills':['Clarity','Customer Attentiveness','Customer service',"Dead air",'Empathy SS',"Hold update",'Issue Type Comprehension',
                                        "Outage",'Pleasure  to serve','Willingness to Assist'],
                        "TCDDD":["App Feedback and Troubleshooting","Availability dashers folder","C and R Compensation Denied","Cancel My Order",
                                "Change Delivery Address Date Time","Delivery Too Late","Feedback about Dasher Merchant","Food Quality Issue",
                                "Help Placing an Order","Missing or Incorrect Items","Never Delivered","Order Cart Adjustment","Order Status Inquiries",
                                "Refund Credit Inquiry","Report Unauthorized Charges","Update Account Information","Why was my Order Cancelled",
                                "Wrong Order Received"],
                        "Understandability":['Auditory Understandability','Contextual Understandability','Speech Understandability']

                            },

                    "Indra":{
                        "AutoFail":["Agent does not retake the lead","Agent is not doing active listening","Dead air",
                                    "Displays hesitation to keep the call going","Incorrect tone of voice","Knowledge gaps",
                                    "Language not supported by the campagin","Respect","Slaming evidence","Talking over customer","Transfer to supervisor"],
                        "Beneficios":["Price protection","Rewards program"],
                        "Branding and Introduction":["Branding - Agent name","Branding - Company name","Correct expectations",
                                    "Recording disclosure","Statements to approach customer"],
                        "Closing":["Disclosure","Transfer Language"],
                        "CX-Attentiveness":["Brand apologies", "Building interaction", "Confidence",
                                            "Detection","Focuses the call`s objective","Offering further assitance",
                                            "Personalization","Support customer`s needs"],
                        "CX-Efforts":["Makes to repeat information"],
                        "CX-Non negotiable":["Agent Profanity","Hang up in previous call"],
                        "CX-Process":["Customer Identity Verification","Guides with areas in charge",
                                    "Slowness of tools","Tool malfunctions","Working deadline for response-resolution"],
                        "CX-Service":["Able to respond to various situations","Look for all alternatives",
                                    "Need to reconduct the call"],
                        "CX-WAHA Attributes":["Background sounds","Connection inconveniences",
                                            "Voice pitch volume"], 
                        "Objection handling":['Refusal-Customer already has a provider','Refusal-Customer is busy',
                                    'Refusal-Customer is not interested','Refusal-Dialed number is wrong',"Refusal-Moving-selling home",'Refusal-Not authorized to make decisions',
                                    'Refusal-Price or More benefits','Refusal-Send info over','Refusal-Speak to spouse|I need to think about it']  ,                             
                        "Outcomes":["Do Not Contact", "Voicemail", "Wrong Number"],
                        "Qualifying":["Bill information","Confirm utility company","Find DM - FFQ",
                                        "Get copy of the bill","Government assistance","Looking for lead","Payment status"],
                        "Rebuttals handling":['Rebuttal-Customer already has a provider',
                                        'Rebuttal-Customer is busy',"Rebuttal-Customer is not interested",
                                        'Rebuttal-Dialed number is wrong', 'Rebuttal-Moving-selling home',
                                        "Rebuttal-Not authorized to make decisions",'Rebuttal-Price or More benefits',
                                        'Rebuttal-Send info over','Rebuttal-Speak to spouse|I need to think about it']
                                    
                                },

                        "ALG_2": { 
                            "ALG Complaint Analysis": 
                                ["Extreme", 
                                "Mild", 
                                "Moderate"],
                            "ALG Dissatisfaction Breakdown":[
                                "ALG Agent",
                                "ALG Payment Related",
                                "ALG Service",
                                "ALG Technology Related"
                            ],
                            " ALG General Customer Experience":[
                                "Dissatisfaction", 
                                "Escalation", 
                                "Neutral", 
                                "Positive Sentiments", 
                                "Repeat Contact", 
                                "Transfer" 
                            ],
                            "ALG TCD":[
                                "ALG All TCD"
                            ]

                
                        }




                            
    }
    list_metadata=list_metadata["Indra"]
    list_scores=list_scores["Indra"]
    list_Categories=create_list_categories(list_categories, "Indra")

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
    usuario_CM, contrasenia_CM = "cesar.bustossanchez@teleperformance.com","Password001**"
    Login(driver, account, usuario_CM, contrasenia_CM)

    #Select days
    Select_dates(driver,From,To)
    time.sleep(5)

    #Search and download
    Search_select_download(driver,list_metadata,list_scores, list_Categories)
    
    time.sleep(5)
    print("Download complete")
    #merge_full_data_from_callminer_with_hold(To, date_folder, download_file_path)
    #print("Done merged full_data_from_callminer_with_hold")
    driver.quit()
    rename_SearchReport(date_folder)
    time.sleep(5)
    


# %%

if __name__=="__main__":
    main(From, To)
    
    


