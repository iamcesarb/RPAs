from os.path import join
import pandas as pd
import warnings
import os
warnings.filterwarnings("ignore")



import numpy as np
def sampling_metadata(metadata,sample_size, time_filter):
    #convert time to secs

    metadata["Segment Duration2"]=pd.to_timedelta(metadata["Segment Duration"])

    metadata["secs"]=metadata["Segment Duration2"].dt.total_seconds()

    #filter >30sec
    metadata=metadata[metadata["secs"]>time_filter]

    #sampled_metadata=sampling_metadata(metadata, sample_size)
    print(f"We have {len(metadata)} rows in metadata after filtering")
    
    #sampling 
    columns=["Participant Station"] 
    N=int(len(metadata["Participant Station"])*sample_size)
    
    metadata_sample=metadata.groupby(columns, group_keys=False).apply(lambda x: x.sample(int(np.rint(N*len(x)/len(metadata))))).sample(frac=1).reset_index(drop=True)
    #print(len(metadata_sample["Participant Station"]))
    print(f"We have {len(metadata_sample)} rows in sampled_metadata")
    
    return metadata_sample




def change_wav_name(path2):
    file_list = os.listdir(path2)
    filtered_files = [file for file in file_list if file.endswith(".wav") and file.startswith('_')]
    df = pd.DataFrame(filtered_files, columns = ['first_name'])
    df2_split_names = df['first_name'].str.split('_', n = 4, expand = True)
    

    df['nombre_final'] = df2_split_names[3].astype('str')+'.wav'
    df['nombre_final_2'] = df2_split_names[3].astype('str')+'_duplicado.wav'
    

    
    for x, y in zip(df['first_name'].astype('str'), df['nombre_final'].astype('str')):
        try:
            x = join(path2, x)
            y = join(path2, y)
            os.rename(x, y)
        except:
            for x, y in zip(df['first_name'].astype('str'), df['nombre_final_2'].astype('str')):
                
                x = join(path2, x)
                y = join(path2, y)
                os.rename(x, y)



def change_wav_name(path2):
    file_list = os.listdir(path2)
    filtered_files = [file for file in file_list if file.endswith(".wav") and file.startswith('_')]
    df = pd.DataFrame(filtered_files, columns = ['first_name'])
    df2_split_names = df['first_name'].str.split('_', n = 4, expand = True)
    

    df['nombre_final'] = df2_split_names[3].astype('str')+'.wav'
    df['nombre_final_2'] = df2_split_names[3].astype('str')+'_duplicado.wav'
    df['nombre_final_3'] = df2_split_names[2].astype('str')+'.wav'
    df['nombre_final_4'] = df2_split_names[2].astype('str')+'_duplicado.wav'

    df.to_excel('test.xlsx')
    
    for x, y in zip(df['first_name'].astype('str'), df['nombre_final_3'].astype('str')):
        if x.startswith('_2023'):
            try:
                #print(x)
                x = join(path2, x)
                y = join(path2, y)
                os.rename(x, y)
            except:
                try:
                    for x, y in zip(df['first_name'].astype('str'), df['nombre_final_4'].astype('str')):
                        #print(x,y)
                        x = join(path2, x)
                        y = join(path2, y)
                        os.rename(x, y)
                except:
                    pass

        for x, y in zip(df['first_name'].astype('str'), df['nombre_final'].astype('str')): 
            if x.startswith('_1'):
                try:
                    #print(x)
                    x = join(path2, x)
                    y = join(path2, y)
                    os.rename(x, y)
                except:
                    try:
                        for x, y in zip(df['first_name'].astype('str'), df['nombre_final_2'].astype('str')):
                            #print(x,y)
                            x = join(path2, x)
                            y = join(path2, y)
                            os.rename(x, y)
                    except:
                        pass

import paramiko
import glob
from tqdm import tqdm
import time
from datetime import datetime, timedelta

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


def upload_xml_to_sftp_callminer(filepath, sftp:paramiko,download_file_path_xml:str,yesterday_date_str:str,start):
    # Upload
    
    #sftp.mkdir('/test/' + yesterday_date_str)
    
    #print(filepath)
    ruta = glob.glob(download_file_path_xml + '\\*.xml')
    #print("ruta:",ruta)

    for i in tqdm(range(start,len(ruta))):
        localpath = ruta[i]
        name_file = ruta[i].split('\\')[-1]
        #print(name_file, localpath, filepath)
        sftp.put(localpath,filepath + '/' + name_file, confirm=False)
        #print(filepath + '/' + name_file)
        time.sleep(1)   

def upload_audios_to_sftp_callminer(filepath, sftp:paramiko,download_file_path:str,yesterday_date_str:str,start):
    # Upload
    
    #sftp.mkdir('/test/' + yesterday_date_str)
    #filepath = "/test/" + yesterday_date_str
    
    #print(filepath)
    ruta = glob.glob(download_file_path + '\\*.wav')
    #print("ruta:",ruta)
    #for i in range(1,len(ruta)):
    for i in tqdm(range(start,len(ruta))):   
        localpath = ruta[i]
        #
        # print(localpath)
        name_file = ruta[i].split('\\')[-1]
        #print(name_file)
        sftp.put(localpath,filepath + '/' + name_file, confirm=False)
        time.sleep(1)

def upload_files_to_SFTP(date, path2):
    #print(f"Uploading Audios to SFTP from {date} at the time --->", dt.datetime.now())
    #fechat = datetime.strptime(date,'%Y/%d/%m').date()

    myUsername, myPassword = "TPCMSite243_FTP" , "Kbr2G76ttBb6Jsaf"
    myHostname = 'uploads.callminer.net'

    filepath="/Reclutamiento_spanish/"

    date= date.replace("/","-")

    sftp=connect_to_callminer_sftp(myHostname, myUsername, myPassword)
    #download_file_path_xml=f"//10.151.232.156/Import_xml/EPSON{fechat}"
    download_file_path_wavs=path2
    download_file_path_xml=path2+f"\\XML {date}"
    
    #download_file_path_wavs=f"//10.151.232.156/Import_xml/EPSON2022-12-20"

    upload_audios_to_sftp_callminer(filepath, sftp, download_file_path_wavs, date,0)
    upload_xml_to_sftp_callminer(filepath, sftp, download_file_path_xml, date,0)

    

    print(f"Finish Uploading Audios to SFTP from {date} at the time --->", datetime.now())




    #Cruzar bases para dejar metadata con agentes

HC_reclutamiento=pd.read_excel(r"Data\HC_Reclutamiento.xlsx")
HC_reclutamiento



def metadata_preprocessing(date_folder, fake_date):
    """
        fake_date in format 2023-02-14=YYYY-MM-DD
        date_folder in format 02-14-2023=MM-DD-YYYY
    """
    
    fechat = datetime.strptime(date_folder,'%m-%d-%Y').date()
    day = fechat.strftime('%#d')
    month=fechat.strftime('%B')

    months_dict={"January":"Enero","February":"Febrero","March":"Marzo","April":"Abril","May":"Mayo","June":"Junio",
                "July":"Julio","August":"Agosto","September":"Septiembre","October":"Octubre","November":"Noviembre",
                "December":"Diciembre"}

    month_spa=months_dict[month]
    
    #Depende de como Dani suba el nombre del archivo
    metadata_raw=pd.read_csv(f"Data\\Reclutamiento Daily {month_spa} {day}.csv", encoding = "UTF-16LE")
    
    #metadata_raw=pd.read_csv(f"Data\\{month_spa} {day}.csv", encoding = "UTF-16LE")

    metadata_raw=metadata_raw[metadata_raw["Participant Station"]!=" "]
    metadata_raw["Participant Station"]=metadata_raw["Participant Station"].astype(int)
    metadata_raw=metadata_raw.merge(HC_reclutamiento, right_on="Extensión", left_on="Participant Station", how="inner")
    metadata=metadata_raw.copy()

    metadata["Fecha"]=metadata["Segment Start Time"].str.split(" ").str[0]
    metadata["Fecha"]=pd.to_datetime(metadata["Fecha"], format="%d/%m/%Y")
    metadata["Fecha"]=metadata["Fecha"].dt.date

    #Create keys with phone and date
    metadata["Participant Phone Number"]=metadata["Participant Phone Number"].str.strip()#.astype("float64").astype("int64")
    metadata["Segment Duration"]=metadata["Segment Duration"].str.strip()
    #metadata["date_phone"]= metadata["Fecha"].astype(str)+"_"+metadata["Participant Phone Number"]

    #metadata=metadata[~metadata["Nombre"].isin(["ND",0])]
    metadata=metadata.drop_duplicates(subset="Segment ID")
    #metadata["Segment ID"]=metadata["Segment ID"].astype(float).astype("int64")
    metadata["AudioFileName"]=metadata["Segment ID"].astype(str)+".wav"
    metadata["Participant Station"]=metadata["Participant Station"].astype(int)
    #metadata["UCID"]=metadata["UCID"].astype("int64")
    metadata["Nombre"]=metadata["Nombre"].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    metadata["Supervisor"]=metadata["Supervisor"].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    metadata["Segment Stop Time"]=metadata["Segment Stop Time"].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.strip().str.replace("p. m.","PM").str.replace("a. m.","AM")
    metadata["FakeDate"]=fake_date
    print(metadata.shape)
    return metadata



import os
def convert_row(row):
    return """<?xml version="1.0" encoding="UTF-8"?>
<Recording>
    <Data>
       <AudioFileName>%s</AudioFileName>
        <SegmentID>%s</SegmentID>
        <SegmentStartTime>%s</SegmentStartTime>
        <SegmentStopTime>%s</SegmentStopTime>
        <SegmentDuration>%s</SegmentDuration>
        <UCID>%s</UCID>
        <ParticipantStation>%s</ParticipantStation>
        <Reclutador>%s</Reclutador>
        <Pais>%s</Pais>
        <Supervisor>%s</Supervisor>
        <RealDate>%s</RealDate>
    </Data>
</Recording>""" % (row["AudioFileName"], 
                    row["Segment ID"], 
                    row["FakeDate"],
                    row["Segment Stop Time"], 
                    row["Segment Duration"], 
                    row["UCID"], 
                    row["Participant Station"],
                    row["Usuario de red"], 
                    row["Pais"], 
                    row["Supervisor"],
                    row["Fecha"]
                    )

def create_XML(date,metadata):
    
    try: 
        #os.mkdir(os.getcwd()+f"\\Download folder\\{date}")
        os.mkdir(os.getcwd()+f"\\Data\\Copy of audios in import\\{date}\\XML {date}")
    except:
        pass
    ruta_salida = os.getcwd()+f"\\Data\\Copy of audios in import\\{date}\\XML {date}"

    for index,row in metadata.iterrows():
        #file_name = get_filename(row[1])

        file_name=row["AudioFileName"]
        file_name=str(file_name).replace(".wav","")
        #print(file_name,",",row[1])
        with open(f"{ruta_salida}\\{file_name}.xml", "w") as file_xml: 
            file_xml.write(convert_row(row))



def found_paths_audios(sampled_metadata, month):  #month en formato 06
    
    folders=os.listdir(f"\\\\10.151.232.156\Import_xml\Reclutamiento Col Daily\\2023\\{month}")
    #metadata=metadata_preprocessing(date_folder="2023-01-19", fake_date="2023-02-24")
    sampled_metadata["Segment ID"]=sampled_metadata["Segment ID"].astype(str)
    ids=sampled_metadata["Segment ID"].to_list()

    print(f"length of sampled_metadata {len(ids)}")
    ids_found=[]
    ids_found_paths=[]
    for folder in folders:
        #print(folder)
        files=os.listdir(f"\\\\10.151.232.156\\Import_xml\\Reclutamiento Col Daily\\2023\\{month}\\{folder}")
        
        for file in files:
            for id in ids:
                if id in file:
                    ids_found.append(id)
                    #print(f"id exists in file: {file}\nin day:{folder}")
                    ids_found_paths.append(f"\\\\10.151.232.156\\Import_xml\\Reclutamiento Col Daily\\2023\\{month}\\{folder}\\"+file)
    
    return ids_found_paths



import shutil
def copy_selected_files(ids_found_paths, date_folder):
    print(f"We are moving {len(ids_found_paths)} audio files into {date_folder}")
    path2=os.getcwd()+f"\\Data\\Copy of audios in import\\{date_folder}"
    for file in ids_found_paths:
        file_name=file.split("\\")[-1]
        #try:
        #print(file, path2+'\\'+file_name)
        if file_name.startswith('__'):
            shutil.copy(file, path2+'\\'+''.join(file_name[1:]))
        else:   
            shutil.copy(file, path2+'\\'+file_name)


def main(date_folder, fake_date, month):
    try: 

        os.mkdir(os.getcwd()+f"\\Data\\Copy of audios in import\\{date_folder}")
    except:
        pass
    path2=os.getcwd()+f"\\Data\\Copy of audios in import\\{date_folder}"
    metadata=metadata_preprocessing(date_folder, fake_date)
    sampled_metadata=sampling_metadata(metadata,1,30) #aquí es donde se cambia, el 1 es el 100% y el 30 son los segundos que se filtran
    sampled_metadata.to_csv(f"Data\\Copy of audios in import\\{date_folder}\\sampled_metadata{date_folder}.csv", index=False)

    ids_found_paths=found_paths_audios(sampled_metadata, month)
    copy_selected_files(ids_found_paths,date_folder)
    create_XML(date_folder, sampled_metadata)
    change_wav_name(path2)
    upload_files_to_SFTP(date_folder, path2)



if __name__=="__main__":
    main(date_folder, fake_date, month)