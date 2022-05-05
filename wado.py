import requests_toolbelt as tb
from io import BytesIO
import requests
import pydicom
import numpy as np
import png
import xml.etree.ElementTree as ET
import xmltodict, json
import pymongo
import urllib.parse
from uuid import uuid4
import datetime
import psycopg2
from minio import Minio
import os
from datetime import datetime

# global parameter
#pg

#mongo
host = "10.121.12.154"
port = 27017
user_name = "a94c3150-a781-417f-a223-73c615f19c73"
pass_word = "PdozTTuLRg9YRlwb0bp40Bxs"  
db_name = "2221088b-bcce-461c-94cc-682d00ceee50"  
#pacs
pacs_url = 'http://10.221.252.156/QidoRS/QidoRS.svc/bipacsFIR/studies'
#blob
blob_prefix = 'https://blob.wise-paas.vghtpe.gov.tw/dashboard/'
#url = 'http://10.221.252.156/WadoRS/WadoRS.svc/bipacsFIR/studies/1.3.6.1.4.1.11157.2020.1.13.11.17.45.236.169268'

client = pymongo.MongoClient(host, port)
client.admin.authenticate(user_name, pass_word, mechanism = 'SCRAM-SHA-1', source=db_name)
# mongo db 連線
mydb = client[db_name]

hps = mydb.hospital_patient

# pg連線
conn = psycopg2.connect(database="SAS_ESP", user="sasesp", password="sasesp", host="10.221.252.50", port="5432")
cur = conn.cursor()

minio_client = Minio(
    "10.121.12.154:8999",
    access_key="bT9j6csOcpZfu52KleaahIiq",
    secret_key="j9Uiv28XM7LIxdv9AXIFQ4N7",
    secure=False,
)

def get_patients():
    # 從mongo取得所有病歷號
    patients = hps.find({})
    patient_ids = {}
    for patient in patients:
        sql = "select \"CHARTID\", \"UUID\" from \"BDC_BASE\".\"ID\" where \"CHARTID\" = \'" + patient['patient_id'] + "\'"
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) > 0 :
            patient_ids[rows[0][1]] = patient['patient_id']
    return patient_ids
	
def get_wado(url):
    headers = {}
    client = requests.session()
    response = client.get(url, headers=headers) #, verify=False)
#     print(response)
    mpd = tb.MultipartDecoder.from_response(response)
    i = 0
    retrieve_urls = []
    for part in mpd.parts:
        # xml to json 比較好處理
        obj = xmltodict.parse(part.content)
       
        is_cr = False
        retrieve_url = ""
        for dicom_info in obj["NativeDicomModel"]["DicomAttribute"] :
            if dicom_info['@keyword'] == 'ModalitiesInStudy':
                #cr是x光，非x光的不拿
                if dicom_info['Value']['#text'] == 'CR':
                    is_cr = True
            elif dicom_info['@keyword'] == 'RetrieveURL':
                retrieve_url = dicom_info['Value']['#text']

        if is_cr:
#             print("retrieve_url: " + retrieve_url)
#             parser_dicom(retrieve_url, i)
            retrieve_urls.append(retrieve_url)
            i += 1
    return retrieve_urls
            
def parser_dicom(url, patient_id):
    headers = {}
    client = requests.session()
    response = client.get(url, headers=headers) 
#     print(response)
    mpd = tb.MultipartDecoder.from_response(response)
    for part in mpd.parts:
        # Note that the headers are returned as binary!
        ds = pydicom.dcmread(BytesIO(part.content))
        file_name = patient_id + '_' + ds.StudyDate + '_' + ds.StudyTime + '.png'
        #已存在就直接下一張
        if is_existed(blob_prefix + file_name) :
            continue
        shape = ds.pixel_array.shape
            
        # Convert to float to avoid overflow or underflow losses.
        image_2d = ds.pixel_array.astype(float)
#         print(ds.PhotometricInterpretation)
        # Rescaling grey scale between 0-255
        if ds.PhotometricInterpretation == 'MONOCHROME2' :
            image_2d_scaled = np.maximum(image_2d, 0) / image_2d.max() * 255.0
        else :
            image_2d_scaled = (1 - (np.maximum(image_2d, 0) / image_2d.max())) * 255.0
        # Convert to uint
        image_2d_scaled = np.uint8(image_2d_scaled)
		
def is_existed(url):
    xrays = mydb.xray
    patients = xrays.find_one({"xray_url": url})
    if patients is None:
        return False
    else:
        return True
    
def insert_record(patient_id, study_date, xray_url):
    dt = datetime.strptime(study_date, "%Y%m%d")
    xray = {"_id": str(uuid4()),
         "patient_id": patient_id,
         "xray_url": xray_url,
         "xray_ts": dt}
    xrays = mydb.xray
    xrays.insert_one(xray)
        
        # Write the PNG file
        with open(file_name, 'wb') as png_file:
            w = png.Writer(shape[1], shape[0], greyscale=True)
            w.write(png_file, image_2d_scaled)
        minio_client.fput_object("dashboard", file_name, file_name,)
        os.remove(file_name)
        #write to db
        insert_record(patient_id, ds.StudyDate, blob_prefix + file_name)
        #只取第一張
        break

print("start")
patients = get_patients()
# print(patients)

for patient in patients: 
    retrieve_urls = get_wado(pacs_url + "?PatientID=" + patient)
#     print(patients[patient])
    for retrieve_url in retrieve_urls:
        parser_dicom(retrieve_url, patients[patient])
#     print(retrieve_urls)
