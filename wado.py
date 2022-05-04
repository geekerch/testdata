import requests_toolbelt as tb
from io import BytesIO
import requests
import pydicom
import numpy as np
import png
import xml.etree.ElementTree as ET
import xmltodict, json

#url = 'http://10.221.252.156/WadoRS/WadoRS.svc/bipacsFIR/studies/1.3.6.1.4.1.11157.2020.1.13.11.17.45.236.169268'

def get_wado(url):
    headers = {}
    client = requests.session()
    response = client.get(url, headers=headers) #, verify=False)
    print(response)
    mpd = tb.MultipartDecoder.from_response(response)
    i = 0
    for part in mpd.parts:
        # Note that the headers are returned as binary!
#         ds = pydicom.dcmread(BytesIO(part.content))
#         print(dcm.PatientName)
#         print(dcm.SOPInstanceUID)
      #  print(part.content)
        obj = xmltodict.parse(part.content)
       # print(json.dumps(obj))
        print('-------------------------')
        is_cr = False
        retrieve_url = ""
        for dicom_info in obj["NativeDicomModel"]["DicomAttribute"] :
            if dicom_info['@keyword'] == 'ModalitiesInStudy':
                if dicom_info['Value']['#text'] == 'CR':
                    is_cr = True
            elif dicom_info['@keyword'] == 'RetrieveURL':
                retrieve_url = dicom_info['Value']['#text']

        if is_cr:
            print("retrieve_url: " + retrieve_url)
            parser_dicom(retrieve_url, i)
            i += 1
def parser_dicom(url, i):
    headers = {}
    client = requests.session()
    response = client.get(url, headers=headers) #, verify=False)
    print(response)
    mpd = tb.MultipartDecoder.from_response(response)
    for part in mpd.parts:
        # Note that the headers are returned as binary!
        ds = pydicom.dcmread(BytesIO(part.content))
        print(dcm.PatientName)
        print(dcm.SOPInstanceUID)
        shape = ds.pixel_array.shape
            
        # Convert to float to avoid overflow or underflow losses.
        image_2d = ds.pixel_array.astype(float)
        print(ds.PhotometricInterpretation)
        # Rescaling grey scale between 0-255
        if ds.PhotometricInterpretation == 'MONOCHROME1' :
            image_2d_scaled = np.maximum(image_2d, 0) / image_2d.max() * 255.0
        else :
            image_2d_scaled = (1 - (np.maximum(image_2d, 0) / image_2d.max())) * 255.0
        image_2d_scaled = np.maximum(image_2d, 0) / image_2d.max() * 255.0
      
            # Convert to uint
        image_2d_scaled = np.uint8(image_2d_scaled)

        # Write the PNG file
  
        with open(str(i) +'.png', 'wb') as png_file:
            w = png.Writer(shape[1], shape[0], greyscale=True)
            w.write(png_file, image_2d_scaled)
            print(i)
            i += 1
            
url = 'http://10.221.252.156/QidoRS/QidoRS.svc/bipacsFIR/studies?PatientID=fe0e7c09-207e-49a1-9d58-c0858e7eb26a'

get_wado(url)
  
