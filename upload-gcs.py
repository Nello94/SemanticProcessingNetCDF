from google.cloud import storage

import os
import glob

from os import listdir
from os.path import isfile, join



# Per caricare le credenziali generate da gcp in formato json
storage_client = storage.Client.from_service_account_json("google-credentials.json")

bucketName = 'bucket-nc'

bucket = storage_client.get_bucket(bucketName)

# Path contenente le varie cartelle relative ai diversi file netCDF
localFolder = 'data/'

# Cartella contenente i file (compresedi sub-directories) da caricare
bucketFolder = localFolder + 'ww33_d01_20200329Z1200/'


count = 0
for r, d, f in os.walk(bucketFolder):
    for file in f:
        print ("Caricamento del file: " + str(os.path.join(r, file)))
        blob = bucket.blob(os.path.join(r, file))
        blob.upload_from_filename(os.path.join(r, file))
        count = count+1

print ("Numero di file .nc4 caricati: " + str(count))