from netCDF4 import Dataset
from os import mkdir, path, remove, makedirs, rmdir
import numpy as np

import glob
from google.cloud import storage

import json
import subprocess
from sys import exit

import shutil

import sys
if not sys.warnoptions:
    import os, warnings
    warnings.simplefilter("ignore", RuntimeWarning) # Change the filter in this process


import logging
import random
import threading
import time

def calc_divisors(num):
    result = []
    for x in range(1, num + 1):
        if num % x == 0:
            result.append(x)
    return result



logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )

class Counter(object):
    def __init__(self, start = 0):
        self.lock = threading.Lock()
        self.value = start
    def increment(self):
        logging.debug('Waiting for a lock')
        self.lock.acquire()
        try:
            logging.debug('Acquired a lock')
            self.value = self.value + 1
        finally:
            logging.debug('Released a lock')
            self.lock.release()

def worker(c, dataset_name, variable_name, range_lat_start, range_lat_end, range_lon_start, range_lon_end):
    
    """
        START
    """
    c.increment()
    logging.debug('Starting thread')
    

        
    bucketName = 'bucket-nc'

    base_path = "download/"

    # Dataset
    dataset_name = "ww33_d01_20200329Z1200"

    final_path = base_path + dataset_name 

    filename = final_path + "/__meta__.nc4"

    # Se non esiste il path ricrealo
    if not path.exists(final_path):
        makedirs(final_path)

    # Se non esiste il file meta di questo particolare dataset, riscaricalo
    if not path.exists(final_path + "/__meta__.nc4"):
        cmd_opendataset = "gsutil -m cp -r gs://bucket-nc/data/" + dataset_name + "/__meta__.nc4 " +  final_path + "/__meta__.nc4"

        # Prova a scaricare il meta del dataset 
        try:
            process_p = subprocess.check_output(cmd_opendataset, shell=True, stderr=subprocess.STDOUT)
            #print("File [" + str(filename) + "] scaricato con successo!")
        except subprocess.CalledProcessError:
            # There was an error - command exited with non-zero code
            print("Errore! Controllare il nome del dataset inserito.")
            rmdir(final_path)
            exit(0)

    

    rootgrp = Dataset(filename)

    
    dict_lat = ()
    dict_lon = ()
    dict_time = ()

    # Dizionario latitudini
    file_lat = open("dictionary/lat.txt", "r")
    for line in file_lat.readlines():
        dict_lat = dict_lat + (line.rstrip(), )
    file_lat.close()
    
    # Dizionario latitudini
    file_long = open("dictionary/lon.txt", "r")
    for line in file_long.readlines():
        dict_lon = dict_lon + (line.rstrip(), )
    file_long.close()

    # Dizionario tempo
    file_time = open("dictionary/time.txt", "r")
    for line in file_time.readlines():
        dict_time = dict_time + (line.rstrip(), )
    file_time.close()


    #print ("Dizionario dei sinonimi delle latitudini: " + str(dict_lat))  
    #print ("Dizionario dei sinonimi delle longitudini: " + str(dict_lon))
    #print ("Dizionario dei sinonimi del tempo: " + str(dict_time))


    #print ("")
    dimensions_divisors = {}
    for dimension in rootgrp.dimensions.values():        
        dimensions_divisors[str(dimension.name)] = calc_divisors(len(dimension))
        #print("'" + str(dimension.name) + "' ha dimensione = " + str(dimension.size))
        #print(str(dimension.name), dimensions_divisors[str(dimension.name)])

    # Convenzione = prima latitudine e poi longitudine
    Y_DIM = "NONE" 
    X_DIM = "NONE" 
    Z_DIM = "NONE" # TIME
    Z_SIZE = 0

    # In letteratura so che è 8MB è un valore ottimo con cui lavorare
    center = 8196
    time = 1
    level = 1
    dtype = 4
    sizes = []

    #print ("")

    # Stampa corrispondenze trovate
    dimensions = {}
    for dimension in rootgrp.dimensions.values():
        if str(dimension.name) in dict_lat:
            #print("Trovata corrispondenza '" + str(dimension.name) + "' relativa ai sinonimi delle latitudini.")
            Y_DIM = str(dimension.name)
        if str(dimension.name) in dict_lon:
            #print("Trovata corrispondenza '" + str(dimension.name) + "' relativa ai sinonimi delle longitudini.")
            X_DIM = str(dimension.name)
        if str(dimension.name) in dict_time:
            #print("Trovata corrispondenza '" + str(dimension.name) + "' relativa ai sinonimi del tempo.")
            Z_DIM = str(dimension.name)
            Z_SIZE = dimension.size
        
    #if (Y_DIM == "NONE"):
        #print ("Non è stata trovata alcuna corrispondenza per la latitudine.")
        #print ("Aggiornare il dizionario dei sinonimi.")

    #if (X_DIM == "NONE"):
        #print ("Non è stata trovata alcuna corrispondenza per la longitudine.")
        #print ("Aggiornare il dizionario dei sinonimi.")
    
    #if (Z_DIM == "NONE"):
        #print ("Non è stata trovata alcuna corrispondenza per il tempo.")
        #print ("Aggiornare il dizionario dei sinonimi.")


    #print ("")
    y_size = dimensions_divisors[Y_DIM][-1]
    x_size = dimensions_divisors[X_DIM][-1]    
    xy_size = y_size * x_size * dtype

    #print("Dimensionalità '" + str(Y_DIM) + "': " + str(y_size))
    #print("Dimensionalità '" + str(X_DIM) + "': " + str(x_size))


    # Stampa gli split della dimensionalità relative alla variabile scelta dall'utente
    for variable in rootgrp.variables.values():
        
        if (variable.name == variable_name):
            for k in variable.ncattrs():
                if (k == "_cos_split_latitude"):
                    _cos_split_latitude = variable.getncattr(k)
                if (k == "_cos_split_longitude"):
                    _cos_split_longitude = variable.getncattr(k)

    
    #print ("\nDimensionalità degli split per la latitudine: " + str(_cos_split_latitude))
    #print ("Dimensionalità degli split per la longitudine: " + str(_cos_split_longitude))

    
    ####

    # Algoritmo per sizes
    for j in range(0, len(dimensions_divisors[Y_DIM])):
        new = []
        m = dimensions_divisors[Y_DIM][j]
        for i in range(0, len(dimensions_divisors[X_DIM])):
            
            n = dimensions_divisors[X_DIM][i]
            q = xy_size / (n*m)
            f = abs(center-q)
            #print(m, n, q, f)

            new.append(f)

        sizes.append(new)
    
    # Sizes contiene lo spazio variabile in tutte le possibili coppie di divisori
    #print("\nSizes:")
    #print(sizes)

    minVal = 1E37
    j0 = -1
    i0 = -1
    
    # Per evitare j = 0 e i = 0
    # Visto che sizes ora va da 0 a N
    for j in range(1, len(sizes)):
        for i in range(1, len(sizes[j])):
            #print (sizes[j][i], minVal)
            if sizes[j][i] < minVal:
                minVal = sizes[j][i]
                j0 = j
                i0 = i
    
    # Divisori della lat. e long.
    div_lat_y = dimensions_divisors[Y_DIM][j0]
    div_lon_x = dimensions_divisors[X_DIM][i0]

    ####

    # Crea liste e azzerale
    start_lat = [0] * div_lat_y
    end_lat = [0] * div_lat_y
    start_lon = [0] * div_lon_x
    end_lon = [0] * div_lon_x

    
    for ind in range(0, div_lat_y):
        start_lat[ind] = (ind * _cos_split_latitude)
        end_lat[ind] = ((_cos_split_latitude * (ind+1)) -1)
    
    for ind in range(0, div_lon_x):
        start_lon[ind] = (ind * _cos_split_longitude) 
        end_lon[ind] = ((_cos_split_longitude * (ind+1)) -1)
    
    #print("\nNumero di split per la latitudine: " + str(div_lat_y))
    #print("Numero di split per la longitudine: " + str(div_lon_x))

    #print ("\n\nRange della dimensionalità di ogni file splittato:")
    

    #print ("\nNum: [start_lat, end_lat]")
    
    #for cnt in range(0, len(start_lat)):
        #print (str(cnt) + ": [" + str(start_lat[cnt]) + ", " + str(end_lat[cnt]) + "]")
    
    #print ("\nNum: [start_lon, end_lon]")

    #for cnt in range(0, len(start_lon)):
        #print (str(cnt) + ": [" + str(start_lon[cnt]) + ", " + str(end_lon[cnt]) + "]")


    # Approssimazione del valore di range lat e lon
    round_start_lat = int(range_lat_start/_cos_split_latitude) * _cos_split_latitude
    round_end_lat = int(range_lat_end/_cos_split_latitude) * _cos_split_latitude + (_cos_split_latitude-1)

    
    round_start_lon = int(range_lon_start/_cos_split_longitude) * _cos_split_longitude
    round_end_lon = int(range_lon_end/_cos_split_longitude) * _cos_split_longitude + (_cos_split_longitude-1)


    """
    print ("\nRange per la latitudine scelto: [" + str(range_lat_start) + "," + str(range_lat_end) + "] - approssimato a [" \
        + str(round_start_lat) + ", " + str(round_end_lat) + "] - dimensionalità iniziale: [" \
            + str(start_lat[0]) + ", " + str(end_lat[-1]) + "]")
    
    print ("Range per la longitudine scelto: [" + str(range_lon_start) + "," + str(range_lon_end) + "] - approssimato a [" \
        + str(round_start_lon) + ", " + str(round_end_lon) +  "] - dimensionalità iniziale: [" \
            + str(start_lon[0]) + ", " + str(end_lon[-1]) + "]")

    """

    
    folder_lat_start = int(range_lat_start/_cos_split_latitude)
    folder_lat_end = int(range_lat_end/_cos_split_latitude)
    file_lon_start = int(range_lon_start/_cos_split_longitude)
    file_lon_end = int(range_lon_end/_cos_split_longitude)
    
    #print ("\nFile interessati: ")
    #print ("Latitudine, cartella da [" + str(folder_lat_start) + "] a [" + str(folder_lat_end) + "]")
    #print ("Longitudine, file da [" + str(file_lon_start) + "] a [" + str(file_lon_end) + "]")

    numero_cartelle = folder_lat_end - folder_lat_start + 1
    numero_file = file_lon_end - file_lon_start + 1

    numero_file_totali = numero_cartelle * numero_file

    #print ("\nNumero di cartelle: " + str(numero_cartelle))
    #print ("Numero di file in una cartella: " + str(numero_file))
    #print ("Numero di file totali: " + str(numero_file_totali))

    new_lat_dim = ((folder_lat_end-folder_lat_start) + 1) * _cos_split_latitude
    new_lon_dim = ((file_lon_end-file_lon_start) + 1) * _cos_split_longitude

    #print ("\nNuova dimensionalità latitudine: " + str(new_lat_dim))
    #print ("Nuova dimensionalità longitudine: " + str(new_lon_dim))



    ##############


    meta_path = final_path + "/__meta__.nc4"

    # Creazione path con nome della variabile, in download
    if not path.exists(final_path + "/" + str(variable_name)):
        makedirs(final_path + "/" + str(variable_name))


    file_merged_path = final_path + "/" + str(variable_name) + "/" + str(variable_name) \
        + "_lat[" + str(round_start_lat) + "-" + str(round_end_lat) \
            + "]_lon[" + str(round_start_lon) + "-" + str(round_end_lon) + "].nc4"

    rootgrp = Dataset(meta_path)


    """
        Scarica i file richiesti dall'utente

        METODO PIU' LENTO CON GSUTIL
    
    print ("")
    count_file = 0
    # Per ora il time è sempre 0
    for ind_j in range(folder_lat_start, folder_lat_end+1):
        for ind_i in range(file_lon_start, file_lon_end+1):
            count_file = count_file + 1
            cmd_download_files = "gsutil -m cp -r gs://bucket-nc/data/" + dataset_name \
                + "/" + variable_name + "/0/" + str(ind_j) + "/" + str(ind_i) \
                    + ".nc4 " + final_path + "/" + variable_name + "/temp/0/" \
                        + str(ind_j) + "/" + str(ind_i) + ".nc4 "
            
            # Prova a scaricare il meta del dataset inserito dall'utente
            try:
                process_p = subprocess.check_output(cmd_download_files, shell=True, stderr=subprocess.STDOUT)
                print("Scaricati (temporaneamente) [" + str(count_file) + "] file con successo!")
            except subprocess.CalledProcessError:
                # There was an error - command exited with non-zero code
                print("Errore! Riprovare.")
                exit(0)
    """

    """
        Metodo più veloce con 'download_blob_to_file', ma richiede le credenziali google.
    """
    storage_client = storage.Client.from_service_account_json("google-credentials.json")


    #print ("")
    count_file = 0
    # Per ora il time è sempre 0
    for ind_j in range(folder_lat_start, folder_lat_end+1):
        for ind_i in range(file_lon_start, file_lon_end+1):
            count_file = count_file + 1
            if not os.path.exists('download/' + dataset_name + '/' + variable_name + '/temp/0/' + str(ind_j)):
                os.makedirs('download/' + dataset_name + '/' + variable_name + '/temp/0/' + str(ind_j))
            with open('download/' + dataset_name + '/' + variable_name + '/temp/0/' + str(ind_j) + '/' + str(ind_i) + '.nc4', 'wb') as file_obj:
                
                storage_client.download_blob_to_file('gs://bucket-nc/data/' + dataset_name \
                    + '/' + variable_name + '/0/' + str(ind_j) + '/' + str(ind_i) + '.nc4', file_obj)
    
    
    #print ("Scaricati [" + str(count_file) + "] file.")


    dataset = Dataset(file_merged_path, "w", format="NETCDF4")

    
    # Ora fill dei dati della variabile scelta

    
    # Dimensioni globali
    dimensions = {}
    for dimension in rootgrp.dimensions.values():    
        if ((str(dimension.name) == str(Y_DIM))):
            dim = dataset.createDimension(str(dimension.name), new_lat_dim)
        
        elif ((str(dimension.name) ==  str(X_DIM))):
            dim = dataset.createDimension(str(dimension.name), new_lon_dim)
            
        else:
            # Dimensione non relativa a lat o lon, quindi recuperabile dal file originario
            dim = dataset.createDimension(str(dimension.name), dimension.size)
        
        dimensions[dimension.name] = dim

    # Copia degli attributi globali
    for name in rootgrp.ncattrs():
        
        # Se c'è _NCProperties vuol dire che non ci sono attributi globali
        if(name != "_NCProperties"):
            dataset.setncattr(name, getattr(rootgrp, name))

    # Variabili
    for variable in rootgrp.variables.values():

        # Dimensioni della variabile
        dimension_names = ()
        for dimension_name in variable.dimensions:
            dimension_names = dimension_names + (dimension_name, )

        """
        C'è un problema con gli attributi _fillValue e missingValue.
        Entrambi non possono essere gestiti dagli utenti in modo normale.
        Bisogna creare tali attributi "ad hoc" ogni volta che si crea una variabile.
        """

        attr_fillValue = False

        # Ottengo l'attributo _FillValue (se esiste)
        for k in variable.ncattrs():
            if (k == "_FillValue" ):
                attr_fillValue = variable.getncattr(k)

        # Se non esiste l'attributo crea regolarmente
        if (attr_fillValue == False):
            temp = dataset.createVariable(variable.name, variable.dtype, dimension_names, zlib=True)
            
        # Se l'attributo esiste
        if (attr_fillValue != False):
            temp = dataset.createVariable(variable.name, variable.dtype, dimension_names, zlib=True, fill_value=attr_fillValue)
        
        # Se la variabile attuale è quella scelta dall'utente
        if variable.name == variable_name:

            for ind_j in range(folder_lat_start, folder_lat_end+1):
                for ind_i in range(file_lon_start, file_lon_end+1):
                    
                    
                    temp_dataset = Dataset(final_path + "/" + variable_name + "/temp/0/" \
                        + str(ind_j) + "/" + str(ind_i) + ".nc4")

                    
                    # Se si tratta di un solo file in totale
                    if numero_file_totali == 1:
                        var_finale = temp_dataset.variables[variable_name][:]
                    
                    # Se sono più file totali
                    else:

                        # Se sono più file totali, ma 1 solo file di longitudine (per cartella, latitudine)
                        if numero_file == 1:
                            
                            # Se siamo alla prima cartella
                            if ind_j == folder_lat_start:
                                var_finale = temp_dataset.variables[variable_name][:]
                            

                            # Se abbiamo già superato la prima cartella
                            else:
                                var_finale = np.concatenate([var_finale, temp_dataset.variables[variable_name][:]], axis=1)
                            
                        # Se ci sono più file, ma una cartella
                        elif numero_cartelle == 1:
                            
                            # Se siamo al primo file
                            if ind_i == file_lon_start:
                                var_finale = temp_dataset.variables[variable_name][:]

                            # Se abbiamo già superato il primo file
                            else:
                                var_finale = np.concatenate([var_finale, temp_dataset.variables[variable_name][:]], axis=2)

                        # Se ci sono più file e più cartelle
                        elif numero_file > 1 and numero_cartelle > 1:
                            
                            # Se siamo alla prima cartella
                            if ind_j == folder_lat_start:

                                # Se siamo al primo file 
                                if ind_i == file_lon_start:
                                    var_parziale = temp_dataset.variables[variable_name][:]

                                # Se abbiamo già superato il primo file (ma non siamo all'ultimo)
                                elif ind_i < file_lon_end:
                                    var_parziale = np.concatenate([var_parziale, temp_dataset.variables[variable_name][:]], axis=2)
                                
                                # Se siamo all'ultimo file
                                else:
                                    var_parziale = np.concatenate([var_parziale, temp_dataset.variables[variable_name][:]], axis=2)
                                                                                
                                    var_parziale_cartella = var_parziale

                            # Siamo alle cartelle successive
                            else:
                                # Se siamo al primo file 
                                if ind_i == file_lon_start:
                                    var_parziale = temp_dataset.variables[variable_name][:]

                                # Se abbiamo già superato il primo file (ma non siamo all'ultimo)
                                elif ind_i < file_lon_end:
                                    var_parziale = np.concatenate([var_parziale, temp_dataset.variables[variable_name][:]], axis=2)
                                
                                # Se siamo all'ultimo file
                                else:
                                    
                                    # parziale di tutti i file lon della cartella
                                    var_parziale = np.concatenate([var_parziale, temp_dataset.variables[variable_name][:]], axis=2)

                                    # parziale di tutti i file della cartella attuale e quella precedente
                                    var_parziale_cartella = np.concatenate([var_parziale_cartella, var_parziale], axis=1)
        
                                    
                                    # aggiorna var_finale
                                    var_finale = var_parziale_cartella
            temp[:] = var_finale[:]
        
        # Se è una variabile diversa (da quella scelta dall'utente)
        else:

            # Se è variabile time
            if variable.name == Z_DIM:
                temp[:] = rootgrp.variables[variable.name][:]
            
            # Se è la lat
            elif variable.name == Y_DIM:
                temp[:] = variable[round_start_lat:round_end_lat+1]

            # Se è la lon
            elif variable.name == X_DIM:
                temp[:] = variable[round_start_lon:round_end_lon+1]
            

            # Se è qualsiasi altra variabile non bisogna fare nulla


        # Copia il resto degli attributi
        for k in variable.ncattrs():
            if (k != "_FillValue"):
                temp.setncattr(k, variable.getncattr(k))
    

    deleteTempPath = 'download/' + dataset_name + '/' + variable_name + '/temp'

    try:
        shutil.rmtree(deleteTempPath)
        #print ("\nDirectory temporanea [" + str(deleteTempPath) + "] rimossa con successo!")
    except:
        print('\nErrore! Non riesco ad eliminare la directory temporanea.')


    #print ("\nFile risultante come [" + file_merged_path + "] creato con successo!")
    dataset.close()


    logging.debug('Done\n')

if __name__ == '__main__':

    counter = Counter()
    
    t1 = threading.Thread(target=worker, args=(counter, "ww33_d01_20200329Z1200", "dir", 20, 234, 320, 924))
    t2 = threading.Thread(target=worker, args=(counter, "ww33_d01_20200329Z1200", "period", 20, 234, 320, 924))
    t3 = threading.Thread(target=worker, args=(counter, "ww33_d01_20200329Z1200", "hs", 20, 234, 320, 924))
    t4 = threading.Thread(target=worker, args=(counter, "ww33_d01_20200329Z1200", "lm", 20, 234, 320, 924))
    t5 = threading.Thread(target=worker, args=(counter, "ww33_d01_20200329Z1200", "fp", 20, 234, 320, 924))
    
    
   
    #TIME 1
    start_time = time.time()
    t1.start()
    t1.join()
    end_time = time.time()

    time_1 = end_time - start_time

    #TIME 2
    start_time = time.time()
    t2.start()
    t2.join()
    end_time = time.time()

    time_2 = end_time - start_time


    #TIME 3
    start_time = time.time()
    t3.start()
    t3.join()
    end_time = time.time()
    
    time_3 = end_time - start_time

    #TIME 4
    start_time = time.time()
    t4.start()
    t4.join()
    end_time = time.time()
    
    time_4 = end_time - start_time

    #TIME
    start_time = time.time()
    t5.start()
    t5.join()
    end_time = time.time()

    time_5 = end_time - start_time



    print("Thread 1: ")
    print("\n--- %s seconds ---" % (time_1))
    print("")
    print("Thread 2: ")
    print("\n--- %s seconds ---" % (time_2))
    print("")
    print("Thread 3: ")
    print("\n--- %s seconds ---" % (time_3))
    print("")
    print("Thread 4: ")
    print("\n--- %s seconds ---" % (time_4))
    print("")
    print("Thread 5: ")
    print("\n--- %s seconds ---" % (time_5))
    print("")
    

    main_thread = threading.currentThread()

    logging.debug('Counter: %d', counter.value)
