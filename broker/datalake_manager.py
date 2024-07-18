from azure.storage.blob import BlobServiceClient
import time
import pandas as pd
import threading
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json
import os
from io import BytesIO
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Puedes establecer el nivel específico para este logger

    
load_dotenv()

account_name = os.getenv("ACCOUNT_NAME")
account_key = os.getenv("KEY")



# Create the BlobServiceClient
client = BlobServiceClient(
    account_url=f"https://{account_name}.blob.core.windows.net",
    credential=account_key,
)


def drop_intermediate_dates(df):
    # Asegúrate de que la columna de fecha sea de tipo datetime
   

    # Crea una serie booleana para seleccionar filas intercaladas
    mask = [i % 2 == 0 for i in range(len(df))]

    # Conserva solo las filas seleccionadas
    df = df[mask]

    return df


def mide_tiempo(funcion):
    def funcion_medida(*args, **kwargs):
        inicio = time.time()
        c = funcion(*args, **kwargs)
        timer =  time.time() - inicio
        logging.info(f'Tiempo De La Operación: {timer} \n')
        return c
    return funcion_medida    

@mide_tiempo
def get_file(container_name, file_name):
    try:
        print("Get File: ",file_name)
        blob_client = client.get_blob_client(container_name, file_name)
        blob = blob_client.download_blob()
        data_file = pd.read_csv(blob)
        del blob
        return data_file
    except Exception as e:
        print(e)
    finally:
        if blob_client:
            blob_client.close()

@mide_tiempo
def get_file_parquet(container_name, file_name):
    try:
        print("Get File: ",file_name)
        blob_client = client.get_blob_client(container_name, file_name)
        blob = blob_client.download_blob()
        bytes_io = BytesIO(blob.readall())
        data_file = pd.read_parquet(bytes_io)  
        del bytes_io
        return data_file
    except Exception as e:
        print(e)
    finally:
        if blob_client:
            blob_client.close()

@mide_tiempo
def get_data_day_datalake(deviceId, fecha_inicio, container_name="raw-csv",file_type="csv"):
    
    year = fecha_inicio.year
    month = str(fecha_inicio.month).zfill(2)
    day = str(fecha_inicio.day).zfill(2)
    data = pd.DataFrame()
    try:
        file_name = f"/deviceId={deviceId}/year={year}/month={month}/day={day}/"
        
        blobs_client = client.get_container_client(container_name)
        blobs = blobs_client.list_blobs(file_name)
        files_name = []
        
        
        
        for blob in blobs:
            if file_type=="csv" and blob.name.endswith(".csv"):                    
                files_name.append(blob.name)
            elif file_type=="parquet" and blob.name.endswith(".parquet"):
                files_name.append(blob.name)
        dataframes = []
        
        for file in files_name:
            if file_type=="csv":                
                data_file = get_file(container_name, file)
                dataframes.append(data_file)
            elif file_type=="parquet":
                data_file = get_file_parquet(container_name, file)
                dataframes.append(data_file)
            
            
        
        if not dataframes == []:
            data = pd.concat(dataframes, ignore_index=True)
        return data
    except Exception as  e:
        print(e)
    
    finally:
        if blobs_client:
            blobs_client.close()
        




def process_batch_operational_data(query):
    
    inicio = time.time()
    logging.info(f"empieza-> {inicio}")
    # Realiza la operación de procesamiento aquí
    data = query
    
    timer = time.time() - inicio
    logging.info('Tiempo De La Operación: {timer} \n')
    return data

def execute_queries_parallel_and_process(queries):
    results = []
    threads = []

    for query in queries:
        thread = threading.Thread(
            target=lambda rig,date,container,file_type, re: re.append(get_data_day_datalake(rig,date,container,file_type)), \
                args=(query["rig"],query["fecha"],query["container"],query["file_type"], results))
        thread.start()
        threads.append(thread)

    # Espera a que todos los hilos terminen
    for thread in threads:
        thread.join()

    return results

@mide_tiempo
def get_data_dataLake_parallel(fecha_inicio:datetime, fecha_fin:datetime, deviceId:str, container:str, file_type:str):
    logging.info("<------------------------ inicio del proceso ------------------------>")
    
    #fecha_inicio = datetime.strptime(fecha_inicio, "%Y-%m-%d %H:%M:%S")
    #fecha_fin = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S")
    
    fecha_inicio_org = fecha_inicio
    fecha_fin_org = fecha_fin
    
    fecha_inicio = (fecha_inicio - timedelta(days=1))
    fecha_fin = (fecha_fin + timedelta(days=1))
    
    dataframes = []
    querys = []
    while fecha_inicio < fecha_fin:

        querys.append({"rig":deviceId,
                       "fecha":fecha_inicio,
                       "container":container,
                       "file_type":file_type})
        
        fecha_inicio = (fecha_inicio + timedelta(days=1)).replace(hour=0, minute=0, second=0)
        
    
    rig_data =  execute_queries_parallel_and_process(querys)   
        
    for data_operation_rig in rig_data:
        if not data_operation_rig.empty:
            dataframes.append(data_operation_rig)
        
    if dataframes != []:
        logging.info("<------------------------ fin del proceso ------------------------>")    
        data = pd.concat(dataframes, ignore_index=True)
        
        data['fecha_hora'] = pd.to_datetime(data['fecha_hora'], format='%Y-%m-%dT%H:%M:%S.%fZ')
        data = data.sort_values(by='fecha_hora')
        
        data = data[(data['fecha_hora'] >= fecha_inicio_org) & (data['fecha_hora'] <= fecha_fin_org)]

        data['fecha_hora'] = data['fecha_hora'].dt.strftime('%Y-%m-%d %H:%M:%S')
        data = data.reset_index(drop=True)
        
        data['fecha_hora'] = pd.to_datetime(data['fecha_hora'])

        # Se ordenan los datos por fecha y hora.
        data = data.sort_values(by="fecha_hora").reset_index(drop=True)
        
        return data

    

    
def update_datalake(fecha_dt, dataframe:pd.DataFrame, deviceId, container_name,):
        
    year = fecha_dt.year
    month = str(fecha_dt.month).zfill(2)
    day = str(fecha_dt.day).zfill(2)
  
    file_name_insert = f"deviceId={deviceId}/year={year}/month={month}/day={day}/{deviceId}-{year}-{month}-{day}.csv"
    blob_client = client.get_blob_client(container_name, file_name_insert)
    if blob_client.exists():
        data = get_file(container_name,file_name_insert)
        result_concat = pd.concat([data, dataframe], ignore_index=True)
        blob_client.upload_blob(result_concat.to_csv(index=False), overwrite=True)
    else:
        blob_client.upload_blob(dataframe.to_csv(index=False), overwrite=True)