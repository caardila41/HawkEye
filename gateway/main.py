from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ValidationError
from app.modelo import Coordinates, OutputCoordinates
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
import os

# Inicialización de la aplicación FastAPI
app = FastAPI()

# Variables globales para el estado del broker MQTT y la persistencia de datos fallidos
global broker_connection_flag
global data_fail

broker_connection_flag = True
data_fail = True

# Nombre del archivo CSV para guardar coordenadas fallidas
filename_to_send = "coordenadas_fallidas.csv"

# Función para leer datos desde un archivo CSV
def read_csv(filename):
    if os.path.isfile(filename):
        df_coord = pd.read_csv(filename)
        # Borra el archivo después de leerlo exitosamente
        try:
            os.remove(filename)
            print(f"Archivo {filename} borrado exitosamente.")
        except Exception as e:
            print(f"Error al intentar borrar el archivo: {e}")
        return df_coord

# Función para manejar la tarea periódica de envío de datos
def tarea_periodica():
    global broker_connection_flag
    global data_fail
    
    print(f"Tarea ejecutada: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    save_again = False
    
    if broker_connection_flag and data_fail:
        df_to_send = read_csv(filename_to_send)
        data_to_save_again = []
        
        if df_to_send is not None:
            print("Hay datos para enviar")
            for index, row_df in df_to_send.iterrows():
                row_dict_to_send = row_df.to_dict()
                try:
                    if not publish(row_dict_to_send.copy()):
                        save_again = True
                        data_fail = True
                        broker_connection_flag = False
                except Exception as e:
                    print(f"Error {e}")
                    save_again = True
                    data_fail = True
                
                if save_again:
                    try:
                        coords = Coordinates(**row_dict_to_send)
                        data_to_save_again.append(coords)
                    except ValidationError as ve:
                        print(f"Error de validación: {ve}")
            
            if save_again:
                for point in data_to_save_again:
                    print("Guardando datos para reintentar")
                    guardar_csv(point, filename_to_send)
            
            broker_connection_flag = False
            data_fail = False
        else:
            print("No hay datos para enviar")

# Inicialización del planificador de tareas periódicas
scheduler = BackgroundScheduler()
scheduler.add_job(tarea_periodica, 'interval', seconds=30)
scheduler.start()

# Manejador de eventos para el apagado de la aplicación
@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()

# Función de callback para manejar la conexión MQTT
def on_connect(client, userdata, flags, rc):
    print(f"Conectado con el código de resultado {rc}")

# Función para enviar un mensaje MQTT
def enviar_mensaje(client, topic, message):
    client.publish(topic, message)
    print(f"Mensaje enviado a {topic}: {message}")

# Cliente MQTT
client = mqtt.Client()

# Función para conectar al broker MQTT
def conectar_mqtt():
    try:
        client.connect("**.**.***.***", 1883, 60)  # Ajustar según la configuración del servidor MQTT
        client.loop_start()
        return True
    except Exception as e:
        print(f"Error al conectar al broker MQTT: {e}")
        return False

# Función para guardar coordenadas en CSV si falla la conexión MQTT
def guardar_csv(coordenadas, archivo):
    df = pd.DataFrame([coordenadas.dict()])
    if not os.path.isfile(archivo):
        df.to_csv(archivo, index=False)
    else:
        df.to_csv(archivo, mode='a', header=False, index=False)

# Endpoint para obtener la hora actual
@app.get("/time")
def get_time():
    current_time = datetime.now().strftime("%H:%M:%S")
    return {"time": current_time}

# Endpoint para obtener la fecha actual
@app.get("/date")
def get_date():
    current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return {"date": current_date}

# Endpoint raíz de prueba
@app.get("/h")
async def root():
    return {"message": "Hello World"}

# Función para transformar y validar las coordenadas recibidas
def transform_schema_dict(input_dict: dict):
    date_str = input_dict.pop('date')
    time_str = input_dict.pop('timestamp')
    datetime_str = f"{date_str} {time_str}"
    input_dict['date_time'] = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
    return OutputCoordinates(**input_dict)

# Función para publicar las coordenadas en MQTT
def publish(point: dict):
    coord = transform_schema_dict(point)
    
    if conectar_mqtt():
        data_to_send = coord.model_dump_json()
        print(f"Publicando {data_to_send}")
        client.publish("torre/coordenadas", data_to_send, qos=1)
        client.loop_stop()
        client.disconnect()
        return True
    return False

# Endpoint para recibir coordenadas individuales y procesarlas
@app.post("/api/coordinates")
async def receive_coordinates(coords: Coordinates):
    global broker_connection_flag
    global data_fail
    
    print(f"Coordenadas recibidas: {coords.latitude}, {coords.longitude}, {coords.satelites}, {coords.altitud}, {coords.timestamp} {coords.date}")

    try:
        guardar_csv(coords, "backup.csv")
        if not publish(coords.model_dump()):
            guardar_csv(coords, filename_to_send)
            data_fail = True
            broker_connection_flag = False
            return {"status": "Guardado en CSV debido a fallo de conexión"}
        else:
            broker_connection_flag = True
            return {"status": "Enviado al MQTT"}
        
    except Exception as e:
        print(e)

# Endpoint para recibir una lista de coordenadas y procesarlas
@app.post("/api/coordinates_batch")
async def receive_coordinates_batch(coords: list[Coordinates]):
    global broker_connection_flag
    global data_fail
    
    print("Coordenadas recibidas en lote")
    print(coords)
    
    try:
        for point in coords:
            guardar_csv(point, "backup.csv")
            
        for point in coords:
            if not publish(point.model_dump()):
                guardar_csv(point, filename_to_send)
                data_fail = True
                broker_connection_flag = False
            else:
                broker_connection_flag = True
        
        return {"message": "Coordenadas enviadas correctamente"}
    
    except Exception as e:
        print(e)
