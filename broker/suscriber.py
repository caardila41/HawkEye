import paho.mqtt.client as mqtt
import json
from azure.storage.filedatalake import DataLakeServiceClient, DataLakeDirectoryClient
from datetime import datetime
import os
from dotenv import load_dotenv
import pandas as pd
from datalake_manager import update_datalake  # Suponiendo que este es un módulo personalizado para gestionar el Data Lake

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Obtener variables de entorno
account_name = os.getenv("ACCOUNT_NAME")
credential = os.getenv("KEY")
file_system_name = os.getenv("FILE_SYSTEM_NAME")

# Configuración del cliente MQTT
broker_address = "localhost"  # Dirección del broker MQTT
port = 1883  # Puerto del broker MQTT
topic = "torre/coordenadas"  # Tema al que se suscribe el cliente MQTT

# Función para manejar mensajes recibidos
def on_message(client, userdata, message):
    """
    Maneja los mensajes recibidos del broker MQTT.
    Guarda los datos recibidos en Azure Data Lake Storage.
    
    Args:
    client (mqtt.Client): Cliente MQTT que recibe el mensaje.
    userdata: Datos del usuario (no se utiliza aquí).
    message (mqtt.MQTTMessage): Mensaje MQTT recibido.
    """
    payload = message.payload.decode()  # Decodificar el payload del mensaje
    data_dict = json.loads(payload)  # Convertir el payload JSON a un diccionario Python
    print(f"Mensaje recibido: {payload}")
    print(type(data_dict))
    
    # Convertir el diccionario de datos a un DataFrame de Pandas
    df = pd.DataFrame([data_dict])
    print(df)
    
    # Guardar el mensaje en Azure Data Lake Storage
    save_to_data_lake(df)

def save_to_data_lake(data):
    """
    Guarda los datos en Azure Data Lake Storage.
    
    Args:
    data (pd.DataFrame): DataFrame de Pandas con los datos a guardar.
    """
    try:
        # Llamar a una función personalizada para actualizar el Data Lake
        update_datalake(datetime.now(), data, "Torre1", "hawkeye")
        
        print("Datos almacenados correctamente en Azure Data Lake Storage.")
    except Exception as e:
        print(f"Error al almacenar datos en Azure Data Lake Storage: {str(e)}")

# Configurar el cliente MQTT
client = mqtt.Client()
client.on_message = on_message

# Conectar al broker MQTT
client.connect(broker_address, port=port)
client.subscribe(topic)  # Suscribirse al tema especificado

# Iniciar el bucle de mensajes para recibir continuamente
client.loop_forever()
