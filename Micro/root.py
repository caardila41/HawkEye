import utime, time
import urandom
import os
import re
from machine import UART, Pin
from math import radians, sin, cos, sqrt, atan2
from microGPS import MicropyGPS  # https://github.com/inmcm/micropyGPS
from pico_eth_ch9121 import config
from pico_eth_ch9121.config import reader, writer
from pico_eth_ch9121.tcp_client_socket import TcpClientSocket
from http_client import http_client
from long_calculator import CalculadoraDistancia

# Configuración del módulo GPS
modulo_gps = UART(1, baudrate=9600, tx=Pin(4), rx=Pin(5))
Zona_Horaria = -5
gps = MicropyGPS(Zona_Horaria)

# Configuración de red y constantes
port_host = 3000
host_ip = "192.168.2.5"
ip_dateway = "192.168.2.1"
ip_mask = "255.255.255.0"
min_sample_length = 1.3

# Variables globales
global date
date = ""  # Variable para la fecha
global sycn_timer_flag
sycn_timer_flag = False
flag_storage = False

# Nombre del archivo CSV
filename = "datos.csv"
headers = ["latitude", "longitude", "satelites", "timestamp", "altitud", "date"]

# Inicialización de objetos
http = http_client(host_ip, port_host)
calcultator = CalculadoraDistancia()

# Función para establecer temporizador
def temporizador(segundos):
    global sycn_timer_flag
    tiempo_inicial = utime.time()
    tiempo_final = tiempo_inicial + segundos

    while utime.time() < tiempo_final:
        tiempo_restante = int(tiempo_final - utime.time())
        print(f"Tiempo restante: {tiempo_restante} segundos")
        utime.sleep(1)

    sycn_timer_flag = True

# Función para verificar la existencia de un archivo
def file_exists(file_name):
    try:
        return os.stat(file_name) is not None
    except OSError:
        return False

# Configuración inicial del CH9121
cw = writer.ConfigWriter()
cw.begin()
cw.dhcp_on()
cw.gateway_ip(ip_dateway)
cw.subnet_mask(ip_mask)
cw.p1_tcp_client()
cw.p1_randomly_enable()
cw.p1_baud_rate(115200)
cw.end()

# Lectura de la configuración actual del CH9121
cr = reader.ConfigReader()
cr.print_net()
cr.print_p1()

# Función para convertir grados minutos a grados decimales
def convertToDegree(RawDegrees):
    RawAsFloat = float(RawDegrees)
    firstdigits = int(RawAsFloat / 100) 
    nexttwodigits = RawAsFloat - float(firstdigits * 100) 
    Converted = float(firstdigits + nexttwodigits / 60.0)
    Converted = '{0:.6f}'.format(Converted)
    return float(Converted)

# Función para validar el formato de mensajes GPRMC
def validation_gprmc(elementos):
    patrones = [
        r"^b'\$GPRMC$",  # Patrón para b'$GPRMC'
        r"^\d\d\d\d\d\d\.\d\d$",  # Patrón para hora en formato HHMMSS.SS
        r"^[AV]$",  # Patrón para estatus
        r"^\d\d\d\d\d\.\d\d\d\d\d$",  # Patrón para latitud en formato DDMM.MMMMM
        r"^[NS]$",  # Patrón para Norte o Sur
        r"^\d\d\d\d\d\d\.\d\d\d\d\d$",  # Patrón para longitud en formato DDDMM.MMMMM
        r"^[EW]$",  # Patrón para Este u Oeste
        r"^\d\.\d\d\d$",  # Patrón para HDOP en formato D.DD
        r"^$",  # Patrón para campo vacío
        r"^\d\d\d\d\d\d$",  # Patrón para fecha en formato DDMMYY
        r"^$",  # Patrón para campo vacío
        r"^$",  # Patrón para campo vacío
        r"^[AV]$",  # Patrón para estatus
        r"^\*\d\d\\r\\n'$"  # Patrón para checksum'
    ]

    if len(elementos) != len(patrones):
        print("Faltan elementos")
        return False
    
    if not re.match(patrones[1], elementos[1]) and not re.match(patrones[7], elementos[7]):
        return False   
    
    return True

# Función para validar el formato de mensajes GPGGA
def verificar_formato(elementos):
    patrones = [
        r'^b\'\$GPGGA$',
        r'^\d\d\d\d\d\d\.\d\d$',  # Hora en formato HHMMSS.SS
        r'^\d\d\d\d\.\d\d\d\d\d$',  # Latitud en formato DDMM.MMMMM
        r'^[NS]$',          # Norte o Sur
        r'^\d\d\d\d\d\.\d\d\d\d\d$',  # Longitud en formato DDDMM.MMMMM
        r'^[EW]$',          # Este u Oeste
        r'^\d$',            # Estatus
        r'^\d$|^\d\d$',     # Número de satélites
        r'^\d\.\d\d$',     # HDOP en formato D.DD
        r'^\d\d\d\d\.\d$',     # Altitud en formato D.D
        r'^M$',             # Unidad de altitud
        r'^\d\.\d$',        # Geoidal en formato D.D
        r'^M$',             # Unidad de geoidal
        r'^$',              # Campo vacío
        r'^\*\d\d\\r\\n\'$'  # Checksum en formato *XX\r\n'
    ]
   
        
    # Verificar que cada elemento coincide con su patrón correspondiente
    if len(elementos) != len(patrones):
        print("Faltan elementos")
        return False
    
    for elemento, patron in zip(elementos, patrones):
        print(f"Elemento: {elemento} Patron: {patron}")
        if not re.match(patron,elemento):
            return False
    
    return True

# Función para guardar datos en un archivo CSV
def save_to_csv(file_name, data):
    try:
        with open(file_name, 'a') as f:
            data_to_save = ','.join(str(value) for value in data) + '\n'
            f.write(data_to_save)
            f.close()
    except Exception as e:
        print("Error al guardar datos en el archivo:", e)

# Función para limpiar un archivo CSV
def clear_csv(file_name, data):
    try:
        with open(file_name, 'w') as f:
            data_to_save = ','.join(str(value) for value in data) + '\n'
            f.write(data_to_save)
            f.close()
    except Exception as e:
        print("Error al guardar datos en el archivo:", e)

# Función para leer datos desde un archivo CSV
def read_csv(filename):
    try:
        with open(filename, 'r') as f:
            lines = f.readlines()
            if not lines:
                print("No hay datos, creando encabezados")
                save_to_csv(filename, headers)
                return []
            
            headers_file = lines[0].replace('\n,', '').strip().split(',')
            data = []
            for line in lines[1:]:
                values = line.strip().replace('\n,', '').split(',')
                if len(values) == 6:
                    row = {headers_file[i]: values[i] for i in range(len(headers))}
                    data.append(row)
            
            print("Formateando archivo...")
            clear_csv(filename, headers)
            print("Creado")

            return data
    except Exception as e:
        print("Error al leer datos del archivo:", e)
        return []

# Hilo para el temporizador
def synk_task_thread():
    while True:
        temporizador(60)

# Obtener fecha desde el gateway
request = http.get("/date")
if request is None:
    flag_gprmc = True
    while flag_gprmc:
        largo = modulo_gps.any()
        if largo > 255:
            data_bytes = modulo_gps.readline()
            data = str(data_bytes)
            parts = data.split(',')
                
            if (parts[0] == "b'$GPRMC" ):
                print(parts)
                if validation_gprmc(parts):
                    for x in data:
                        gps.update(x)                
                    date = "{0}-{1:02d}-{2:02d}".format(gps.date[2], gps.date[1], gps.date[0])
                    print(date)
                    flag_gprmc = False
else:
    res = http.get_body(request[0])
    date = res["date"]

print("Inicio de lectura")
stored_data = read_csv(filename)
print(stored_data)
if stored_data != []:
    request = http.post("/api/coordinates_batch", stored_data)
    if request is None:
        print("No se puede enviar lote al inicio")
        for record in stored_data:
            save_to_csv(filename, [record["latitude"], record["longitude"], record["satelites"], record["timestamp"], record["altitud"], record["date"]])
            flag_storage = True
    else:
        flag_storage = False

# Bucle principal para la lectura continua del GPS
while True:
    largo = modulo_gps.any()
    if largo > 255:
        data_bytes = modulo_gps.readline()
        data = str(data_bytes)
        parts = data.split(',')

        if (parts[0] == "b'$GPRMC" ):
            print(parts)
            if validation_gprmc(parts):
                for x in data:
                    gps.update(x)                
          
            print(gps.date)
            
        if parts[0] == "b'$GPGGA" and  len(parts)>7:
            print("$GPGGA")
            print(parts)
            
            if verificar_formato(parts):
                latitude = convertToDegree(parts[2])
                if (parts[3] == 'S'):
                    latitude = -latitude
                longitude = convertToDegree(parts[4])
                if (parts[5] == 'W'):
                    longitude = -longitude
                satelites = parts[7]
                timestamp = parts[1][0:2] + ":" + parts[1][2:4] + ":" + parts[1][4:6]
                FIX_STATUS = True
                altitud = parts[9]

                data_dict = {
                    "latitude": str(latitude),
                    "longitude": str(longitude),
                    "satelites" : str(satelites),
                    "timestamp" : str(timestamp),
                    "altitud" : str(altitud)
                }
                
                if not first_iteration:
                    distancia = calcultator.distancia_haversine_extendida((latitude,longitude,float(altitud)),(float(old_data["latitude"]),float(old_data["longitude"]),float(old_data["altitud"])))
                    print(f"distancia -->{distancia }")
                    
                    if distancia > min_sample_length:
                        if sycn_timer_flag:
                            request = http.get("/date")
                            if request is None:
                                pass
                            else:
                                res = http.get_body(request[0])
                                date = res["date"]
                                
                        data_dict["date"] = date
                                
                        request = http.post("/api/coordinates", data_dict)
                        
                        if request is None:
                            print("Guardando registro...")
                            save_to_csv(filename, [latitude, longitude, satelites, timestamp, altitud, date])
                            flag_storage = True
                             
                        elif flag_storage == True:
                            stored_data = read_csv(filename)
                            if stored_data != []:
                                request = http.post("/api/coordinates_batch", stored_data)
                                if request is None:
                                    print("No se puede enviar datos en lote")
                                    for record in stored_data:
                                        save_to_csv(filename, [record["latitude"], record["longitude"], record["satelites"], record["timestamp"], record["altitud"], record["date"]])
                                        flag_storage = True
                                else:
                                    flag_storage = False

                old_data = data_dict.copy()
                first_iteration = False
            else:
                print("No cumple el formato")
                print(parts)
