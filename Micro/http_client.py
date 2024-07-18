import utime
from pico_eth_ch9121.tcp_client_socket import TcpClientSocket
import json

class http_client:
    
    def __init__(self, host_dst, port):
        """
        Inicializa el cliente HTTP con el host de destino y el puerto.

        Args:
        - host_dst (str): Dirección IP o nombre de host del servidor.
        - port (int): Puerto de conexión del servidor.
        """
        self.host = host_dst
        self.port = port
        
    def petition(self, request):
        """
        Realiza una petición HTTP al servidor y devuelve la respuesta.

        Args:
        - request (str): Solicitud HTTP en formato de cadena UTF-8.

        Returns:
        - response (bytes): Respuesta recibida del servidor en formato de bytes.
        """
        socket = TcpClientSocket(self.host, self.port)
        response = None
        for attempt in range(2):  # Intentar dos veces
            try:
                if socket is not None:
                    # Enviar solicitud al servidor y recibir respuesta
                    print(f">> {request.replace('\r', '\\r').replace('\n', '\\n')} | {socket.send_utf8_str(request)}")
                    response = socket.receive_sync(0.02)
                    print("Response:")
                    print(response)
                    utime.sleep_ms(200)
                    if response is not None:
                        break  # Salir del bucle si la solicitud fue exitosa
            except Exception as e:
                print(f"Error en la solicitud HTTP: {e}")
                utime.sleep(1)  # Esperar un segundo antes de reintentar
                continue  # Intentar nuevamente
            
        return response
    
    def get_body(self, http_response):
        """
        Extrae y devuelve el cuerpo de la respuesta HTTP en formato JSON.

        Args:
        - http_response (bytes): Respuesta HTTP recibida del servidor en formato de bytes.

        Returns:
        - body_dict (dict): Cuerpo de la respuesta HTTP parseado como un diccionario.
        """
        # Convertir bytes a cadena
        response_str = http_response.decode('utf-8')

        # Separar encabezados y cuerpo
        headers, body = response_str.split('\r\n\r\n', 1)
        
        # Procesar el cuerpo JSON si existe
        if len(body) > 0:
            body_dict = json.loads(body)
            return body_dict
        
    def get_response(self, http_response):
        """
        Extrae y devuelve el código de respuesta HTTP.

        Args:
        - http_response (bytes): Respuesta HTTP recibida del servidor en formato de bytes.

        Returns:
        - dict_response (dict): Diccionario que contiene el código de respuesta HTTP.
        """
        # Convertir bytes a cadena
        response_str = http_response.decode('utf-8')

        # Separar encabezados y cuerpo
        headers, body = response_str.split('\r\n\r\n', 1)
        
        # Extraer el código de respuesta HTTP
        if len(headers) > 1:
            dict_response ={
                "code": headers[1]
            }
        
        return dict_response
