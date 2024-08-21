#from ..distributed.helper.protocol_codes import FIND_NODES

import pickle
import socket
import threading
import time
import threading
def handle_client(client_socket):
    try:
        while True:
            # Recibir datos del servidor
            data = client_socket.recv(1024)
            if not data:
                break
            print("Mensaje recibido:", pickle.loads(data))
    except Exception as e:
        print(f"Error al recibir datos: {e}")
    finally:
        client_socket.close()

def start_server(host: str, port: int):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(1)
    print(f"Escuchando en {host}:{port}")

    while True:
        client_socket, addr = server_socket.accept()
        print(f"Conexión aceptada desde {addr}")
        # Crear un nuevo hilo para manejar el cliente
        client_thread = threading.Thread(target=handle_client, args=(client_socket,))
        client_thread.start()
        
        
def broadcast(port=8001,time_:float=2):
    while True :
        try:     
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            to_send = pickle.dumps(
                (28, None)
            )  # Serializar el objeto para poder enviarlo
            s.sendto(
                to_send, (str(socket.INADDR_BROADCAST), port)
            )  # Enviar el broadcast
    
            s.close()
            time.sleep(time_)
        except:
            print("Ocurrio un error al enviar el broadcast")
            
threading.Thread(target=broadcast,daemon=True).start()
# Configuración del servidor (cliente en modo servidor)
ip = socket.gethostbyname(socket.gethostname())
host = ip # Aceptar conexiones de cualquier IP
port = 8001      # Puerto de escucha

start_server(host, port)
