import socket
import pickle
import traceback
def _send_data(addr: str, data: bytes,ip,port) -> bool:
        """metodo para enviar data

        Args:
            addr (str): _description_
            data (bytes): _description_

        Returns:
            bool: _description_
        """
        try:
           

            # Crear el socket del cliente
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # Conectar al servidor
            client_socket.connect((ip, port))

            # Enviar los datos
            client_socket.sendall(data)

            # Cerrar la conexión
            client_socket.close()
            print(f"Enviado la data al addr {addr}")
            return True
        except Exception as e:
            print(f"Error enviando data al addr { addr} Error: {e} \n {traceback.print_exc()} ")
            return False
        
ip = socket.gethostbyname(socket.gethostname())       
_send_data("172.18.0.6:8001",pickle.dumps("hola"),"172.18.0.6",8001)