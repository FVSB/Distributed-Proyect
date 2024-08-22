from utils import SearchServers
import socket
import requests
import time
import pickle
import os
ip = socket.gethostbyname(socket.gethostname())
servers=SearchServers(ip=ip,port=8001)

def get_query_from_server(server_ip:str,params:dict):
    try:
            # Realizar la solicitud GET con los parámetros
            url_query=f'http://{server_ip}:8000/query'
            response = requests.get(url_query, params=params, stream=True)
            response.raise_for_status()

            

            if response.status_code==200:
                #{"id":result.id,"title":result.title,"snipet":result.snippet,"score":result.score}
                return response.json()
    except:
            return None

def make_query(query:str,posibles_extensions:list[str],max_results:int=10,min_score:float=0,):
    
    
    
    while True:
        server_ip:str=servers.get_random_server_ip()
            
        params={"query":query,"max_results":max_results,"min_score":min_score,"extensions":posibles_extensions}
        
        response:dict=get_query_from_server(server_ip,params=params)
        
        if response is None:
            continue
        
        return response
    
def make_crud_post(server_ip:str,sub_route:str,data:object)->dict:
    """
    dado el ip de un server la subruta y la data a enviar, se encarga de hacerle una peticion al server
    se encarga de redirigir devuelve el dicc si todo fue ok y None si hubo algun error

    Args:
        server_ip (str): _description_
        sub_route (str): _description_
        files (dict[str,bytes]): _description_

    Raises:
        Exception: _description_

    Returns:
        dict: _description_
    """
  
    try:
        data_bytes=pickle.dumps(data)
    
        files={'file':data_bytes}

        url=f'http://{server_ip}:8000/{sub_route}'
        
        response = requests.post(url, files=files,timeout=200)
        
        if response.status_code in [404,i for i in range(499,600,1)]:
            raise Exception(f'La respuesta tuvo un codigo {response.status_code}')
        # Imprime la respuesta del servidor
        data=response.json()
        if response.status_code==301:
            
            new_ip=data['ip']
            return make_crud_post(server_ip=new_ip,sub_route=sub_route,files=files)
        
        return data
    except Exception as e:# Si hubo un error pq el nodo se cayo o algo retorno none
        return None
    
def _insert_update_helper(title:str,text:str,is_insert:bool)->str:
    """
    Este metodo es para que lo usen los metodos de insertar y actualizar

    Args:
        title (str): _description_
        text (str): _description_
        is_insert (bool): True si es insertar  False si es actualizar

    Returns:
        str: _description_
    """
    server_ip:str=servers.get_random_server_ip()  
    sub_route="upload" if is_insert else "update"
    resp=make_crud_post(server_ip=server_ip,sub_route=sub_route,data=(title,text))
    
    a='insertando' if is_insert else 'actualizando'
    if resp is None:
        return f"Ocurrio un error {a}: {title}"
    
    return resp
def insert_document(title:str,text:str)->str:
    """
    LLamar para insertar un documento

    Args:
        title (str): _description_
        text (str): _description_

    Returns:
        str: _description_
    """
    return _insert_update_helper(title=title,text=text,is_insert=True)
    
    
    


def update_document(title:str,text:str)->str:
    """
    Metodo para actualizar los docuementos

    Args:
        title (str): _description_
        text (str): _description_

    Returns:
        str: _description_
    """
    
    return _insert_update_helper(title=title,text=text,is_insert=False)


def delete_document(title:str)->str:
    """
    Llamar para eliminar un documento

    Args:
        title (str): _description_

    Returns:
        str: _description_
    """
    server_ip:str=servers.get_random_server_ip()  
    
    resp=make_crud_post(server_ip=server_ip,sub_route='delete_file',data=title)
    
   
    if resp is None:
        return f"Ocurrio un error Eliminando: {title}"
    
    return resp
    
class Paquete:
    def __init__(self, numero, bytes_datos, es_final):
        self.numero = numero
        self.bytes_datos = bytes_datos
        self.es_final = es_final

    def serialize(self):
        return pickle.dumps(self)

    @staticmethod
    def deserialize(data) -> "Paquete":
        return pickle.loads(data)

    
def _download_file_and_download(url, file)->str:
    chunk_size = 1024  # Tamaño de cada paquete
    start_part = 1
    folder = "/app/logs"
    save_path = os.path.join(folder, file)
    file_exists = os.path.exists(save_path)

    if file_exists:
        # Abre el archivo en modo binario para leer su contenido
        with open(file, "rb") as f:
            # Lee el archivo en bloques de 1024 bytes
            block_size = 1024
            blocks_count = 0
            while True:
                # Lee el siguiente bloque del archivo
                block = f.read(block_size)

                # Si el bloque leído es vacío, hemos llegado al final del archivo
                if not block:
                    break

                # Incrementa el contador de bloques
                blocks_count += 1

                # Imprime el número total de bloques de 1024 bytes
            print(blocks_count)
            start_part = blocks_count + 1

    try:
        print(f"La start_part es {start_part}")
        # Definir los parámetros de la solicitud
        params = {"start": start_part, "name": file}

        # Realizar la solicitud GET con los parámetros
        response = requests.get(url, params=params, stream=True)
        if  response.status_code==409:
            res=response.json()
            return res['message']
        response.raise_for_status()
        text=""
        with open(save_path, "ab" if file_exists else "wb") as f:
            #print("aca")
            for chunk in response.iter_content(chunk_size=None):
                #print("acaaa")
                if chunk:
                    # f.write(chunk)
                    paquete = Paquete.deserialize(chunk)
                    #print("ff")
                    a: str = pickle.loads(paquete.bytes_datos)
                    text+=a
                    f.write(a.encode())
        
        return text
    except requests.exceptions.RequestException as e:
        return f"An error occurred: {e}"
    
    


def _download_file(url,file)->str:
    chunk_size = 1024  # Tamaño de cada paquete
    start_part = 1
    folder = "/app/logs"
    save_path = os.path.join(folder, file)
    file_exists = os.path.exists(save_path)
    try:
        print(f"La start_part es {start_part}")
        # Definir los parámetros de la solicitud
        params = {"start": start_part, "name": file}

        # Realizar la solicitud GET con los parámetros
        response = requests.get(url, params=params, stream=True)
        if  response.status_code==409:
            res=response.json()
            return res['message']
        response.raise_for_status()
        text=""
        for chunk in response.iter_content(chunk_size=None):
                
                if chunk:
                    
                    paquete = Paquete.deserialize(chunk)
                    
                    a: str = pickle.loads(paquete.bytes_datos)
                    text+=a
                   
        
        return text
    except requests.exceptions.RequestException as e:
        return f"An error occurred: {e}"
    
    
    
def download_file(title:str,save_file:bool=False)->str:
    
    ip=servers.get_random_server_ip()
    file_url = f"http://{ip}:8000/get_document_by_name"  # URL del servidor Flask
    
    return _download_file(url=file_url,file=title) if not save_file else _download_file_and_download(url=file_url,file=title)


    

