import Pyro5.api
import socket
import threading
import sys
import time
import hashlib
import traceback
import subprocess
import logging
from logguer import log_message
from concurrent.futures import ThreadPoolExecutor, TimeoutError

    
def getShaRepr(data: str, max_value: int = 16):
    # Genera el hash SHA-1 y obtén su representación en hexadecimal
    hash_hex = hashlib.sha1(data.encode()).hexdigest()

    # Convierte el hash hexadecimal a un entero
    hash_int = int(hash_hex, 16)

    # Define un arreglo o lista con los valores del 0 al 16
    values = list(range(max_value + 1))

    # Usa el hash como índice para seleccionar un valor del arreglo
    # Asegúrate de que el índice esté dentro del rango válido
    index = hash_int % len(values)

    # Devuelve el valor seleccionado
    return values[index]    

def getChordNameAddress(id_:int):
    return f'{id_}.chord'

def check_if_url_is_active(url:str):
        """Chequea que urls estan activas """
            
        ns = Pyro5.api.locate_ns()
        uri = ns.lookup(url)
        try:
            proxy = Pyro5.api.Proxy(uri)
            # Intentar llamar a un método que confirme la conexión o estado del servidor
            # Puedes usar cualquier método definido en el objeto remoto para confirmar la conexión
            node:ChordNodeReference = proxy.ping()  # Suponiendo que el objeto remoto tiene un método ping() para verificar la conexión
            if node is not None:  # Puedes definir el criterio para confirmar que el servidor está activo
                return node
            return None
        except Pyro5.errors.CommunicationError as e:
            log_message(f"Error al acceder a {uri}: {e}",func=check_if_url_is_active)
            return None

class ChordNodeReference:
    def __init__(self,ip: str, port: int = 8001):
        self.id=getShaRepr(ip)
        self.port=port
        self.url=getChordNameAddress(self.id)
      
     
    def ping(self):
        """Devuelve la instancia de la clase
            Si la clase se desconecto None

        Raises:
            Exception: _description_

        Returns:
            _type_: _description_
        """
        return check_if_url_is_active(self.url) # Devuelvo el nodo de chord, None si se desconecto
        


@Pyro5.api.expose
class ChordNode:
    
    def register_url(self,object,url:str,daemon:Pyro5.server.Daemon)->bool:
        """
        Dado una instancia de un objeto , la url y el daemon de pyro lo registra

        Args:
            object (_type_): _description_
            url (_type_): _description_
            daemon (_type_): _description_
        """
    
        ns=Pyro5.api.locate_ns()
        # Registrar los objetos remotos en el servidor de no
        uri1 = daemon.register(object)
        ns.register(url, uri1)
        log_message(f'Registrada la url {url}',func=self.register_url)
        return True
    
    def check_url(self,object,url:str,time_=4):
        """
        Dado una instancia de un objecto y una url chequea que esté activa
        Si no la esta la registra
        Args:
            object (object): _description_
            url (str): _description_
            time_ (int, optional): _description_. Defaults to 4.
        """
        if not isinstance(url,str):
            raise Exception(f'La url tiene que ser un string {url} no tipo, {type(url)}')
        
        while True:
             # Iniciar el daemon
                time.sleep(time_)
                daemon = Pyro5.server.Daemon()
                try:
                    try:
                            # Intentar conectar al servidor de nombres
                            ns = Pyro5.api.locate_ns()
                            log_message(f'El servidor de nombres existe',func=self.check_url)
                            #Localizar la url
                            uri = ns.lookup(url)
                            log_message(f'La url: {url} se encuentra en el nameserver',func=self.check_url)
                    except Exception:
                            
                            try:
                                log_message(f'Mandando a registrar la url {url}',func=self.check_url)
                                self.register_url(object,url,daemon)


                            except:
                                log_message(f'No se a podido registrar el link {url}',func=self.check_url)
                                continue
                            try:
                                threading.Thread(target=daemon.requestLoop,daemon=True).start()
                                log_message(f'Corriendo hilo del demonio de pyro para el url',func=self.check_url)
                            except:
                                traceback.print_exc()
                except Exception as e:
                    log_message(f'Hubo un problema con el registro de nombres: {e} Traceback: {traceback.format_exc()}',func=self.check_url)
    
    
    
    def check_nameserver(self,time_=4):
        """Chequea que el DNS de pyro este activo

        Args:
            time_ (int, optional): _description_. Defaults to 4.
        """
        while True:
            time.sleep(time_)
            try:
                # Intentar conectar al servidor de nombres
                ns = Pyro5.api.locate_ns()
                log_message(f'El name server de pyro está activo')
            except Pyro5.errors.NamingError:
                # Si no se puede conectar, levantar el servicio
                log_message("No se encontró servidor de nombres en la red",func=self.check_nameserver)
                time.sleep(time_*2)
                subprocess.Popen(["python3", "-m", "Pyro5.nameserver"])
                log_message(f'Reiniciado el nameserver',func=self.check_nameserver)
                # Esperar un breve momento para que el servi
                time.sleep(time_)

                continue
            
    
        
            
    def update_all_chord_url(self):
        """Actualiza todas las urls activas de chrod en el NameServer
        """
        while True:
            try:
                
                # Ubicar el servidor de nombres PyroNS
                with Pyro5.api.locate_ns() as ns:
                    # Obtener todos los nombres registrados
                    nombres_registrados = ns.list()

                    # Filtrar los nombres que terminan con ".chord"
                    nombres_chord = [nombre for nombre in nombres_registrados if nombre.endswith(".chord")]
                    
                    temp=[]
                    temp_nodes=[]
                    # Chequear por cada una si esta activa
                    for url in nombres_chord:
                        node=check_if_url_is_active(url)
                        if node is None: continue
                        #Añadir los nombres 
                        temp.append(url)
                        temp_nodes.append(node)
  
                    with self._check_url_lock: # Bloquea los recursos para poder hacer todo
                        self.chord_urls= sorted(temp, key=lambda x: int(x.split('.')[0]), reverse=True) # Ahora actualizo los nombres de url
                        self.actives_chord_nodes=sorted(temp, key=lambda x: x.id, reverse=True) # Ahora actualizo los nombres de los nodos
                
            except Exception as e:
                log_message(f'Hubo un problema actualizando las urls de la chord_network {e} {traceback.format_exc()}',func=self.check_all_chord_url)

    def _check_My_Url(self):
        """Chequea constantemente que mi url esta activa
        """
        self.check_url(self,self.url,time_=4)
        
    
    def __init__(self, ip: str, port: int = 8001, m: int = 160): #m=160
        self.id = getShaRepr(ip)
        self.ip = ip
        self.port = port
        self.url=getChordNameAddress(self.id)
        self.ref:ChordNode = ChordNodeReference(self.ip,self.port)
        self.succ:ChordNode = self.ref # Initial successor is itself
        self.pred:ChordNode = None  # Initially no predecessor
        self.m = m  # Number of bits in the hash/key space
        self.finger = [self.ref] * self.m  # Finger table
        self.index_in_fingers:dict[int,set[int]]={} # Diccionario que a cada id le asigna que indices ocupa en la finger table
        self.next = 0  # Finger table index to fix next
        self.data = {}  # Dictionary to store key-value pairs
        self._key_range=(-1,self.ip) # the key_range [a,b) if a =-1 because no have predecesor
        self.cache:dict[int,str]={} #diccionario con la cache de todos los nodos de la red
        self._check_url_lock:threading.Lock =threading.RLock() # Lock para que solo un hilo pueda comprobar a la vez 
        #las urls disponibles el hilo puede entrar a varias funciones con el mismo lock al mismo tiempo
        self._chord_urls:list[str]=[]
        self._chord_nodes:list[ChordNodeReference]=[]# ESta lista contiene los nodos referencias de los nodos activos de la red
        
        
        # Threads
        threading.Thread(target=self.check_nameserver,daemon=False).start() # thread to check the name server its active always
        threading.Thread(target=self._check_My_Url,daemon=False).start() # Inicializa mi Url y ademas chequea que todo el tiempo este activa
        threading.Thread(target=self.update_all_chord_url,daemon=True).start()#Actualiza todas las urls disponibles en el NameServer
     
    @property
    def chord_urls(self):
        """"""
        with self._check_url_lock:# No dejar que 2 hilos accediendo al mismo tiempo
            return self._chord_urls
        
    @chord_urls.setter
    def chord_urls(self,value:list[str]):
        
        with self._check_url_lock:
            self._chord_urls=value
    
    
    @property
    def actives_chord_nodes(self)->list[ChordNodeReference]:
        """Retorna una lista
        con los nodos de chord activos ordenado de mayor a menor por si id

        Returns:
            _type_: _description_
        """
        with self._check_url_lock:
            return self._chord_nodes
    @actives_chord_nodes.setter
    def actives_chord_nodes(self,value:list[ChordNodeReference]):
        with self._check_url_lock:
            self._chord_nodes=value
        
    @property
    def key_range(self)->tuple[int,int]:
        """ 
        The key range of the chrod node [a,b) b is the id of this node
        
        """
        return self._key_range
        
    @property
    def succ(self):
        return self.succ
   
    @property
    def pred(self):
        return self.pred
    
    
    def ping(self)->ChordNodeReference:
        """Devuelve mi referencia, osea mi url, id, ip ...
        """
        return self.ref
    
    def _send_broadcast(self) -> bytes:
       # Enviar broadcast cada vez que sienta que mi sucesor no existe
        while True:
            log_message(f'Tratando de hacer broadcast',func=self._send_broadcast)
        #Enviar Broadcast por todas la urls menores que yo
            if self.succ.id==self.id or (self.succ.id!=self.id and self.pred is None) :
                
                    log_message(f'Voy a enviar un broadcast para buscar un sucesor ',func=self._send_broadcast)
                    try:
                           for node_ref in self.actives_chord_nodes: # Por los nodos que hay esta  ordenados de mayor a menor
                               if node_ref.id<self.id:
                                   node:'ChordNode'=node_ref.ping()
                                   if node is None:
                                       log_message(f'El nodo {node_ref.ip} no está activo ',self._send_broadcast)
                                       continue
                                   if not node.join(self.ref): # Si no me puedo unir a el 
                                       continue # Continuo hasta que uno me acepta
                                   self.notify(node_ref)# Verifico si puedo hacerlo mi predecesor
                                    
                                   
                                  
                         
                    except Exception as e:
                        log_message(f'Ocurrio un problema enviando el broadcast: {e}',level='ERROR',func=self._send_broadcast)
            time.sleep(3)
            
     # Notify method to INFOrm the node about another node
    def notify(self, node: 'ChordNodeReference'):
        """ Notify method to INFOrm the node about another node"""
        if node.id == self.id:
            pass
        if not self.pred or self._inbetween(node.id, self.pred.id, self.id):
            self.pred = node
        else:
            pass # Enviar mensaje que de no puede y le paso al que tengo como como predecesor de ese id
    
    # Method to join a Chord network using 'node' as an entry point
    def join(self, node: 'ChordNodeReference')->bool:
        """
            Method to join a Chord network using 'node' as an entry point 
            Siempre se busca un sucesor por lo tanto tengo que chequear si el nodo puede ser predecesor mio
        Args:
            node (ChordNodeReference): _description_
            True para que si que el va hacer mi sucesor 
            False a que no puede
        """
        log_message(f'El nodo {node.id} mando solicitud de unirse como predecesor',func=ChordNode.join )
        # Si 
        
        if node:
            if self.succ.id==self.id:# Es pq no tengo sucesor entonces acepto a cualquiera
                # Acepto y le digo que me haga su predecesor
                # Mi succ nuevo será el sucesor en el nuevo nodo
                self.succ=node  #node.find_successor(self.id) # Si da bateo solo quedarme con el nodo
                log_message(f'Acabo de actualizar mi sucesor al nodo {self.succ.id}',func=self.join)  
                log_message(f'Mande a notificar a mi nuevo sucesor :{self.succ.id} para que me haga su predecesor ',func=self.join)
                return True
            else: # Caso que ya tengo un sucesor
                # Le pido al nodo el sucesor mio en su anillo
                log_message(f'Entro aca la peticion del nodo {node.id}',func=self.join)
                node_succ=node  #.find_successor(self.id) # Despues añado que busque al sucesor
                if self._inbetween(node_succ.id,self.id,self.succ.id) or self.pred is None: # Es que se puede insertar pq debe estar entre yo y mi sucesor
                   self.succ=node_succ if node_succ.id<self.id else node # Actualizo mi sucesor
                   log_message(f'Acabo de actualizar mi sucesor al nodo {self.succ.id}',func=self.join)
                   return True# Notifico para que me haga su predecesor
                   #log_message(f'Mande a notificar a mi nuevo sucesor :{self.succ.id} para que me haga su predecesor ',func=self.join)
            
          
        else: # Despues eliminar esto
            self.succ = self.ref
            self.pred = None
        # Si no puedo le respondo por mis ips
        return False
        
def main():
    ip = socket.gethostbyname(socket.gethostname())
    objeto = ChordNode(ip,m=3)
    daemon = Pyro5.server.Daemon()
    uri = daemon.register(objeto)
    
    ns = Pyro5.api.locate_ns(host='127.0.0.1')
    ns.register(objeto.url, uri)
    
    print(f"Objeto1 en Server1 URI: {uri}")
    print("Servidor Pyro5 en Server1 corriendo...")
    daemon.requestLoop()

if __name__ == "__main__":
    main()
