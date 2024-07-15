import Pyro5.api
import socket
import threading
import sys
import time
import hashlib
import traceback
import subprocess
import logging
from helper.logguer import log_message
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import utils
import uuid
import copy




def get_remote_objet(url:str):
    """Devuelve el objeto remoto dada una url
        Puede lanzar excep si no la url se desconecto
    Args:
        url (str): _description_
    """
    ns = Pyro5.api.locate_ns()
    uri = ns.lookup(url)
    return  Pyro5.api.Proxy(uri)
    
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
            proxy:ChordNode = Pyro5.api.Proxy(uri)
            # Intentar llamar a un método que confirme la conexión o estado del servidor
            # Puedes usar cualquier método definido en el objeto remoto para confirmar la conexión
            node:ChordNodeReference = proxy.ping()  # Suponiendo que el objeto remoto tiene un método ping() para verificar la conexión
            node=ChordNodeReference(node.ip,node.port)
            log_message(f'El nodo de referencia de la url {url} es {node} de tipo {type(node)}',func=check_if_url_is_active)
            log_message(f'El nodo tiene al atributo id de tipo {type(node.id)}',func=check_if_url_is_active)
            if node is not None:  # Puedes definir el criterio para confirmar que el servidor está activo
                return node
            return None
        except Exception as e:
            log_message(f"Error al acceder a {uri}, {traceback.print_exc()}: {e}",func=check_if_url_is_active)
            return None
        
def get_guid():
    # Generar dos GUIDs
    guid1 = uuid.uuid4()
    return str(guid1)



def clone_object(obj):
    return copy.deepcopy(obj)


def objects_to_send(object,daemon):
        
       
        
        copy_ref=clone_object(object)
        random_url=f'{get_guid()}.{get_guid()}'
        register_url(copy_ref,random_url,daemon)
        return copy_ref
    
def register_url(object,url:str,daemon:Pyro5.server.Daemon)->bool:
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
    log_message(f'Registrada la url {url}',func=register_url)
    return True

@Pyro5.api.expose
class ChordNodeReference:
    def __init__(self,ip: str, port: int = 8001):
        self.id_=getShaRepr(ip)
        self.ip_=ip
        self.port_=port
        self.url_=getChordNameAddress(self.id)
      
     
    
    @property 
    def id(self):
        return self.id_
    
    @id.setter
    def id(self,value):
        self.id_=value
    
    @property
    def ip(self):
        return self.ip_
    
    @ip.setter
    def ip(self,value):
        self.ip_=value
    @property
    def port(self):
        return self.port_
    
    @port.setter
    def port(self,value):
        self.port_=value
    
    @property 
    def url(self):
        return self.url_
    
    @url.setter
    def url(self,value):
        self.url=value
        
    
    def ping(self):
        """Devuelve la instancia de la clase
            Si la clase se desconecto None

        Raises:
            Exception: _description_

        Returns:
            _type_: _description_
        """
        log_message(f'Tratando de recuperar el nodo {self.id}',func=self.ping)
        try:
            remote_obj= get_remote_objet(self.url)# Devuelvo el nodo de chord, None si se desconecto
            return remote_obj
        except Exception as e:
            log_message(f'Error en ping de ChordNodeRefrecne  {traceback.format_exc()}',func=self.ping)
            raise Exception('Error en ping de chornodereference')
       
        


@Pyro5.api.expose
class ChordNode:
    
    
    
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
                # #host="0.0.0.0"
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
                                register_url(object,url,self.daemon)


                            except:
                                log_message(f'No se a podido registrar el link {url}',func=self.check_url)
                                continue
                            try:
                                threading.Thread(target=self.daemon.requestLoop,daemon=True).start()
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
                subprocess.Popen(["python3", "-m", "Pyro5.nameserver",'--host','0.0.0.0'])
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
                    log_message(f'Los nombres que hay hasta ahora {nombres_chord}')
                    temp=[]
                    temp_nodes=[]
                    # Chequear por cada una si esta activa
                    for url in nombres_chord:
                        node=check_if_url_is_active(url)
                        if node  is None or  url==self.url: continue
                        #Añadir los nombres 
                        temp.append(url)
                        temp_nodes.append(node)
                    
                    sorted_urls=sorted(temp, key=lambda x: int(x.split('.')[0]), reverse=True) # Ahora actualizo los nombres de url
                    sorted_nodes=sorted(temp_nodes, key=lambda x: x.id, reverse=True) # Ahora actualizo los nombres de los nodos
                    with self._check_url_lock: # Bloquea los recursos para poder hacer todo
                        self.chord_urls=sorted_urls
                        self.actives_chord_nodes=sorted_nodes
                        log_message(f'Se actualizó la lista de mensajes {self.actives_chord_nodes}',func=self.update_all_chord_url)
            except Exception as e:
                log_message(f'Hubo un problema actualizando las urls de la chord_network {e} {traceback.format_exc()}',func=self.update_all_chord_url)

    def _check_My_Url(self):
        """Chequea constantemente que mi url esta activa
        """
        self.check_url(self,self.url,time_=4)
        
    
    def __init__(self, ip: str, port: int = 8001, m: int = 160): #m=160
        self.id_ = getShaRepr(ip)
        self.ip_ = ip
        self.port_ = port
        self.url_=getChordNameAddress(self.id)
        self.ref:ChordNodeReference = ChordNodeReference(self.ip,self.port)
        self.succ_:ChordNodeReference = self.ref # Initial successor is itself
        self.pred_:ChordNodeReference = None  # Initially no predecessor
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
        self.daemon = Pyro5.server.Daemon(self.ip)
        
        # Threads
        threading.Thread(target=self.check_nameserver,daemon=False).start() # thread to check the name server its active always
        threading.Thread(target=self._check_My_Url,daemon=False).start() # Inicializa mi Url y ademas chequea que todo el tiempo este activa
        threading.Thread(target=self.update_all_chord_url,daemon=True).start()#Actualiza todas las urls disponibles en el NameServer
        threading.Thread(target=self.show,daemon=True).start() # Start funcion que se esta printeando todo el tipo cada n segundos
        threading.Thread(target=self._send_broadcast,daemon=True).start()
    
    
    @property
    def id(self):
        return self.id_
    @property
    def ip(self):
        return self.ip_
    @property
    def port(self):
        return self.port_
    
    @property
    def url(self):
        return self.url_
    @property
    def succ(self):
        return self.succ_
   
    @succ.setter
    def succ(self,value:ChordNodeReference):
        self.succ_=value
    @property
    def pred(self):
        return self.pred_
    @pred.setter
    def pred(self,value):
        self.pred_=value
    
    
    
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
        
    
    def show(self):
        """
        Show my ip and id and mi predecessor and succesors ips and ids
        """
        print(f'ENtro en print')
        """Printea quien soy yo"""
        while True:
            log_message('-'*20,level='INFO')
            log_message(f'Mi predecesor es {self.pred.id if self.pred else None} con ip {self.pred.ip if self.pred else None} ',level='INFO')
            log_message(f'Yo soy id:{self.id},con ip:{self.ip} ',level='INFO')
            if self.succ.id == self.id:
                if self.succ.ip !=self.ip:
                    log_message('El sucesor tiene igual ID pero no tiene igual ip',level='INFO')
                log_message(f'Todavia no tengo sucesor',level='INFO'),
            else:
                log_message(f'Mi sucesor es {self.succ.id if self.succ else None} con ip {self.succ.ip  if self.succ else None}',level='INFO')
            log_message('*'*20,level='INFO')
            
            
            time.sleep(3) # Se presenta cada 10 segundos
    
    
    def ping(self)->ChordNodeReference:
        """Devuelve mi referencia, osea mi url, id, ip ...
        """
        
        # Mandar a resializar el objeto
        copy_ref=objects_to_send(self.ref,self.daemon)
        return copy_ref
    
    def _send_broadcast(self) -> bytes:
       # Enviar broadcast cada vez que sienta que mi sucesor no existe
        daemon= Pyro5.server.Daemon(self.ip)# Crear un nuevo demonio para este hilo
        while True:
            log_message(f'Tratando de hacer broadcast',func=self._send_broadcast)
        #Enviar Broadcast por todas la urls menores que yo
            if self.succ.id==self.id or (self.succ.id!=self.id and self.pred is None) :
                
                    log_message(f'Voy a enviar un broadcast para buscar un sucesor ',func=self._send_broadcast)
                    try:
                           active_nodes:list[ChordNodeReference]=clone_object(self.actives_chord_nodes)
                           for node_ref in active_nodes: # Por los nodos que hay esta  ordenados de mayor a menor
                               log_message(f'Analizando al nodo {node_ref.id} el tipo del {type(node_ref)}',func=self._send_broadcast)
                               if node_ref.id>self.id:
                                   node:'ChordNode'=node_ref.ping()
                                   log_message(f'El nodo {node.id} despues de hacer ping esta vivo',func=self._send_broadcast)
                                   if node is None:
                                       log_message(f'El nodo {node_ref.ip} no está activo ',func=self._send_broadcast)
                                       continue
                                   log_message(f'Entrando al Join con el nodo de typo {type(node_ref)}',func=self._send_broadcast)
                                   if not self.join(node_ref): # Si no me puedo unir a el  y primero hago que el objeto sea enviable por pyro
                                       log_message(f'El nodo con id {node_ref.id} no fue aceptado como succesor ',func=self._send_broadcast)
                                       continue # Continuo hasta que uno me acepta
                                   log_message(f'Decirle al nodo {node_ref.id} que me haga su predecesor',func=self._send_broadcast)
                                   to_ask_node=get_remote_objet(node_ref.url)# Nodo a preguntar si le cuadra ser mi sucesor
                                   log_message(f'Recuperado el objeto remoto con id {to_ask_node.id} ',func=self._send_broadcast)
                                   my_ref_to_send=objects_to_send(self.ref,daemon)# REferencia mia para enviar al nodo
                                   
                                   resp=to_ask_node.notify(my_ref_to_send)# Verifico si puedo hacerlo mi predecesor
                                   log_message(f'Se le envio al nodo {node_ref.id} que yo sea su predecesor y obtuve de respuesta {resp}',func=self._send_broadcast)  
                                     
                                   
                                  
                         
                    except Exception as e:
                        log_message(f'Ocurrio un problema enviando el broadcast: {e} {traceback.format_exc()}',level='ERROR',func=self._send_broadcast)
            time.sleep(3)
            
     # Notify method to INFOrm the node about another node
    def notify(self, node: 'ChordNodeReference')->bool:
        """ Notify method to INFOrm the node about another node"""
        log_message(f'El nodo {node.id} quiere que lo haga mi predecesor',func=self.notify )
        if node.id == self.id:
            return False
        if not self.pred or self._inbetween(node.id, self.pred.id, self.id):
            self.pred = node
            return True
        else:
            return False
            # Enviar mensaje que de no puede y le paso al que tengo como como predecesor de ese id
    
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
    
    
    # Helper method to check if a value is in the range (start, end]
    def _inbetween(self, k: int, start: int, end: int) -> bool:
        """Helper method to check if a value is in the range (start, end]

        Args:
            k (int): _description_
            start (int): _description_
            end (int): _description_

        Returns:
            bool: _description_
        """
        if start < end:
            return start < k <= end
        else:  # The interval wraps around 0
            return start < k or k <= end

        
def main():
    ip = socket.gethostbyname(socket.gethostname())
    objeto = ChordNode(ip,m=3)
    #daemon = Pyro5.server.Daemon()
    #uri = daemon.register(objeto)
    #
    #ns = Pyro5.api.locate_ns(host='127.0.0.1')
    #ns.register(objeto.url, uri)
    #
    #print(f"Objeto1 en Server1 URI: {uri}")
    #print("Servidor Pyro5 en Server1 corriendo...")
    #daemon.requestLoop()

if __name__ == "__main__":
    main()
