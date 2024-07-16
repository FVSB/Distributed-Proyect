import socket
import threading
import sys
import time
import hashlib
import traceback
import logging
from helper.protocol_codes import *
from helper.logguer import log_message
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import zmq
#logger = logging.getLogger(__name__)

# Function to hash a string using SHA-1 and return its integer representation
#def getShaRepr(data: str):
#    return int(hashlib.sha1(data.encode()).hexdigest(), 16)
#

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

# Class to reference a Chord node
class ChordNodeReference:
    def __init__(self, ip: str, port: int = 8001):
        self.id = getShaRepr(ip)
        self.ip = ip
        self.port = port

    # Internal method to send data to the referenced node
    #def _send_data(self, op: int, data: str = None) -> bytes:
    #    #print(f'mandando la data con op{op} - y data {data}')
    #    #log_message(f'mandando la data con op{op} - y data {data}',func=ChordNodeReference._send_data)
    #    try:
    #        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    #            s.connect((self.ip, self.port))
    #            s.sendall(f'{op},{data}'.encode('utf-8'))
    #            new_data=s.recv(1024)
    #            # print(f'Se recibioo de data envada con opcion {op} {str(new_data)}')
    #            #log_message(f'Se recibioo de data envada con opcion {op} {str(new_data)}',func=ChordNodeReference._send_data)
    #            return new_data
    #    except Exception as e:
    #        #print(f"ERROR sending data: {e} al nodo con id {self.id} e ip {self.ip}")
    #        log_message(f"ERROR sending data: {e} al nodo con id {self.id} e ip {self.ip},Error:{str(traceback.format_exc())}",level='ERROR')
    #        #logger.info()
    #        traceback.print_exc()
    #        return b''
    
    def _send_data(self, op:int, data:str=None)->'ChordNodeReference':
        if isinstance(data,ChordNodeReference):
            data={'id':data.id,'ip':data.ip}
        #log_message(f'Typo de op {type(op)}  tipo de data{type(data)}')
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://{self.ip}:{self.port}")

        try:
            # Enviar datos al servidor
            socket.send_pyobj((op,data))

            # Esperar la respuesta del servidor
            new_data = socket.recv_pyobj()

            # Puedes descomentar estas líneas si deseas imprimir o registrar la respuesta
            # print(f'Se recibió de data enviada con opción {op}: {new_data.decode()}')
            # log_message(f'Se recibió de data enviada con opción {op}: {new_data.decode()}', func=self._send_data)

            return new_data
        
        except Exception as e:
            #print(f"ERROR sending data: {e} al nodo con id {self.id} e ip {self.ip}")
            log_message(f"ERROR sending data: {e} al nodo con id {self.id} e ip {self.ip},Error:{str(traceback.format_exc())}",level='ERROR')
            #logger.info()
            traceback.print_exc()
            return b''
        finally:
            socket.close()
            context.term()
    
    def find_successor(self, id: int) -> 'ChordNodeReference':
        """ Method to find the successor of a given id"""
        response=-1
        try:
            response = self._send_data(FIND_SUCCESSOR, id)
            response.port=self.port
            return response
        except Exception as e:
            log_message(f'Hubo un error en find_successor con response {response} de tipo {type(response)} con Error:{e}',func=self.find_successor)

    # Method to find the predecessor of a given id
    def find_predecessor(self, id: int) -> 'ChordNodeReference':
        try:
            response = self._send_data(FIND_PREDECESSOR, id)
            return response
        except Exception as e:
            log_message(f'Hubo un error en find_successor con response {response} de tipo {type(response)} con Error:{e}',func=self.find_predecessor)
            

    
    # Property to get the successor of the current node
    @property
    def succ(self) -> 'ChordNodeReference':
        response = self._send_data(GET_SUCCESSOR)
        return response

    # Property to get the predecessor of the current node
    @property
    def pred(self) -> 'ChordNodeReference':
        response = self._send_data(GET_PREDECESSOR)
        return response

    # Method to notify the current node about another node
    def notify(self, node: 'ChordNodeReference'):
        self._send_data(NOTIFY, node)

    # Method to check if the predecessor is alive
    def check_predecessor(self)->bool:
        
        response=self._send_data(CHECK_PREDECESSOR)
        
        #print(f'La respuesta de si esta vivo el nodo vivo o no es {response}')
        log_message(f'La respuesta de si esta vivo el nodo vivo o no es {response}',func=ChordNodeReference.check_predecessor,extra_data={'func':'check_predecesor from ChordReferenceNode'})
        if response in ['',' ', None,EMPTYBIT]:
            return   False
        try:
            node= response
            return node.id==self.id
        except:
            log_message(f'Hubo problemas al tratar de conocer la respuesta del nodo predecesor que el envio',level='ERROR',func=self.check_predecessor)
            return False

    # Method to find the closest preceding finger of a given id
    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        response = self._send_data(CLOSEST_PRECEDING_FINGER, str(id))
        return response

    # Method to store a key-value pair in the current node
    def store_key(self, key: str, value: str):
        self._send_data(STORE_KEY, (key,value))

    # Method to retrieve a value for a given key from the current node
    def retrieve_key(self, key: str) -> str:
        response = self._send_data(RETRIEVE_KEY, key)
        return response

    def __str__(self) -> str:
        return f'ChordNodeReference:{self.id},{self.ip},{self.port}'

    def __repr__(self) -> str:
        return str(self)


# Class representing a Chord node
class ChordNode:
    def __init__(self, ip: str, port: int = 8001, m: int = 160): #m=160
        self.id = getShaRepr(ip)
        self.ip = ip
        self.port = port
        self.ref:ChordNodeReference = ChordNodeReference(self.ip, self.port)
        self.succ:ChordNodeReference = self.ref  # Initial successor is itself
        self.pred:ChordNodeReference = None  # Initially no predecessor
        self.m = m  # Number of bits in the hash/key space
        self.finger = [self.ref] * self.m  # Finger table
        self.index_in_fingers:dict[int,set[int]]={} # Diccionario que a cada id le asigna que indices ocupa en la finger table
        self.next = 0  # Finger table index to fix next
        self.data = {}  # Dictionary to store key-value pairs

        self._key_range=(-1,self.ip) # the key_range [a,b) if a =-1 because no have predecesor
        self.cache:dict[int,str]={} #diccionario con la cache de todos los nodos de la red
        self._broadcast_lock:threading.Lock = threading.Lock()

        # Start background threads for stabilization, fixing fingers, and checking predecessor
        threading.Thread(target=self.stabilize, daemon=True).start()  # Start stabilize thread
        threading.Thread(target=self.fix_fingers, daemon=True).start()  # Start fix fingers thread
        threading.Thread(target=self.check_predecessor, daemon=True).start()  # Start check predecessor thread
        threading.Thread(target=self.start_server, daemon=True).start()  # Start server thread
        threading.Thread(target=self.show,daemon=True).start() # Start funcion que se esta printeando todo el tipo cada n segundos
        threading.Thread(target=self._send_broadcast,daemon=True,args=(JOIN,self.ref,)).start() # Enviar broadcast cuando no tengo sucesor
        threading.Thread(target=self._recive_broadcastt,daemon=True).start() # Recibir continuamente broadcast
        threading.Thread(target=self.search_test,daemon=True).start()

    @property
    def key_range(self):
        """ 
        The key range of the chrod node [a,b) b is the id of this node
        
        """
        return self._key_range
    
    
    def search_test(self):
       
            while True:
                try:
                    time.sleep(3)
                    log_message('%'*20,level='INFO')
                    for i in range(0,20):
                        with ThreadPoolExecutor() as executor:
                            future=executor.submit(self.find_succ,i) # Meterlo en el pool de hilos
                            node=future.result(timeout=10)
                            
                            log_message(f'El nodo que le pertenece el id {i} es el nodo con id {node.id}')
                    log_message('+'*20,level='INFO')
                except Exception as e:
                    log_message(f'Error buscando informacion {e}',self.search_test)
        
            
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
            
    def _send_broadcast(self, op: int, data: str = None) -> bytes:
       # Enviar broadcast cada vez que sienta que mi sucesor no existe
        while True:
            log_message(f'Tratando de hacer broadcast',func=self._send_broadcast)
        #Enviar broadcast para descubrir mi nuevo sucesor 
            if self.succ.id==self.id or (self.succ.id!=self.id and self.pred is None) :
                #with self._broadcast_lock:
                    log_message(f'Voy a enviar un broadcast para buscar un sucesor ',func=self._send_broadcast)
                    try:
                        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                        s.sendto(f'{op}-{str(data)}'.encode(), (str(socket.INADDR_BROADCAST), self.port))
                        log_message(f'Enviado el broadcast',func=self._send_broadcast) 
                        s.close()
                         
                    except Exception as e:
                        log_message(f'Ocurrio un problema enviando el broadcast: {e}',level='ERROR',func=self._send_broadcast)
            time.sleep(3)
       
    def _recive_broadcastt(self):
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            client_socket.bind(('', self.port))  # Escucha en todas las interfaces en el puerto 8001
            client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            # Habilitar el uso compartido del puerto
            #client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # Configurar un tiempo de espera
            #client_socket.settimeout(0.2)  # Tiempo de espera de 200 ms
            
            while True:
                try:
                #with self._broadcast_lock:
                    log_message('Esperando Broadcast',func=self._recive_broadcastt)
                    message, address = client_socket.recvfrom(1024)
                    log_message(f'El tipo de lo recibido en broadcast es {type(message)} ,  {type(address)}')
                    log_message(f"Mensaje recibido desde {address}: {message.decode()}",func=self._recive_broadcastt)
                    if address[0]!=socket.gethostbyname(socket.gethostname()):
                        log_message('Se recibio desde otro nodo',func=self._recive_broadcastt)
                        message=str(message.decode())
                        log_message(f'Este ahora es el mensaje {message} que tiene un tipo {type(message)}',func=self._recive_broadcastt)
                        op,node=message.split('-')
                        op=int(op)
                        log_message(f'LLego la respuesta con opcion {op} de tipo {type(op)}')
                        if int(op)==JOIN:
                            log_message(f'El mensaje recibido al broadcast era para join desde el ip {address[0]} de tipo {type(address[0])}')
                            self.join(ChordNodeReference(address[0],self.port))
                    time.sleep(3)
                except:
                    log_message(f'Error en el while True del recv broadcast Error: {traceback.format_exc()}',self._recive_broadcastt)
        except Exception as e:
            log_message(f'Error recibiendo broadcast {e}, Error: {traceback.format_exc()}',func=self._recive_broadcastt,level='ERROR')
        
        
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

      # Method to find the successor of a given id
    def find_succ(self, id: int) -> 'ChordNodeReference':
        """Method to find the successor of a given id

        Args:
            id (int): _description_

        Returns:
            ChordNodeReference: _description_
        """
        node = self.find_pred(id)  # Find predecessor of id
        if node.id!= self.id:
            return node.succ
        else:
            return self.succ
        
            
           

    # Method to find the predecessor of a given id
    def find_pred(self, id: int) -> 'ChordNodeReference':
        """Method to find the predecessor of a given id

        Args:
            id (int): _description_

        Returns:
            ChordNodeReference: _description_
        """
        node:ChordNodeReference = self
        # Comprobar que el sucesor esta vivo, sino se comprueba por 
        while not self._inbetween(id, node.id, node.succ.id):
            
            if node.id==self.id:
                node=self.closest_preceding_finger(id)
            else:
                node = node.closest_preceding_finger(id)
            
        
        return node

    # Method to find the closest preceding finger of a given id
    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        """Method to find the closest preceding finger of a given id

        Args:
            id (int): _description_

        Returns:
            ChordNodeReference: _description_
        """
        
        for i in range(self.m - 1, -1, -1):
            if self.finger[i] and self._inbetween(self.finger[i].id, self.id, id):
                return self.finger[i]
        #log_message('El más cercano soy yo',func=ChordNode.closest_preceding_finger)
        return self.ref

    # Method to join a Chord network using 'node' as an entry point
    def join(self, node: 'ChordNodeReference'):
        """
            Method to join a Chord network using 'node' as an entry point 
            Siempre se busca un sucesor por lo tanto tengo que chequear si el nodo puede ser predecesor mio
        Args:
            node (ChordNodeReference): _description_
        """
        log_message(f'El nodo {node.id} mando solicitud de unirse como predecesor',func=ChordNode.join )
        # Si 
        
        if node:
            if self.succ.id==self.id:# Es pq no tengo sucesor entonces acepto a cualquiera
                # Acepto y le digo que me haga su predecesor
                # Mi succ nuevo será el sucesor en el nuevo nodo
                self.succ=node  #node.find_successor(self.id) # Si da bateo solo quedarme con el nodo
                log_message(f'Acabo de actualizar mi sucesor al nodo {self.succ.id}',func=self.join)
                self.succ.notify(self.ref)
                log_message(f'Mande a notificar a mi nuevo sucesor :{self.succ.id} para que me haga su predecesor ',func=self.join)
            else: # Caso que ya tengo un sucesor
                # Le pido al nodo el sucesor mio en su anillo
                log_message(f'Entro aca la peticion del nodo {node.id}',func=self.join)
                node_succ=node  #.find_successor(self.id) # Despues añado que busque al sucesor
                if self._inbetween(node_succ.id,self.id,self.succ.id) or self.pred is None: # Es que se puede insertar pq debe estar entre yo y mi sucesor
                   self.succ=node_succ if node_succ.id<self.id else node # Actualizo mi sucesor
                   log_message(f'Acabo de actualizar mi sucesor al nodo {self.succ.id}',func=self.join)
                   self.succ.notify(self.ref)# Notifico para que me haga su predecesor
                   log_message(f'Mande a notificar a mi nuevo sucesor :{self.succ.id} para que me haga su predecesor ',func=self.join)
                
          
        else: # Despues eliminar esto
            self.succ = self.ref
            self.pred = None
        # Si no puedo le respondo por mis ips

    # Stabilize method to periodically verify and update the successor and predecessor
    def stabilize(self):
        """Stabilize method to periodically verify and update the successor and predecessor
        """
        Is=False
        node=None
        count_failed=0 # Cant de veces que se a tratado de establecer conexion  sin exito con el predecesor del sucesor
        x=None
        while True:
            try:
                Is=False
                if self.succ.id != self.id: # Es pq tengo sucesor
                    log_message('stabilize',func=ChordNode.stabilize)
                    if self.succ is None: self.succ=self.ref
                    try: 
                        x = self.succ.pred # Aca seria preguntar por quien es el sucesor de mi id
                    except: # Esto es que no responde el sucesor 
                        count_failed+=1
                        
                        if count_failed>3:
                            # Trata de contactar con el succ y preguntarle por el predecesor si pasa algun problema en la peticion
                            # Mi sucesor soy yo
                            #Si mi sucesor era mi antecesor entonces hago el antecesor null
                            #if self.succ.id==self.pred.id: self.pred=None
                            time.sleep(20)
                            log_message('Seleccionando_Nuevo_Sucesor',func=ChordNode.stabilize)
                            nearest_node=self.find_succ(self.id+1)
                            log_message(f'EL nodo que tiene {nearest_node.id} el mas cercano',func=ChordNode.stabilize)
                            self.succ=nearest_node if nearest_node.id!=self.succ.id else self.ref
                            continue
                       
                    log_message(f' Este es X {x}',func=ChordNode.stabilize)
                    if x and x.id != self.id:
                        log_message(f'Otra vez x {x}',func=ChordNode.stabilize)
                        if x and self._inbetween(x.id, self.id, self.succ.id):
                            self.succ = x
                        log_message('A Notificar',func=ChordNode.stabilize)
                        try:
                            self.succ.notify(self.ref)
                        except Exception as e: # Si no se puede conumicar con el nuevo sucesor caso que sea que era sucesor y predecesor
                            # y se desconecto pues poner al sucesor como yo mismo
                            log_message(f' Fallo comunicarse con el nuevo sucesor {e}',func=ChordNode.stabilize,level='ERROR')
                            self.succ=self.ref
        
                    
            except Exception as e:
                
                
                log_message(f"  Is_True:{Is}_  node:{node}_  _::: ERROR in stabilize: {e}",func=ChordNode.stabilize,level='ERROR')
                traceback.print_exc() 

            log_message(f"successor : {self.succ} predecessor {self.pred}",func=ChordNode.stabilize)
            time.sleep(3) #Poner en produccion en 1 segundo

    # Notify method to INFOrm the node about another node
    def notify(self, node: 'ChordNodeReference'):
        """ Notify method to INFOrm the node about another node"""
        if node.id == self.id:
            pass
        if not self.pred or self._inbetween(node.id, self.pred.id, self.id):
            self.pred = node
        else:
            pass # Enviar mensaje que de no puede y le paso al que tengo como como predecesor de ese id
    def _delete_from_all_ocurrencies(self,id_node:int,new_node:ChordNodeReference):
        """_summary_

        Args:
            id_node (int): ID del nodo a eliminar
            new_node (ChordNodeReference): NOdo a insertar

        Raises:
            Exception: _description_
        """
        log_message(f'La finger table antes {self.finger}',func=self._delete_from_all_ocurrencies)
        lis_to_change=self.index_in_fingers[id_node] # cambiar pir mi todas las  apararicones d ela finger table
        log_message(f'La lista de ocurrencias es {lis_to_change}',func=self._delete_from_all_ocurrencies)
        for index in lis_to_change:
            temp= self.finger[index]
            if temp.id!=id_node:
               raise Exception(f'Para poder quitar de la finger table tiene que coincidir el que esta en la table {temp.id} en el index {index} con {old.id}')
            self.finger[index]=new_node
                            # ELiminar la llave del diccionario
        del self.index_in_fingers[id_node]
        log_message(f'Se elimino de la lista de fungers_index el nodo {id_node} por el nodo {new_node.id}',func=self._delete_from_all_ocurrencies)
        log_message(f'la nueva finger table {self.finger}',func=self._delete_from_all_ocurrencies)
    # Fix fingers method to periodically update the finger table
    def fix_fingers(self):
        """Fix fingers method to periodically update the finger table
        """
        while True:
            log_message(f'La finger table {self.finger}',func=self.fix_fingers)
            try:
                self.next += 1
                if self.next >= self.m:
                    self.next = 0
                a=self.find_succ((self.id + 2 ** self.next) % 2 ** self.m)
                ok=False
                for _ in range(3):
                    if a.check_predecessor(): # Chequear que el nodo esta vivo
                        ok=True
                        break
                    else: 
                        log_message(f'El nodo {a.id} no se encuentra en fix fingers',func=ChordNode.fix_fingers)
                    time.sleep(0.5)
               # a=a.succ if not ok else a
                if not ok:
                    log_message(f'El nodo {a.id} se desconecto de la red',func=ChordNode.fix_fingers)
                    try:
                        i=self.next+1
                        k=i if self.next<self.m else 0
                        change=False
                        while i!=self.next:
                            q=self.closest_preceding_finger(i) # Buscar por los antecesores 
                            log_message(f'El nodo q ahora es {q.id} con i = {i}',func=self.fix_fingers)
                            if q.id!=a.id: # Encontrar el primero que sea distinto
                                self._delete_from_all_ocurrencies(a.id,q) #ELiminar todas las ocurriencias de id y poner las de q
                                a=q
                                log_message(f'Se va a parar aca el while con q ={q.id}',func=self.fix_fingers)
                                change=True
                                break
                            i= i+1 if i<self.m else 0
                
                        
                        if not change  :# Entonces lo cambio por mi en la finger table
                           
                            log_message(f'No se pudo encontrar un nuevo sucesor para el nodo {a.id} por lo tanto yo sere el sucesor',func=ChordNode.fix_fingers)
                            self._delete_from_all_ocurrencies(a.id,self.ref) #Eliminar las referencias de la finger table y ponerme a mi como sucesor de ese id
                            
                            a=self.ref # Despues ver si se puede llamar al nodo '0' para que sea el
                            
                        log_message(f'El nodo a ahora es {a}',func=ChordNode.fix_fingers)
                    except:
                        log_message(f'Fallo buscar el sucesor del prececesor del nodo {a.id+1} Error:{traceback.format_exc()} ',func=self.fix_fingers)
                

                self.finger[self.next] = a
                # Añadir al diccionario 
                self.index_in_fingers.setdefault(a.id,set([self.next]))
                self.index_in_fingers[a.id].add(self.next)
                
            except Exception as e:
                log_message(f"ERROR in fix_fingers: {e} Error:{traceback.format_exc()}",func=ChordNode.fix_fingers,level='ERROR')
            time.sleep(3) # 10

    # Check predecessor method to periodically verify if the predecessor is alive
    def check_predecessor(self):
        """Check predecessor method to periodically verify if the predecessor is alive
        """
        counter=0
        while True:
            try:
                if self.pred:
                    if not  self.pred.check_predecessor() or self.pred.succ.id!=self.id:# Saber si esta vivo y su sucesor soy yo
                        counter+=1
                        if counter>3:
                            log_message(f'No se restablecio conexion con el predecesor {self.pred} ',func=ChordNode.check_predecessor,level='ERROR')
                            raise Exception(f'No se restablecio conexion con el predecesor {self.pred} ')
                    else:
                        counter=0
                        log_message(f'El predecesor de mi sucesor es { self.pred.succ.id}',func=self.check_predecessor)
                                          
                    log_message(f'Chequeando predecesor ...',func=ChordNode.check_predecessor)
            except Exception as e:
                log_message(f'Se desconecto el predecesor con id {self.pred.id} e ip {self.pred.ip},error:{e}',func=ChordNode.check_predecessor,level='ERROR')
                traceback.print_exc()
                self.pred=None # Hago mi predecesor en None
                
            time.sleep(3)

    def store_key(self,key:int,value)->ChordNodeReference:
        node=self.find_succ(key)
        if node.id==self.id:
            self.data.setdefault(key,value)
            return self.ref
        else:
            return  node.store_key(key,value)
        
        
    def start_server(self):
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind(f"tcp://{self.ip}:{self.port}")
        while True:
            try:
                message = socket.recv_pyobj()
                data = message
                option = int(data[0])
                #log_message(f'LLego un mensaje con opcion {option}',func=self.start_server)
                a=data[1]
                if isinstance(a,dict):
                    data=(data[0],ChordNodeReference(a['ip']))
                    
                data_resp = None

                if option == FIND_SUCCESSOR:
                    id = int(data[1])
                    data_resp = self.find_succ(id)
                elif option == FIND_PREDECESSOR:
                    id = int(data[1])
                   # log_message(f'Me llego una peticion de buscar el predecesor de {id}',func=self.start_server)
                    data_resp = self.find_pred(id)
                elif option == GET_SUCCESSOR:
                    data_resp = self.succ if self.succ else self.ref
                elif option == GET_PREDECESSOR:
                    data_resp = self.pred if self.pred else self.ref
                elif option == NOTIFY:
                    #id = int(data[1])
                    #ip = data[2]
                    
                    node:ChordNodeReference=data[1]
                    #log_message(f'LLegado al notify {node}',func=self.start_server)
                    id=node.id
                    ip=node.ip
                    #log_message(f'Llego una notificacion del ip:{ip}',func=self.start_server)
                    self.notify(ChordNodeReference(ip, self.port))
                elif option == CHECK_PREDECESSOR:
                    data_resp = self.ref
                elif option == CLOSEST_PRECEDING_FINGER:
                    id = int(data[1])
                    data_resp = self.closest_preceding_finger(id)
                elif option == JOIN:
                    node:ChordNodeReference=data[1]
                    ip = node.ip
                    #log_message(f'Recibido la peticion de JOIN desde ip {ip} con id: {getShaRepr(ip)} ',func=self.start_server)
                    self.join(node)
                elif option == STORE_KEY:
                     response=self.store_key()

                if data_resp:
                    response = data_resp
                    socket.send_pyobj(response)
                else:
                    socket.send_pyobj('')
            except Exception as e:
                log_message(f'Error en start server {e}  {traceback.format_exc()}',func=self.start_server)
            
        socket.close()
        context.term()
            
    # Start server method to handle incoming requests
    #ef start_server(self):
    #   with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    #       s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #       s.bind((self.ip, self.port))
    #       s.listen(10)

    #       while True:
    #           conn, addr = s.accept()
    #           #log_message(f'new connection from {addr}',func=ChordNode.start_server)

    #           data = conn.recv(1024).decode().split(',')

    #           data_resp = None
    #           option = int(data[0])

    #           if option == FIND_SUCCESSOR:
    #               id = int(data[1])
    #               data_resp = self.find_succ(id)
    #           elif option == FIND_PREDECESSOR:
    #              
    #               id = int(data[1])
    #               log_message(f'Me llego una peticion de buscar el predecesor de {id}',func=ChordNode.start_server)
    #               data_resp = self.find_pred(id)
    #           elif option == GET_SUCCESSOR:
    #               data_resp = self.succ if self.succ else self.ref
    #           elif option == GET_PREDECESSOR:
    #               data_resp = self.pred if self.pred else self.ref
    #           elif option == NOTIFY:
    #               id = int(data[1])
    #               ip = data[2]
    #               log_message(f'Llego una notificacion del ip:{ip}',func=ChordNode.start_server)
    #               self.notify(ChordNodeReference(ip, self.port))
    #           elif option == CHECK_PREDECESSOR:
    #              
    #               data_resp=self.ref
    #           elif option == CLOSEST_PRECEDING_FINGER:
    #               id = int(data[1])
    #               data_resp = self.closest_preceding_finger(id)
    #           elif option == JOIN:
    #               ip = data[2]
    #               log_message(f'Recibido la peticion de JOIN desde ip {ip} con id: {getShaRepr(ip)} ')
    #               self.join(ChordNodeReference(ip, self.port))
    #           elif option == STORE_KEY:
    #               pass
    #         
    #           if data_resp:
    #               response = f'{data_resp.id},{data_resp.ip}'.encode()
    #               conn.sendall(response)
    #           conn.close()

if __name__ == "__main__":
    print("Hello dht")
    #time.sleep(10)
    ip = socket.gethostbyname(socket.gethostname())
    node = ChordNode(ip,m=3)

    if len(sys.argv) >= 2:
        other_ip = sys.argv[1]
        #node.join(ChordNodeReference(other_ip, node.port))
    
    while True:
        pass
