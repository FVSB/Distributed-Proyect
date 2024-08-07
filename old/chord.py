import socket
import threading
import sys
import time
import hashlib
import traceback
import logging
from protocol_codes import *
from logguer import log_message

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
    def _send_data(self, op: int, data: str = None) -> bytes:
        #print(f'mandando la data con op{op} - y data {data}')
        log_message(f'mandando la data con op{op} - y data {data}',func=ChordNodeReference._send_data)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.ip, self.port))
                s.sendall(f'{op},{data}'.encode('utf-8'))
                new_data=s.recv(1024)
                # print(f'Se recibioo de data envada con opcion {op} {str(new_data)}')
                log_message(f'Se recibioo de data envada con opcion {op} {str(new_data)}',func=ChordNodeReference._send_data)
                return new_data
        except Exception as e:
            #print(f"ERROR sending data: {e} al nodo con id {self.id} e ip {self.ip}")
            log_message(f"ERROR sending data: {e} al nodo con id {self.id} e ip {self.ip}",level='ERROR')
            #logger.info()
            return b''
    
    def find_successor(self, id: int) -> 'ChordNodeReference':
        """ Method to find the successor of a given id"""
        response = self._send_data(FIND_SUCCESSOR, str(id)).decode().split(',')
        return ChordNodeReference(response[1], self.port)

    # Method to find the predecessor of a given id
    def find_predecessor(self, id: int) -> 'ChordNodeReference':
        response = self._send_data(FIND_PREDECESSOR, str(id)).decode().split(',')
        return ChordNodeReference(response[1], self.port)

    
    # Property to get the successor of the current node
    @property
    def succ(self) -> 'ChordNodeReference':
        response = self._send_data(GET_SUCCESSOR).decode().split(',')
        return ChordNodeReference(response[1], self.port)

    # Property to get the predecessor of the current node
    @property
    def pred(self) -> 'ChordNodeReference':
        response = self._send_data(GET_PREDECESSOR).decode().split(',')
        return ChordNodeReference(response[1], self.port)

    # Method to notify the current node about another node
    def notify(self, node: 'ChordNodeReference'):
        self._send_data(NOTIFY, f'{node.id},{node.ip}')

    # Method to check if the predecessor is alive
    def check_predecessor(self)->bool:
        
        response=self._send_data(CHECK_PREDECESSOR)
        #print(f'La respuesta de si esta vivo el nodo vivo o no es {response}')
        log_message(f'La respuesta de si esta vivo el nodo vivo o no es {response}',func=ChordNodeReference.check_predecessor,extra_data={'func':'check_predecesor from ChordReferenceNode'})
        if response in ['',' ', None,EMPTYBIT]:
            return   False
        return True


    # Method to find the closest preceding finger of a given id
    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        response = self._send_data(CLOSEST_PRECEDING_FINGER, str(id)).decode().split(',')
        return ChordNodeReference(response[1], self.port)

    # Method to store a key-value pair in the current node
    def store_key(self, key: str, value: str):
        self._send_data(STORE_KEY, f'{key},{value}')

    # Method to retrieve a value for a given key from the current node
    def retrieve_key(self, key: str) -> str:
        response = self._send_data(RETRIEVE_KEY, key).decode()
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
        self.next = 0  # Finger table index to fix next
        self.data = {}  # Dictionary to store key-value pairs
        self._key_range=(-1,self.ip) # the key_range [a,b) if a =-1 because no have predecesor
        self.cache:dict[int,str]={} #diccionario con la cache de todos los nodos de la red

        # Start background threads for stabilization, fixing fingers, and checking predecessor
        threading.Thread(target=self.stabilize, daemon=True).start()  # Start stabilize thread
        threading.Thread(target=self.fix_fingers, daemon=True).start()  # Start fix fingers thread
        threading.Thread(target=self.check_predecessor, daemon=True).start()  # Start check predecessor thread
        threading.Thread(target=self.start_server, daemon=True).start()  # Start server thread
        threading.Thread(target=self.show,daemon=True).start() # Start funcion que se esta printeando todo el tipo cada n segundos

    @property
    def key_range(self):
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
        
        return node.succ
        
            
           

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
            node = node.closest_preceding_finger(id)
            
        #print(f'El nodo a retornar tiene id {node.id } a buscar la llave {id}')
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
        log_message('El más cercano soy yo',func=ChordNode.closest_preceding_finger,extra_data={'func':'closest_preceding_finger'})
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
                self.succ.notify(self.id)
                log_message(f'Mande a notificar a mi nuevo sucesor :{self.succ.id} para que me haga su predecesor ',func=self.join)
            else: # Caso que ya tengo un sucesor
                # Le pido al nodo el sucesor mio en su anillo
                node_succ=node   #node.find_successor(self.id) # Despues añado que busque al sucesor
                if self._inbetween(node_succ.id,self.id,self.succ.id): # Es que se puede insertar pq debe estar entre yo y mi sucesor
                   self.succ=node_succ # Actualizo mi sucesor
                   log_message(f'Acabo de actualizar mi sucesor al nodo {self.succ.id}',func=self.join)
                   self.succ.notify(self.id)# Notifico para que me haga su predecesor
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
                            self.succ=self.ref
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

    # Fix fingers method to periodically update the finger table
    def fix_fingers(self):
        """Fix fingers method to periodically update the finger table
        """
        while True:
            try:
                self.next += 1
                if self.next >= self.m:
                    self.next = 0
                a=self.find_succ((self.id + 2 ** self.next) % 2 ** self.m)
                if a.id<self.id:
                    log_message(f'Si logro encontrar un nodo menor {a.id},en el indice {self.next}, {self.finger[1]} ',ChordNode.fix_fingers,level='ERROR')
                    raise Exception(f'Si logro encontrar un nodo menor {a.id},en el indice {self.next}, {self.finger[1]} ')
                   
                ok=False
                for _ in range(3):
                    if a.check_predecessor(): # Chequear que el nodo esta vivo
                        ok=True
                        break
                    else: 
                        log_message(f'El nodo {a.id} no se encuentra en fix fingers')
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
                            log_message(f'El nodo q ahora es {q.id} con i = {i}')
                            if q.id!=a.id: # Encontrar el prinero que sea distinto
                                a=q
                                log_message(f'Se va a parar aca el while con q ={q.id}')
                                change=True
                                break
                            i= i+1 if i<self.m else 0
                        
                        if not change  :
                            log_message(f'No se pudo encontrar un nuevo sucesor para el nodo {a.id} por lo tanto yo sere el sucesor',func=ChordNode.fix_fingers)
                            a=self.ref # Despues ver si se puede llamar al nodo '0' para que sea el
                        log_message(f'El nodo a ahora es {a}',func=ChordNode.fix_fingers)
                    except:
                        log_message(f'Fallo buscar el sucesor del prececesor del nodo {a.id+1}',)
                

                self.finger[self.next] = a
            except Exception as e:
                log_message(f"ERROR in fix_fingers: {e}",func=ChordNode.fix_fingers,level='ERROR')
            time.sleep(3) # 10

    # Check predecessor method to periodically verify if the predecessor is alive
    def check_predecessor(self):
        """Check predecessor method to periodically verify if the predecessor is alive
        """
        counter=0
        while True:
            try:
                if self.pred:
                    if not  self.pred.check_predecessor():
                        counter+=1
                        if counter>3:
                            log_message(f'No se restablecio conexion con el predecesor {self.pred} ',func=ChordNode.check_predecessor,level='ERROR')
                            raise Exception(f'No se restablecio conexion con el predecesor {self.pred} ')
                    else:
                        counter=0
                                          
                    log_message(f'Chequeando predecesor ...',func=ChordNode.check_predecessor)
            except Exception as e:
                log_message(f'Se desconecto el predecesor con id {self.pred.id} e ip {self.pred.ip},error:{e}',func=ChordNode.check_predecessor,level='ERROR')
                traceback.print_exc()
                # En caso de ser un un anillo con dos nodos tb ponerme a mi mismo como sucesor.
                if  self.succ and self.pred.id==self.succ.id:
                    log_message('Entro en el de ponerse a si mimsmo como predecesor',func=ChordNode.check_predecessor)
                    self.succ=self.ref
                
                self.pred=None # Me quedo en none a la espera de un nuevo predecesor
                log_message(f' Acabo de poner mi predecesor en None',func=ChordNode.check_predecessor)
                time.sleep(3)
                
            time.sleep(3)

    
    # Start server method to handle incoming requests
    def start_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.port))
            s.listen(10)

            while True:
                conn, addr = s.accept()
                log_message(f'new connection from {addr}',func=ChordNode.start_server)

                data = conn.recv(1024).decode().split(',')

                data_resp = None
                option = int(data[0])

                if option == FIND_SUCCESSOR:
                    id = int(data[1])
                    data_resp = self.find_succ(id)
                elif option == FIND_PREDECESSOR:
                   
                    id = int(data[1])
                    log_message(f'Me llego una peticion de buscar el predecesor de {id}',func=ChordNode.start_server)
                    data_resp = self.find_pred(id)
                elif option == GET_SUCCESSOR:
                    data_resp = self.succ if self.succ else self.ref
                elif option == GET_PREDECESSOR:
                    data_resp = self.pred if self.pred else self.ref
                elif option == NOTIFY:
                    id = int(data[1])
                    ip = data[2]
                    log_message(f'Llego una notificacion del ip:{ip}',func=ChordNode.start_server)
                    self.notify(ChordNodeReference(ip, self.port))
                elif option == CHECK_PREDECESSOR:
                    data_resp=self.ref
                elif option == CLOSEST_PRECEDING_FINGER:
                    id = int(data[1])
                    data_resp = self.closest_preceding_finger(id)
                
              
                if data_resp:
                    response = f'{data_resp.id},{data_resp.ip}'.encode()
                    conn.sendall(response)
                conn.close()

if __name__ == "__main__":
    print("Hello dht")
    #time.sleep(10)
    ip = socket.gethostbyname(socket.gethostname())
    node = ChordNode(ip)

    if len(sys.argv) >= 2:
        other_ip = sys.argv[1]
        #node.join(ChordNodeReference(other_ip, node.port))
    
    while True:
        pass
