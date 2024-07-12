import socket
import threading
import sys
import time
import hashlib
import traceback
import logging
# Operation codes
EMPTYBIT=b''
FIND_SUCCESSOR = 1
FIND_PREDECESSOR = 2
#FIND_SUCCESSOR_WITHOUT_PREDECESSOR=10 # Busca el nodo que tiene sucesor y no predecesor el cual es el nodo "0 " por lo cual el nodo de mayor rango de la red debe buscarlo
GET_SUCCESSOR = 3
GET_PREDECESSOR = 4
NOTIFY = 5
CHECK_PREDECESSOR = 6
CLOSEST_PRECEDING_FINGER = 7
STORE_KEY = 8
RETRIEVE_KEY = 9
logger = logging.getLogger(__name__)

# Function to hash a string using SHA-1 and return its integer representation
def getShaRepr(data: str):
    return int(hashlib.sha1(data.encode()).hexdigest(), 16)
    

# Class to reference a Chord node
class ChordNodeReference:
    def __init__(self, ip: str, port: int = 8001):
        self.id = getShaRepr(ip)
        self.ip = ip
        self.port = port

    # Internal method to send data to the referenced node
    def _send_data(self, op: int, data: str = None) -> bytes:
        print(f'mandando la data con op{op} - y data {data}')
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.ip, self.port))
                s.sendall(f'{op},{data}'.encode('utf-8'))
                new_data=s.recv(1024)
                print(f'Se recibioo de data envada con opcion {op} {str(new_data)}')
                return new_data
        except Exception as e:
            print(f"Error sending data: {e} al nodo con id {self.id} e ip {self.ip}")
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
        print(f'La respuesta de si esta vivo el lider o no es {response}')
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
            print('-'*20)
            print(f'Mi predecesor es {self.pred.id if self.pred else None} con ip {self.pred.ip if self.pred else None} ')
            print(f'Yo soy id:{self.id},con ip:{self.ip} ')
            if self.succ.id == self.id:
                if self.succ.ip !=self.ip:
                    print('El sucesor tiene igual ID pero no tiene igual ip')
                print(f'Todavia no tengo sucesor')
            else:
                print(f'Mi sucesor es {self.succ.id if self.succ else None} con ip {self.succ.ip  if self.succ else None}')
            print('*'*20)
            
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
        print('El más cercano soy yo')
        return self.ref

    # Method to join a Chord network using 'node' as an entry point
    def join(self, node: 'ChordNodeReference'):
        """
            Method to join a Chord network using 'node' as an entry point
        Args:
            node (ChordNodeReference): _description_
        """
        if node:
            self.pred = None
            self.succ = node.find_successor(self.id)
            print(f'Este es el sucesor {self.succ}')
            self.succ.notify(self.ref)
        else:
            self.succ = self.ref
            self.pred = None

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
                    print('stabilize')
                    if self.succ is None: self.succ=self.ref
                    try: 
                        x = self.succ.pred 
                    except: # Esto es que no responde el sucesor 
                        count_failed+=1
                        
                        if count_failed>3:
                            # Trata de contactar con el succ y preguntarle por el predecesor si pasa algun problema en la peticion
                            # Mi sucesor soy yo
                            #Si mi sucesor era mi antecesor entonces hago el antecesor null
                            #if self.succ.id==self.pred.id: self.pred=None
                            time.sleep(3)
                            print('Seleccionando_Nuevo_Sucesor')
                            nearest_node=self.find_succ(self.id+1)
                            print(f'EL nodo que tiene {nearest_node.id} el mas cercano  ')
                            self.succ=self.ref
                            continue
                       
                    print(f' Este es X {x}')
                    if x and x.id != self.id:
                        print(f'Otra vez x {x}')
                        if x and self._inbetween(x.id, self.id, self.succ.id):
                            self.succ = x
                        print('A Notificar')
                        try:
                            self.succ.notify(self.ref)
                        except Exception as e: # Si no se puede conumicar con el nuevo sucesor caso que sea que era sucesor y predecesor
                            # y se desconecto pues poner al sucesor como yo mismo
                            print(f' Fallo comunicarse con el nuevo sucesor {e}')
                            self.succ=self.ref
                elif self.pred: # Caso que no tengo sucesor pero si tengo predecesor 
                    
                    #Caso de que el predecesor no tenga predecesor => son dos nodos solos en la red:
                    node=self.pred.pred
                    if node.id==self.pred.id:  # En este caso hacemos que el sucesor sea  nuestro predecesor
                       
                        self.succ=self.pred
                    else: # Entonces es que hay mas 2 nodos en la red => implica buscar el sucesor de 0
                         
                        self.succ=self.pred.find_successor(0)
                    
                    self.pred.notify(self.ref)
                    
            except Exception as e:
                
                
                print(f"  Is_True:{Is}_  node:{node}_  _::: Error in stabilize: {e}")
                traceback.print_exc() 

            print(f"successor : {self.succ} predecessor {self.pred}")
            time.sleep(3) #Poner en produccion en 1 segundo

    # Notify method to inform the node about another node
    def notify(self, node: 'ChordNodeReference'):
        """ Notify method to inform the node about another node"""
        if node.id == self.id:
            pass
        if not self.pred or self._inbetween(node.id, self.pred.id, self.id):
            self.pred = node

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
                    raise Exception(f'Si logro encontrar un nodo menor {a.id},en el indice {self.next}, {self.finger[1]} ')
                ok=False
                for _ in range(3):
                    if a.check_predecessor():
                        ok=True
                        break
                    time.sleep(3)
               # a=a.succ if not ok else a
                if not ok:
                    print(f'El nodo {a.id} se desconecto de la red')
                    a=a.succ
                    print(f'El nodo a ahora es {a}')
                

                self.finger[self.next] = a
               # print('/'*40)
               # print(f'Mis finger table es {self.finger} ')
               # print('+'*40)
            except Exception as e:
                print(f"Error in fix_fingers: {e}")
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
                            raise Exception(f'No se restablecio conexion con el predecesor {self.pred} ')
                    else:
                        counter=0
                                          
                    print(f'Chequeando predecesor ...')
            except Exception as e:
                print(f'Se desconecto el predecesor con id {self.pred.id} e ip {self.pred.ip},error:{e}')
                traceback.print_exc()
                # En caso de ser un un anillo con dos nodos tb ponerme a mi mismo como sucesor.
                if  self.succ and self.pred.id==self.succ.id:
                    print('Entro en el de ponerse a si mimsmo como predecesor')
                    self.succ=self.ref
            
                self.pred = None
                
            time.sleep(5)

    # Store key method to store a key-value pair and replicate to the successor
    def store_key(self, key: str, value: str):
        key_hash = getShaRepr(key)
        node = self.find_succ(key_hash)
        node.store_key(key, value)
        self.data[key] = value  # Store in the current node
        self.succ.store_key(key, value)  # Replicate to the successor

    # Retrieve key method to get a value for a given key
    def retrieve_key(self, key: str) -> str:
        key_hash = getShaRepr(key)
        node = self.find_succ(key_hash)
        return node.retrieve_key(key)
    
    
    # Start server method to handle incoming requests
    def start_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.port))
            s.listen(10)

            while True:
                conn, addr = s.accept()
                print(f'new connection from {addr}')

                data = conn.recv(1024).decode().split(',')

                data_resp = None
                option = int(data[0])

                if option == FIND_SUCCESSOR:
                    id = int(data[1])
                    data_resp = self.find_succ(id)
                elif option == FIND_PREDECESSOR:
                   
                    id = int(data[1])
                    print(f'Me llego una peticion de buscar el predecesor de {id}')
                    data_resp = self.find_pred(id)
                elif option == GET_SUCCESSOR:
                    data_resp = self.succ if self.succ else self.ref
                elif option == GET_PREDECESSOR:
                    data_resp = self.pred if self.pred else self.ref
                elif option == NOTIFY:
                    id = int(data[1])
                    ip = data[2]
                    print(f'Llego una notificacion del ip:{ip}')
                    self.notify(ChordNodeReference(ip, self.port))
                elif option == CHECK_PREDECESSOR:
                    data_resp=self.ref
                elif option == CLOSEST_PRECEDING_FINGER:
                    id = int(data[1])
                    data_resp = self.closest_preceding_finger(id)
                elif option == STORE_KEY:
                    key, value = data[1], data[2]
                    self.data[key] = value
                elif option == RETRIEVE_KEY:
                    key = data[1]
                    data_resp = self.data.get(key, '')
              
                    

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
        node.join(ChordNodeReference(other_ip, node.port))
    
    while True:
        pass
