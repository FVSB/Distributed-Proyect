import zmq
import zmq.asyncio
import asyncio
import threading
import socket
import threading
import sys
import time
import hashlib
import traceback

from helper.protocol_codes import *
from helper.logguer import log_message
from utils import getShaRepr

class Message:
    def __init__(self,op:int,data:str):
        self.op:int=op
        self.data:str=data
# Class to reference a Chord node
class ChordNodeReference:
    def __init__(self, ip: str, port: int = 8001):
        self.id = getShaRepr(ip)
        self.ip = ip
        self.port = port
        self.context = zmq.Context()
       

    # Internal method to send data to the referenced node
    #def _send_data(self, op, data)->Message:
    #    message = Message(op,data)
    #    try:
    #        socket=self.context.socket(zmq.REQ)
    #        socket.connect(f"tcp://{self.ip}:{self.port}")
    #        socket.setsockopt(zmq.TCP_KEEPALIVE, 1)  # Habilita keepalive
    #        socket.setsockopt(zmq.TCP_KEEPIDLE, 10000)  # Tiempo de espera antes de enviar un keepalive
    #        socket.setsockopt(zmq.TCP_KEEPINTVL, 2000)  # Intervalo entre keepalive
    #        socket.send_pyobj(message) # Enviar 
    #        log_message(f"Mensaje enviado: {message}",func=ChordNodeReference._send_data)
    #        # Esperar respuesta con tiempo de espera
    #        start_time = time.time()
    #        while True:
    #            if time.time() - start_time > 10:  # Tiempo de espera máximo de 10 segundos
    #                raise TimeoutError("Tiempo de espera excedido")
    #            response = socket.recv_pyobj()
    #            log_message(f"Respuesta recibida: {response}",func=ChordNodeReference._send_data)
    #            return response
            
    #
    #         
    #
    #     except Exception as e:
    #
    #         log_message(f"ERROR enviando datos: {e}",func=ChordNodeReference._send_data)
    #
    #         return None
            
            
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
        log_message(f'La respuesta de si esta vivo el nodo vivo o no es {response}',func=ChordNodeReference.check_predecessor)
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

class ChordNode:
    def __init__(self, ip: str, port: int = 8001, m: int = 160):
        self.id = getShaRepr(ip)
        self.ip:str = ip
        self.port:int=port
        self.ref:ChordNodeReference = ChordNodeReference(self.ip, self.port)
        self.succ:ChordNodeReference = self.ref  # Initial successor is itself
        self.pred:ChordNodeReference = None  # Initially no predecessor
        self.m:int = m  # Number of bits in the hash/key space
        self.finger = [self.ref] * self.m  # Finger table
        self.next = 0  # Finger table index to fix next
        self.data = {}  # Dictionary to store key-value pairs
        
        self._key_range=(-1,self.ip) # the key_range [a,b) if a =-1 because no have predecesor
        self.nodes_discovered = {}
        self.context=zmq.asyncio.Context()
        self.lock = threading.Lock()
        
         # Start background threads for stabilization, fixing fingers, and checking predecessor
        threading.Thread(target=self.stabilize, daemon=True).start()  # Start stabilize thread
        threading.Thread(target=self.fix_fingers, daemon=True).start()  # Start fix fingers thread
        threading.Thread(target=self.check_predecessor, daemon=True).start()  # Start check predecessor thread
        threading.Thread(target=self.start_server, daemon=True).start()  # Start server thread
        threading.Thread(target=self.broadcast,daemon=True).start()# Make all time broadcast
        #asyncio.run(self.start_server()) # Start 
        threading.Thread(target=self.show,daemon=True).start() # Start funcion que se esta printeando todo el tipo cada n segundos
        
    
    def show(self):
        """
        Show my ip and id and mi predecessor and succesors ips and ids
        """
        print(f'ENtro en print')
        """Printea quien soy yo"""
        while True:
            log_message('-'*20,level='INFO',func=ChordNode.show)
            log_message(f'Mi predecesor es {self.pred.id if self.pred else None} con ip {self.pred.ip if self.pred else None} ',level='INFO',func=ChordNode.show)
            log_message(f'Yo soy id:{self.id},con ip:{self.ip} ',level='INFO')
            if self.succ.id == self.id:
                if self.succ.ip !=self.ip:
                    log_message('El sucesor tiene igual ID pero no tiene igual ip',level='INFO',func=ChordNode.show)
                log_message(f'Todavia no tengo sucesor',level='INFO',func=ChordNode.show),
            else:
                log_message(f'Mi sucesor es {self.succ.id if self.succ else None} con ip {self.succ.ip  if self.succ else None}',level='INFO',func=ChordNode.show)
            log_message('*'*20,level='INFO',func=ChordNode.show)
            
            
            time.sleep(3) # Se presenta cada 10 segundos

    # Helper method to check if a value is in the range (start, end]
    def _inbetween(self, k: int, start: int, end: int) -> bool:
        if start < end:
            return start < k <= end
        else:  # The interval wraps around 0
            return start < k or k <= end

    # Method to find the successor of a given id
    def find_succ(self, id: int) -> 'ChordNodeReference':
        node = self.find_pred(id)  # Find predecessor of id
        return node.succ  # Return successor of that node

    # Method to find the predecessor of a given id
    def find_pred(self, id: int) -> 'ChordNodeReference':
        node = self
        while not self._inbetween(id, node.id, node.succ.id):
            node = node.closest_preceding_finger(id)
        return node

    # Method to find the closest preceding finger of a given id
    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        for i in range(self.m - 1, -1, -1):
            if self.finger[i] and self._inbetween(self.finger[i].id, self.id, id):
                return self.finger[i]
        return self.ref

    # Method to join a Chord network using 'node' as an entry point
    def join(self, node: 'ChordNodeReference'):
        if node:
            self.pred = None
            #self.succ = node.find_successor(self.id)
            #self.succ.notify(self.ref)
        else:
            self.succ = self.ref
            self.pred = None

    # Stabilize method to periodically verify and update the successor and predecessor
    def stabilize(self):
        while True:
            try:
                if self.succ.id != self.id:
                    print('stabilize')
                    x = self.succ.pred
                    if x.id != self.id:
                        print(x)
                        if x and self._inbetween(x.id, self.id, self.succ.id):
                            self.succ = x
                        self.succ.notify(self.ref)
                #elif self.pred: # Caso que no tengo sucesor pero si tengo predecesor 
                #    #node=self.find_pred(0) #Buscar el predecesor del 0
                #    #print(f'El sucesor de 0  es {self.find_succ(350).id}, otro: {self.find_pred(350).id}')
                #    #try:
                #    #    self.succ=node.pred
                #    #except:
                #    #    print(f'El nodo con id {node.id} no tiene predecesor ')
                #    
                #    #Caso de que el predecesor no tenga predecesor => son dos nodos solos en la red:
                #    node=self.pred.pred
                #    if node.id==self.pred.id:  # En este caso hacemos que el sucesor sea  nuestro predecesor
                #       
                #        self.succ=self.pred
                #    else: # Entonces es que hay mas 2 nodos en la red => implica buscar el sucesor de 0
                #         
                #        self.succ=self.pred.find_successor(0)
                #    
                #    self.pred.notify(self.ref)
                            
                        
                    
            except Exception as e:
                print(f"Error in stabilize: {e}")
            #print(f"successor : {self.succ} predecessor {self.pred}")
            time.sleep(2)

    # Notify method to inform the node about another node
    def notify(self, node: 'ChordNodeReference'):
        if node.id == self.id:
            pass
        if not self.pred or self._inbetween(node.id, self.pred.id, self.id):
            self.pred = node

    # Fix fingers method to periodically update the finger table
    def fix_fingers(self):
        while True:
            try:
                self.next += 1
                if self.next >= self.m:
                    self.next = 0
                self.finger[self.next] = self.find_succ((self.id + 2 ** self.next) % 2 ** self.m)
            except Exception as e:
                print(f"Error in fix_fingers: {e}")
            time.sleep(10)

    # Check predecessor method to periodically verify if the predecessor is alive
    def check_predecessor(self):
        while True:
            try:
                if self.pred:
                    self.pred.check_predecessor()
            except Exception as e:
                self.pred = None
            time.sleep(10)

    #async def publisher(self):
    #    socket = self.context.socket(zmq.PUB)
    #    socket.bind("tcp://*:8000")
#
    #    while True:
    #        #message = f"{self.node_id}:{self.ip}"
    #        message=Message(JOIN,self.ip)
    #        await socket.send_pyobj(message)
    #        #print(f"Mensaje enviado: {message}")
    #        log_message(f'Mensaje de broadcast, Enviado ',func=ChordNode.publisher)
    #        await asyncio.sleep(1)
    
    
    def broadcast(self):
        socket = self.context.socket(zmq.PUB)
        try:
            socket.bind(f"tcp://{self.ip}:8000")
        except zmq.ZMQError as e:
            log_message(f"Error al enlazar el socket: {e}", func=self.publisher)
            return

        while True:
            message = Message(JOIN, self.ip)
            socket.send_pyobj(message)
            log_message(f'Mensaje de broadcast, Enviado ', func=self.publisher)
            time.sleep(1)
            
    def start_server(self):
        socket = self.context.socket(zmq.SUB)
        try:
            socket.connect(f"tcp://{self.ip}:8000")
        except zmq.ZMQError as e:
            log_message(f"Error al conectar el socket: {e}", func=self.subscriber)
            return
        socket.setsockopt_string(zmq.SUBSCRIBE, "")

        while True:
            message: Message = socket.recv_pyobj()
            log_message(f'Mensaje recibido {message}', func=self.subscriber)
            ip = message.data
            id_ = getShaRepr(ip)
            log_message(f'Se descubrio el nodo', func=self.subscriber)
            #with self.lock: # Bloquear el hilo
            #    self.nodes_discovered[id_] = ip
            #log_message(f"Mensaje recibido: {message}, Nodo descubierto: {self.nodes_discovered}", func=self.subscriber)
            if message.op == JOIN:
                self.join(ChordNodeReference(ip))

    #async def subscriber(self):
    #    socket = self.context.socket(zmq.SUB)
    #    socket.connect(f"tcp://{self.ip}:8000")
    #    socket.setsockopt_string(zmq.SUBSCRIBE, "")
#
    #    while True:
    #        message:Message = await socket.recv_pyobj()
    #        #print(message)
    #        log_message(f'Mensaje recibido {message}',func=ChordNode.subscriber)
    #        ip =message.data
    #        id_=getShaRepr(ip)
    #        log_message(f'Se descubrio el nodo',func=ChordNode.subscriber )
    #        with self.lock:
    #            self.nodes_discovered[id_] = ip
    #        log_message(f"Mensaje recibido: {message}, Nodo descubierto: {self.nodes_discovered}",func=ChordNode.subscriber)
    #        if message.op==JOIN:
    #            self.join(ChordNodeReference(ip))

    #async def start_server(self):
    #    await asyncio.gather(self.publisher(), self.subscriber())

if __name__ == "__main__":
    ip = socket.gethostbyname(socket.gethostname()) # Tomar mi ip
    node = ChordNode(ip)
    


