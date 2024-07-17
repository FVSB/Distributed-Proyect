import socket
import pickle
import traceback
from helper.protocol_codes import *
def send_data( op:int,ip:str,port:int=8001, data:str=''):
        #log_message(f'Typo de op {type(op)}  tipo de data{type(data)}')
        data=(op,data)
        # Serializar el objeto
        #print(f':a data es {data}')
        data = pickle.dumps(data)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((ip, port))
                s.settimeout(10)# Decir que a lo sumo espera 3 segundos
                s.sendall(data)
                new_data=s.recv(1024)
                #log_message(f'La data es {new_data}',func=self._send_data)
                data_format=pickle.loads(new_data)#Al tipo de dato de python
                #log_message(f'A llegado un objeto {data_format} de tipo {type(data_format)}',func=self._send_data)
                return data_format
        except Exception as e:
           print(f'Ocurrio el problema {e}')
           traceback.print_exc()
           
import time    
   
while True:
       
        try:       
            a=send_data(STORE_KEY_CLIENT,'172.17.0.2',data=(9,"La data a guardar"))
            b=send_data(RETRIEVE_KEY_CLIENT,'172.17.0.4',data=9)
            if a is None : continue
            print(a)
            print(b)
            break
        except:
           print("Nada")
           traceback.print_exc()
        time.sleep(10) 