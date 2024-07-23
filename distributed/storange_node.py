from chord_lider import *
from chord_lider import *
from flask import Flask,request,jsonify,Response
import socket
import jsonpickle
from helper.docs_class import *
import helper.db as db
from enum import Enum

app = Flask(__name__)
app.logger.disabled = True
log = logging.getLogger('werkzeug')
log.disabled = True


def serialize_pyobj_to_json_file(obj)->Response:
    serialize= jsonpickle.encode(obj)
    return jsonify(serialize)

def deserialize_pyobj_from_json(data):
    return jsonpickle.decode(data)


class StoreNode(Leader):
    def __init__(self, ip: str, port: int = 8001, m: int = 160):
        super().__init__(ip, port, m)
        
        
    def start_threads(self):
        """Inicia todos los hilos

        Returns:
            _type_: _description_
        """
        return super().start_threads()
    
    
    
    



if __name__ == "__main__":
    print("Hello from Storage node")
    #time.sleep(10)
    ip = socket.gethostbyname(socket.gethostname())
    node = StoreNode(ip,m=3)
    node.start_threads()#Iniciar los nodos
    while True:
        pass
