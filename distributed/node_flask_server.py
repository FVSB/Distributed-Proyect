from chord_lider import *
from flask import Flask,request,jsonify,Response
import socket
import jsonpickle
from helper.docs_class import *
import helper.db as db
from enum import Enum

class CrudCodes:
    """
    Dice el tipo de comando para los CRUD

    Returns:
        _type_: _description_
    """
    OK=1, # Se inserto correctamente
    OLDER=2, # Este es mas viejo que el que tenia 
    ERROR=3, # Error insertando
    UPDATED=5,#Se actualizo la version


app = Flask(__name__)
app.logger.disabled = True
log = logging.getLogger('werkzeug')
log.disabled = True

def serialize_pyobj_to_json_file(obj)->Response:
    serialize= jsonpickle.encode(obj)
    return jsonify(serialize)

def deserialize_pyobj_from_json(data):
    return jsonpickle.decode(data)



class NodeServer(Leader):
    """
    Nodo servidor de peticiones

    Args:
        Leader (_type_): _description_
    """
    
    
    def start_threads(self):
        """Empieza los hilos
        """
        super().start_threads()
        
    def start_flask_server(self):
        app.run(host=self.ip, port=self.port,debug=True)
        
    def __init__(self, ip: str, port: int = 8001, m: int = 160):
        
        super().__init__(ip, port, m)
        


    @app.route('/', methods=['GET'])
    def index(self):
        return serialize_pyobj_to_json_file(self.ref) 
    
    
    
    def insert_document_in_node(self,document:Document)->bool:
        """
            Insertar documento en el nodo 
        Args:
            document (Document): _description_

        Returns:
            _type_: _description_
        """ 
      #  log_message(f'Insertando documento {document.id} titulo {document.title}',func=self.insert_document_in_node)
        
        doc,node_id=db.get_document_by_id(document.id)
        if doc:
            if not doc.record.can_update(document.record):
               ## log_message(f'El documento a insertar se modifico por ultima vez {document.record.last_change} y el que se tenia guardado {doc.record.last_change}',func=self.insert_document_in_node)
                return CrudCodes.OLDER #No puedo inseetar pq es demasiado viejo
            else:
              #  log_message(f'Se va a upgradear el documento {document.id} con titulo {document.title}',func=self.insert_document_in_node)
               
                if   db.update_document(document.id,document):
                  #  log_message(f'Se upgradeo correctamente el documento {document.id} con titulo {document.title}',func=self.insert_document_in_node)
                    return CrudCodes.UPDATED
                
                else: 
                   # log_message(f'Error al insertar el documento {document.id} con titulo {document.title}',func=self.insert_document_in_node)
                    return CrudCodes.ERROR
        else:
         if db.insert_document(document,self.id): return CrudCodes.OK
         else : return CrudCodes.ERROR
                
                
        
    
    
    @app.route('/insert_document',methods=['POST'])
    def add_document(self):
        data= request.get_json()
        obj=deserialize_pyobj_from_json(data)#Obtener objeto documento
        if not isinstance(obj,Document):
           # log_message(f'Se esperaba un tipo documento no un {type(obj)} {obj}',func=self.add_document)
            return {'message':f'Error document can´t {type(obj)}  this will be type Document '}
        
        
        

       
        


if __name__ == '__main__':
    ip = socket.gethostbyname(socket.gethostname())
    node=NodeServer(ip,m=3)
    print(f'Mi ip {ip}')
    app.run(host=ip,port=node.port)
    node.start_threads()#Iniciar los nodos
    while True:
        pass

        