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
        
        self.setup_routes()
        
    def setup_routes(self):
        # Registrar la ruta y vincularla al método de instancia
        app.add_url_rule('/upload', view_func=self.upload_file, methods=['POST'])
        
        
    def start_threads(self):
        """Inicia todos los hilos

        Returns:
            _type_: _description_
        """
       
        super().start_threads()
        threading.Thread(target=lambda:app.run(host=self.ip,port=8000),daemon=True).start()
        
        

    
    
   # @app.route('/upload', methods=['POST'])
    def upload_file(self):
        log_message(f'Se a mandado a guardar un archivo ',func= self.upload_file)
        if not 'file' in request.files:# Es que no se envió nada
            #Retornar error de no file
            return  jsonify({'message': 'Bad Request: Parámetro "param" requerido'}), 400
       
        # El cliente manda (Nombre archivo, archivo)
        file=request.files['file']
        
        doc_to_save=file.stream.read()
        
        name,doc_to_save=pickle.loads(doc_to_save)
        log_message(f'La data es {name}{doc_to_save}',func=self.upload_file)
        
        log_message(f'El archivo tiene nombre {name}',func=self.upload_file)
        hash_name=getShaRepr(name) # Hashear el nombre dado que esta sera la llave


        # Mandar a guardar en la base de datos

        # Comprobar que no esta en la base de datos
       
        doc= db.get_document_by_id(hash_name)
        log_message(f'El tipo de doc es {type(doc)}',func=self.upload_file)
        if doc is not None:
            log_message(f'El documento con nombre {name} ya estaba guardado ',func=self.upload_file)
            return jsonify({'message': f'Conflict: El archivo {name} ya existe en el servidor'}), 409
         
        try:
            db.insert_document(Document(name,doc_to_save),self.id)
            log_message(f'El archivo con nombre {name} se a guardado correctamente en la base de datos',func=self.upload_file)
            return jsonify({'message':f'El documento con nombre {name} se guardo correctamente'}),200
        
        except Exception as e :
            log_message(f' Hubo un error tratando de guardar el archivo {name} \n {e} \n {traceback.format_exc()}',func=self.upload_file)
            
            return jsonify({'message':f'Ocurrio un problema guardando el documento {name}'}),500
       
           
    
    



if __name__ == "__main__":
    print("Hello from Storage node")
    #time.sleep(10)
    ip = socket.gethostbyname(socket.gethostname())
    node = StoreNode(ip,m=3)
    
    node.start_threads()#Iniciar los nodos
    while True:
        pass
