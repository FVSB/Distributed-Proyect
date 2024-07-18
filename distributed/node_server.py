import helper.db as db
from chord_lider import *
from helper.docs_class import *
import helper.db as db
from enum import Enum
class CrudCodes(Enum):
    """
    Dice el tipo de comando para los CRUD

    Returns:
        _type_: _description_
    """
    OK=1, # Se inserto correctamente
    OLDER=2, # Este es mas viejo que el que tenia 
    ERROR=3, # Error insertando
    UPDATED=5,#Se actualizo la version
    
    
class STORENODE(Leader):
    def __init__(self, ip: str, port: int = 8001, m: int = 160):
        super().__init__(ip, port, m)
        
    
    def handle_request(self, data, option, a):
        if option==STORE_KEY_SERVER:
            
        return super().handle_request(data, option, a)
        
    
    def insert_document(self,document:Document):
        node=self.find_key_owner(document.id)
        if node.id!=self.id:
            log_message(f'El nodo que es dueño del documento con titulo {document.title} es {node.id} y yo soy el nodo {self.id}',func=self.insert_document)
            raise Exception(f'Error al insertar')
        
        doc,node_id=db.get_document_by_id(document.id)
        if node_id !=self.id:
            log_message(f'El nodo que tenia la informacion es {node_id} y yo soy {self.id}',func=self.insert_document)
            raise Exception('Error insertando ')
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
