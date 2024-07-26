from sqlalchemy import create_engine, Column, Integer, Text, LargeBinary,Boolean,update
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from helper.docs_class import  Document
from helper.logguer import log_message
import pickle
# Crear el motor de base de datos
engine = create_engine('sqlite:///app/database/database.db')

# Crear una sesión
Session = sessionmaker(bind=engine)
session = Session()


# Crear la base declarativa
Base = declarative_base()

# Definir el modelo de la tabla
class Docs(Base):
    __tablename__ = 'docs'

    id = Column(Integer, primary_key=True)
    title = Column(Text)
    document = Column(LargeBinary)
    """
    Tipo de documento osea la extension 
    """
    persistent=Column(Boolean,default=False)
    """
    Si es persistente o no
    """
    
    node_id = Column(Integer)
    

# Crear la tabla si no existe
Base.metadata.create_all(engine)





def insert_document(document:Document,node_id:int,persistent:bool=False)->bool:
    """
        Dado un documento se trata de insertar, True si se inserto ,False si hubo algun error
    

    Args:
        document (Document): _description_
        persistent (bool, optional): _description_. Defaults to False.

    Returns:
        bool: _description_
    """
    try:
        session=Session()
        serialized_data=pickle.dumps(document)
        to_save=Docs(id=document.id,title=document.title,node_id=node_id,document=serialized_data,persistent=persistent)
        session.add(to_save)
        session.commit()
        session.close()
        return True
    except Exception as e:
        log_message(f'Ocurrio un error insertando el documento con id {document.id} y titulo {document.title}',func=insert_document)
        return False
    
    
    
def has_document(id_document:int)->bool:
    try:
        session=Session()
        doc=session.query(Docs).filter_by(id=id_document).first()
        session.close()
        return doc is None
    except Exception as e:
        log_message(f'Ocurrion un problema preguntando si existe el documento con id {id_document} {e}',func=has_document)
        
    
    
def persist_document(id_document:int):
    """
    Dado el id de un documento lo hace persistente osea que ya se entrego el mensaje de check en la base de datos

    Args:
        id_document (int): _description_

    Returns:
        _type_: _description_
    """
    
    try:
        session=Session()
        doc=session.query(Docs).filter_by(id=id_document).first()
        if doc is None:# Es que no existe el documento
            log_message(f'No se puede hacer persistente el documento con id {id_document} pq no esta en la base de datos',func=persist_document)
            return False
        doc.persistent=True # Se hizo persistente el documento
        session.commit()
        session.close()
        return True
    except Exception as e:
        log_message(f'Ocurrio una excepcion intentando hacer persistente el documento con id {id_document} \n {e}',func=persist_document)
        return False
    
def make_false_persist_document(id_document:int):
    
    session=Session()
    doc=session.query(Docs).filter_by(id=id_document).first()
    if doc is None:
        log_message(f'El documento con id {id_document } no se puedo hacer no persistente pq no esta en la base de datos',func=make_false_persist_document)
        return False
    doc.persistent=False
    session.commit()
    session.close()
    return False
    
def is_document_persistent(id_document:int)->bool:
    """
    Retorna True o False si la columna persistente de un documento esta en True o False
    None si el documento no se encuentra en la base de datos
    Puede lanzar exeptions

    Args:
        id_document (int): _description_

    Returns:
        bool: _description_
    """
    
    session=Session()
    doc=session.query(Docs).filter_by(id=id_document).first()
    session.close()
    if doc is None:
        return None

    return doc.persistent
           
           
def make_false_persist_all_nodes_rows(node_id:bool):
    """
    Dado el id de un nodo de chord todas las filas que lo tengan a el como dueño le van hacer el campo persist como False
    Retorna True si se pudo completar exitosamente la operacion
    False si ocurrio un error

    Args:
        node_id (bool): _description_
    """
    try:
        session=Session()
        session.query(Docs).filter(Docs.node_id == node_id).update({Docs.persistent: False})
        session.commit()
        session.close()
        return True
    except Exception as e:
        log_message(f'Hubo un error tratando de hacer False la columna persistent de el nodo {node_id}')
        return False
    


    
        
    
def get_document_by_id(id_document:int)->Document:
    """
    Dado un id de documento trata de devolver el documento
    None si el documento no existe

    Args:
        id_document (int): _description_

    Returns:
        Document: Documento 
        El documento si existe, None si no existe
    """
    session=Session()
    doc=session.query(Docs).filter_by(id=id_document).first()
    data=None
    if doc:
      data:Document=pickle.loads(doc.document)# Tomar el documento como objeto
    session.close()
    return data

def get_all_nodes_i_save()->set[int]:
    """
    Retorna un set con todos ids de nodos que guarda
    Lo maximo que puede guardar es 3

    Returns:
        set[int]: _description_
    """
    session=Session()
    nodes=session.query(Docs.node_id).all()
    session.close()
    response=set()
    for node in nodes:
        response.add(node[0])# Pq node es una tupla
    return response





def update_document(id_document:int,new_data:Document):
    session=Session()
    doc=session.query(Docs).filter_by(id=id_document).first()
    response=False
    if doc:
        doc.document=pickle.dumps(new_data)
        session.commit()

        response=True
    session.close()
    return response

def delete_document(document_id:int):
    session=Session()
    doc=session.query(Docs).filter_by(id=document_id).first()
    response=False
    if doc:
        session.delete(doc)
        session.commit()
        response=True
    session.close()

