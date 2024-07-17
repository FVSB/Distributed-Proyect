from sqlalchemy import create_engine, Column, Integer, Text, LargeBinary
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from doc import  Documento
import pickle
# Crear el motor de base de datos
engine = create_engine('sqlite:///stable.db')

# Crear una sesión
Session = sessionmaker(bind=engine)
session = Session()


# Crear la base declarativa
Base = declarative_base()

# Definir el modelo de la tabla
class MiTabla(Base):
    __tablename__ = 'docs'

    id = Column(Integer, primary_key=True)
    titule = Column(Text)
    document = Column(LargeBinary)
    node_id = Column(Integer)

# Crear la tabla si no existe
Base.metadata.create_all(engine)


# Crear un objeto Documento
mi_documento = Document(100, "Mi título", b'\x01\x23\x45\x67\x89\xAB\xCD\xEF', 456)



def insert_document(document:Document,node_id:int):
    session=Session()
    serialized_data=pickle.dumps(document)
    to_save=MiTabla(id=document.id,titule=document.titule,document=serialized_data,node_id=node_id)
    session.add(to_save)
    session.commit()
    session.close()
    return True
    
def get_document_by_id(id_document:int):
    session=Session()
    doc=session.query(MiTabla).filter_by(id=id_document).first()
    data=None
    if doc:
      data=pickle.loads(doc.document)# Tomar el documento como objeto
    session.close()
    return data

def get_all_nodes_i_save():
    session=Session()
    nodes=session.query(MiTabla.node_id).all()
    session.close()
    response=set()
    for node in nodes:
        response.add(node[0])# Pq node es una tupla
    return response



def update_document(id_document:int,new_data:Document):
    session=Session()
    doc=session.query(MiTabla).filter_by(id=id_document).first()
    response=False
    if doc:
        doc.document=pickle.dumps(new_data)
        session.commit()

        response=True
    session.close()
    return response

def delete_document(document_id:int):
    session=Session()
    doc=session.query(MiTabla).filter_by(id=document_id).first()
    response=False
    if doc:
        session.delete(doc)
        session.commit()
        response=True
    session.close()

insert_document(mi_documento,0)  
        
# Insertar el objeto en la tabla
#nueva_fila = MiTabla(id=mi_documento.id, titule=mi_documento.titule, document=mi_documento.document, nodo_id=mi_documento.nodo_id)
#session.add(nueva_fila)
#session.commit()
#
## Cerrar la sesión
#session.close()