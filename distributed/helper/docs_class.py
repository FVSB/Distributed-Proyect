from datetime import datetime
import pickle

import hashlib
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


        


class DocsRecords:
    def __init__(self,doc_id:int):
        self.doc_id=doc_id
        self.is_delete:bool=False
        self.last_change:datetime=datetime.utcnow()# Muestra la hora utc
        
        
    def can_update(self,other:'DocsRecords')->bool:
        if self.doc_id!=other.doc_id: return False
        return self.last_change<other.last_change # Si el ultimo cambio es mas reciente se queda el ultimo cambio
    
        
class Document:
    def __init__(self,title:str,text:str):
        self.id=getShaRepr(title,128)
        self.title:str=title
        self.text:str=text
        self.record:DocsRecords=DocsRecords(self.id)
    
    def get_in_bytes(self)->bytes:
        return pickle.dumps(self)
    
    
if __name__=='__main__':
    pass