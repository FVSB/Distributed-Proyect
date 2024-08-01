from datetime import datetime
import pickle
from .utils import getShaRepr


class DocsRecords:
    
    def __init__(self, doc_id: int):
        self.doc_id = doc_id
        self.is_delete: bool = False
        self.last_change: datetime = datetime.utcnow()  # Muestra la hora utc

    def update(self):
        """
        Cuando se llama el cambio actual se registra como ahora
        
        """
        self.last_change=datetime.utcnow()  # Muestra la hora utc
        
    
    def can_update(self, new_doc: "DocsRecords") -> bool:
        """
        Dado el documento nuevo a update se ve si es mas nuevo que el de esta instancia
        

        Args:
            other (DocsRecords): _description_

        Returns:
            bool: True si el other es mas nuevo False si esta instancia es mas nueva
        """
        if not isinstance(new_doc, DocsRecords):
            raise Exception(f'Other debe ser de tipo  DocsRecords no:{type(new_doc)} {new_doc} ')
        
        if self.doc_id != new_doc.doc_id:
            return False
        return (
            self.last_change < new_doc.last_change
        )  # Si el ultimo cambio es mas reciente se queda el ultimo cambio
    def __str__(self)->str:
        return f'Historial: {self.last_change}'
    def __repr__(self) -> str:
        return str(self)
class Document:
    def __init__(self, title: str, text: str, max_value=16):
        self.id = getShaRepr(title)#, max_value)
        self.title: str = title
        self.text: str = text
        self.record: DocsRecords = DocsRecords(self.id)

    def get_in_bytes(self) -> bytes:
        return pickle.dumps(self)

    def delete(self):
        """
        Hace como que el documento fue eliminado
        """
        self.text=None
        self.record.is_delete=True
        self.record.update()

def get_document_from_bytes(data:bytes)->Document:
    """
    Dado unos bytes devuelve el objeto documento
    

    Args:
        data (bytes): bytes que serializan un documento

    Raises:
        Exception: Si al deserializar no es de tipo Document

    Returns:
        Document: _description_
    """
    doc= pickle.loads(data)
    if  not isinstance(doc,Document):
        raise Exception(f'los bytes deben contener un document pero contienen un {type(doc) } {doc}')
    return doc


if __name__ == "__main__":
    pass
