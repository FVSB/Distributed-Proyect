from datetime import datetime
import pickle
from .utils import getShaRepr


class DocsRecords:
    def __init__(self, doc_id: int):
        self.doc_id = doc_id
        self.is_delete: bool = False
        self.last_change: datetime = datetime.utcnow()  # Muestra la hora utc

    def can_update(self, other: "DocsRecords") -> bool:
        """
        Dado el documento nuevo a update se ve si es mas nuevo que el de esta instancia
        

        Args:
            other (DocsRecords): _description_

        Returns:
            bool: True si el other es mas nuevo False si esta instancia es mas nueva
        """
        if not isinstance(other, DocsRecords):
            raise Exception(f'Other debe ser de tipo  DocsRecords no:{type(other)} {other} ')
        
        if self.doc_id != other.doc_id:
            return False
        return (
            self.last_change < other.last_change
        )  # Si el ultimo cambio es mas reciente se queda el ultimo cambio
    def __str__(self)->str:
        return f'Historial: {self.last_change}'
    def __repr__(self) -> str:
        return str(self)
class Document:
    def __init__(self, title: str, text: str, max_value=16):
        self.id = getShaRepr(title, max_value)
        self.title: str = title
        self.text: str = text
        self.record: DocsRecords = DocsRecords(self.id)

    def get_in_bytes(self) -> bytes:
        return pickle.dumps(self)


if __name__ == "__main__":
    pass
