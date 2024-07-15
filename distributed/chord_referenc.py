import Pyro5.api
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


def getChordNameAddress(id_:int):
    return f'{id_}.chord'

import Pyro5.api
@Pyro5.api.expose
class ChordNodeReference:
    def __init__(self,ip: str, port: int = 8001):
        self.id=getShaRepr(ip)
        self.ip=ip
        self.port=port
        self.url=getChordNameAddress(self.id)
      
     
    #def ping(self):
    #    """Devuelve la instancia de la clase
    #        Si la clase se desconecto None
#
    #    Raises:
    #        Exception: _description_
#
    #    Returns:
    #        _type_: _description_
    #    """
    #    return check_if_url_is_active(self.url) # Devuelvo el nodo de chord, None si se desconecto
        