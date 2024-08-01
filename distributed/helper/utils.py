import hashlib
from flask import Flask, request, jsonify, Response
import socket
import jsonpickle
import pickle
import uuid

from enum import Enum, auto


def obj_to_bytes(obj:object)->bytes:
    return pickle.dumps(obj)
def getShaRepr(data: str, max_value: int = 120000):
    """Hashea a SHA-1 los datos que entren y lo devuelve en un numero entre 1 y 16

    Args:
        data (str): _description_
        max_value (int, optional): _description_. Defaults to 16.

    Returns:
        _type_: _description_
    """
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


def get_guid() -> str:
    """
    Genera un guid aleatorio

    Returns:
        str: _description_
    """
    # Genera un GUID aleatorio
    guid = uuid.uuid4()

    # Convierte el GUID a string
    guid_str = str(guid)

    return guid_str


def is_equal_list(lis_1:list,list_2:list)->bool:
    """
    Retorna True si la lista 1 y la 2 los objetos en ellas son iguales
    False en otro caso

    Args:
        lis_1 (list): _description_
        list_2 (list): _description_

    Returns:
        bool: _description_
    """
    return set(lis_1)==set(list_2)

def serialize_pyobj_to_json_file(obj) -> Response:
    serialize = jsonpickle.encode(obj)
    return jsonify(serialize)


def deserialize_pyobj_from_json(data):
    return jsonpickle.decode(data)


class Paquete:
    def __init__(self, numero, bytes_datos, es_final):
        self.numero = numero
        self.bytes_datos = bytes_datos
        self.es_final = es_final

    def serialize(self):
        return pickle.dumps(self)

    @staticmethod
    def deserialize(data) -> "Paquete":
        return pickle.loads(data)





class CrudCode(Enum):
    Insert = auto()
    Update = auto()
    Delete = auto()
    ReInsertSelf=auto()

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented

    def __eq__(self, other):
        if self.__class__ is other.__class__:
            return self.value == other.value
        return NotImplemented

    def __gt__(self, other):
        if self.__class__ is other.__class__:
            return self.value > other.value
        return NotImplemented