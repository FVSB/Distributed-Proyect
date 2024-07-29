from helper.utils import *
from helper.docs_class import *
from helper.logguer import log_message
import heapq
import threading
import time
import traceback
from typing import Callable


class DeleteHandle:
    """
    Clase que irá en el heap para decir que tipo de documentos se deben eliminar pq no se recibió respuesta del dueño
    """

    def __init__(
        self,
        time_: int,
        document_id: int,
        func_handle: Callable[[], None],
        guid: str = None,
    ):
        self.time_ = time_
        self.guid: str = get_guid() if guid is None else guid
        self.document_id: int = document_id
        self.func_handle: Callable[[], None] = func_handle

    def execute(self):
        try:
            log_message(
                f"Se va a ejecutart el execute del documento {self.document_id} con guid {self.guid} que tiene vencimiento en {self.time_}",
                func=self.execute,
            )
            self.func_handle()
        except Exception as e:
            log_message(
                f"Ocurrio un Error en el execute del documento con id {self.document_id} y con guid {self.guid} con vencimiento {self.time_} Error:{e} \n {traceback.format_exc()}",
                func=self.execute,
            )

    def __eq__(self, value: "DeleteHandle") -> bool:
        if not isinstance(value, DeleteHandle):
            log_message(
                f"El valor de value no es de tipo DeleteHandle es de tipo {type(value)}, {value}",
                func=self.__eq__,
            )
            raise Exception(
                f"El valor de value no es de tipo DeleteHandle es de tipo {type(value)}, {value}"
            )
        return self.time_ == value.time_

    def __lt__(self, other: "DeleteHandle"):

        if not isinstance(other, DeleteHandle):
            log_message(
                f"El valor de other no es de tipo DeleteHandle es de tipo {type(other)}, {other}",
                func=self.__lt__,
            )
            raise Exception(
                f"El valor de other no es de tipo DeleteHandle es de tipo {type(other)}, {other}"
            )

        return self.time_ < other.time_


import heapq
import threading


class MinHeap:
    def __init__(self, initial=None):
        self._heap: list[DeleteHandle] = []
        self._lock: threading.RLock = threading.RLock()
        if initial:
            self._heap = initial
            heapq.heapify(self._heap)

    def push(self, item: DeleteHandle):
        with self._lock:
            heapq.heappush(self._heap, item)

    def pop(self) -> DeleteHandle:
        with self._lock:
            return heapq.heappop(self._heap)

    def peek(self) -> DeleteHandle:
        with self._lock:
            if self._heap:
                return self._heap[0]
            return None

    def is_empty(self) -> bool:
        with self._lock:
            return not bool(self._heap)

    def __getitem__(self, index):
        with self._lock:
            return self._heap[index]

    def __len__(self):
        with self._lock:
            return len(self._heap)

    def __str__(self):
        with self._lock:
            return str(self._heap)

    def __repr__(self):
        with self._lock:
            return f"MinHeap({self._heap})"

    def __iter__(self):
        with self._lock:
            return iter(self._heap)

    def __contains__(self, item):
        with self._lock:
            return item in self._heap


class Set:
    """
    Tener un set seguro ante hilos para controlar los guids que ya se hicieron check
    """

    def __init__(self) -> None:
        self._set: set[str] = set([])
        self._lock: threading.RLock = threading.RLock()

    def add_guid(self, guid: str):
        if not isinstance(guid, str):
            log_message(
                f"Error añadiendo al set gestor de hilos guid debe ser str no {type(guid)} {guid}",
                func=self.add_guid,
            )
        with self._lock:
            self._set.add(guid)

    def contains_guid(self, guid: str) -> bool:
        if not isinstance(guid, str):
            log_message(
                f"Error vuendo si lo contiene al set gestor de hilos guid debe ser str no {type(guid)} {guid}",
                func=self.contains_guid,
            )
        with self._lock:
            return guid in self._set

    def delete_guid_if_exist(self, guid: str) -> bool:
        """
        Dado un guid lo elimina del set
        True si estaba
        False si no estaba

        Args:
            guid (str): _description_

        Returns:
            bool: _description_
        """
        if not isinstance(guid, str):
            log_message(
                f"Error tratando de eliminar  del set gestor de hilos guid debe ser str no {type(guid)} {guid}",
                func=self.delete_guid_if_exist,
            )
        with self._lock:
            if not guid in self._set:
                log_message(
                    f"Se queria eliminar el guid {guid } pero no estaba en el set",
                    func=self.delete_guid_if_exist,
                )
                return False
            self._set.remove(guid)
            log_message(
                f"Se a eliminado del set el guid {guid}", func=self.delete_guid_if_exist
            )
            return True


class DataReplicatedGestor:
    def __init__(self) -> None:
        self.heap = MinHeap([])
        """
        Heap de mínimos
        """
        self.time_now_: int = 0
        """
        Tiempo actual
        """
        self._time_lock: threading.RLock = threading.RLock()
        """
        Lock para que no haya problemas con los hilos
        """
        self.ok_status: Set = Set()
        """
        El set es para in añadiendo las confirmaciones 
        Si esta en el set => que no puede ser aplicada la función handle
        """

        threading.Thread(target=self._loop, daemon=False).start()  # Inicializar el Hilo

    @property
    def time_now(self) -> int:
        """
        Tiempo actual del heap

        Returns:
            int: _description_
        """
        with self._time_lock:
            return self.time_now_

    @time_now.setter
    def time_now(self, value: int):
        if not isinstance(value, int):
            raise Exception(f"Value debe ser entero no {type(value)} {value}")
        with self._time_lock:
            self.time_now_ = value

    def _increment_time(self):
        """
        Incrementa el tiempo actual en 1
        """
        with self._time_lock:
            self.time_now_ += 1

    def pop_from_heap_and_execute(self) -> list[DeleteHandle]:
        try:
            lis: list[DeleteHandle] = []
            while (
                not self.heap.is_empty() and self.heap.peek().time_ <= self.time_now
            ):  # Si no está vacio y ademas el que esta en la puenta su vencimiento es menor o igual al tiempo actual
                item = self.heap.pop()
                lis.append(item)
                log_message(
                    f"Se va a mandar a ejecutar el evento del contrato {item.document_id} en el tiempo {self.time_now} con guid {item.guid}",
                    func=self.pop_from_heap_and_execute,
                )

                guid = item.guid
                log_message(
                    f"Comprobar si el guid {guid} está en el set",
                    func=self.pop_from_heap_and_execute,
                )
                if self.ok_status.contains_guid(guid):
                    log_message(
                        f"El guid {guid} fue confirmado => que no se ejecutará el execute",
                        func=self.pop_from_heap_and_execute,
                    )
                    if not self.ok_status.delete_guid_if_exist(guid):
                        log_message(
                            f"No se pudo eliminar correctamente el guid {guid} del set",
                            func=self.pop_from_heap_and_execute,
                        )
                    continue
                log_message(
                    f"El guid {guid} no se encuentra en el set => hay que ejecutar el execute",
                    func=self.pop_from_heap_and_execute,
                )
                item.execute()  # Ejecutar el evento
                log_message(
                    f"Se ejecuto el evento de contrato {item.document_id} en el tiempo {self.time_now } con guid {item.guid}",
                    func=self.pop_from_heap_and_execute,
                )
            return lis
        except Exception as e:
            log_message(
                f"Error al extraer los eventos que se cumplen del heap en el tiempo {self.time_now} Error:{e} \n {traceback.format_exc()}",
                func=self.pop_from_heap_and_execute,
            )

    def _loop(self, time_waiting: int = 1):

        while True:
            try:
                time.sleep(time_waiting)  # Esperar un segundo
                to_make = (
                    self.pop_from_heap_and_execute()
                )  # Toma los valores que deben ejecutarse
                log_message(
                    f"Se acabo el tiempo para estos contratos {to_make}",
                    func=self._loop,
                )
                self._increment_time()  # Incrementado el tiempo en Uno

            except Exception as e:
                log_message(
                    f"Error al ejecutar el loop del gestor de evento en el tiempo {self.time_now} Error:{e} \n {traceback.format_exc()}",
                    func=self._loop,
                )

    def add_document_to_the_queue(
        self,
        document: Document,
        handle_fun: Callable[[int, str], None],
        time_waiting: int = 10,
    ) -> str:
        """
            Dado un documento lo añade en la cola para saber cuando se debe eliminar el documento o no
        Args:
            document (Document): _description_
            time_waiting (int): Tiempo a esperar  Default=10
        returns:
            guid:(str) El identificador unico del proceso
        """
        try:
            guid = get_guid()

            def handle():
                handle_fun(document.id, guid)

            time_end = self.time_now + time_waiting
            handle = DeleteHandle(time_end, document.id, handle, guid)
            guid = handle.guid
            log_message(
                f"El documento {document.id} {document.title} se eliminará de no tener confirmación en el tiempo {time_end} estamos en el {self.time_now} guid: {guid}",
                func=self.add_document_to_the_queue,
            )
            self.heap.push(handle)
            return guid
        except Exception as e:
            log_message(
                f"Ocurrion un error tratando de añadir el documento {document.id} {document.title} en la cola de eventos en el tiempo {self.time_now} Error: {e} \n {traceback.format_exc()} ",
                func=self.add_document_to_the_queue,
            )

    def update_document(
        self,
        new_document: Document,
        old_document: Document,
        old_owner: int,
        handle_fun: Callable[[int, str], None],
        time_waiting: int = 10,
    ) -> str:
        """
        Dado el documento a upgradear : new document y el viejo : old_document
        si en el tiempo dicho no se a actualizado a tiempo la información
        se devuelve al anterior version

        Args:
            new_document (Document): _description_
            old_document (Document): _description_
             old_owner:int El nodo dueño del archivo antes del update
            handle_fun (Callable[[int, str], None]): _description_
            time_waiting (int, optional): _description_. Defaults to 10.

        Returns:
            guid str: identificador unico del proceso

        """
        if old_document.id != new_document.id:
            raise Exception(
                f"El documento nuevo tiene id {new_document.id} y el antiguo tiene {old_document.id} son distintos Error"
            )
        try:
            guid = get_guid()

            def handle():  # document_id: int, event_guid: str, old_document: Document, old_owner: int
                handle_fun(old_document.id, guid, old_document, old_owner)

            time_end = self.time_now + time_waiting
            handle = DeleteHandle(time_end, old_document.id, handle, guid)
            guid = handle.guid
            log_message(
                f"El documento {old_document.id} {old_document.title} se volverá al estado anterior  de no tener confirmación en el tiempo {time_end} estamos en el {self.time_now} guid: {guid}",
                func=self.add_document_to_the_queue,
            )
            self.heap.push(handle)
            return guid
        except Exception as e:
            log_message(
                f"Ocurrion un error tratando de añadir el documento {old_document.id} {old_document.title} en la cola de eventos en el tiempo {self.time_now} Error: {e} \n {traceback.format_exc()} ",
                func=self.add_document_to_the_queue,
            )

    def confirm_guid(self, guid: str):
        """
        Es que se acepto la transacción por tanto no se ejecutará la función handle

        Args:
            guid (str): _description_
        """
        self.ok_status.add_guid(guid)
