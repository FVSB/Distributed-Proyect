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
                item.execute()  # Ejecutar el evento
                log_message(
                    f"Se ejecuto el evento de contrato {item.id} en el tiempo {self.time_now } con guid {item.guid}",
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