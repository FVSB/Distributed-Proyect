from storange_node import *


#app = Flask(__name__)
#app.logger.disabled = (
#    True  # Desactivar el  logguer de flask para que no haya conflicto con el mio
#)
#log = logging.getLogger("werkzeug")
#log.disabled = True  # Desactivar los otros posibles logguers que no sea el mio


class SyncStoreNode(StoreNode):
    def __init__(self, ip: str, port: int = 8001, flask_port: int = 8000, m: int = 160):
        super().__init__(ip, port, flask_port, m)
        self.is_sync_data_: bool = False
        """
        True Si la data esta sincronizada => que ya despues de la ultima eleccion
        los nodos responsables de cada archivo se hicieron cargo
        """
        self.is_sync_data_lock_: threading.RLock = threading.RLock()
        """
        Lock para saber si la data esta sincronizada o no
        """
        self.can_other_sync_data_with_me_: bool = False
        """
        True si en la resincronización puedo recibir data
        False si todavia no he hecho no persistente mis datos
        """
        self.can_other_sync_data_with_me_lock: threading.RLock = threading.RLock()

    def data_to_print(self):
        super().data_to_print()
        log_message(
            f"Los datos están sincronizados {self.is_sync_data}", func=self.show
        )
        log_message(
            f"Se pueden sincronizar los datos {self.can_other_sync_data_with_me}",
            func=self.data_to_print,
        )

    @property
    def can_other_sync_data_with_me(self):
        """
         True si en la resincronización puedo recibir data
        False si todavia no he hecho no persistente mis datos
        Es seguro ante hilos
        Returns:
            _type_: _description_
        """
        with self.can_other_sync_data_with_me_lock:
            return self.can_other_sync_data_with_me_

    @can_other_sync_data_with_me.setter
    def can_other_sync_data_with_me(self, value: bool):
        if not isinstance(value, bool):
            raise Exception(
                f"Se esperaba que value fuera Bool no {type(value)} {value}"
            )
        with self.can_other_sync_data_with_me_lock:
            self.can_other_sync_data_with_me_ = value

    @property
    def is_sync_data(self) -> bool:
        """
        Retorna de forma segura entre hilos
        Si la data esta sincronizada True
        Si se esta sincronizando False

        Returns:
            bool: _description_
        """
        with self.is_sync_data_lock_:
            return self.is_sync_data_

    @is_sync_data.setter
    def is_sync_data(self, value):
        """
        Setea con seguridad en hilos los valores de value

        Args:
            value (_type_): _description_

        Raises:
            Exception: _description_
        """
        if not isinstance(value, bool):
            raise Exception(f"Value tiene que ser bool no {type(value)} {value}")
        with self.is_sync_data_lock_:
            self.is_sync_data_ = value

    def start_threads(self):
        super().start_threads()
        threading.Thread(
            target=self.check_need_sync_store_data, daemon=True
        ).start()  # Está revisando si es necesario sincronizar con la red la data
        threading.Thread(
            target=self.check_can_other_sync_data_with_me, daemon=True
        ).start()  # Chequea si en caso ser inestable puede insertar valores

    def setup_routes(self):
        super().setup_routes()
        app.add_url_rule(
            "/sync_keys_from_others",
            view_func=self.sync_keys_from_others,
            methods=["POST"],
        )  # Metodo que recibe los documentos que ahora le pertenecen en la resincronización
        ###########################
        #                         #
        #       Endpoints         #
        #                         #
        ###########################

    # Endpoint can_recive_update_documents
    def can_recive_update_documents(self):
        """
        Endpoints que dice si se puede recibir o no documentos
        True si es que Estoy con can_other_sync_data_with_me En true
        Sino espero 30 vueltas preguntando sino Lanzo error
        """
        try:
            addr_from = request.remote_addr
            time_ = 0.5
            for i in range(30):
                try:
                    time.sleep(time_)
                    if not self.can_other_sync_data_with_me:
                        log_message(
                            f"En la iteración {i} no se a podido completar el pedido de {addr_from}",
                            func=self.can_recive_update_documents,
                        )
                        continue

                    log_message(
                        f"Se puede sincronizar los datos en la iteración {i} preguntado por el addr{ addr_from}",
                        func=self.can_recive_update_documents,
                    )
                    return Response("Puede sincronizar", status=HTTPStatus.OK)

                except Exception as e:
                    log_message(
                        f"Ocurrio un error tratando de responder si puedo recibir documentos de resincronización en la iteracion {i+1} Error:{e} \n {traceback.format_exc()}",
                        func=self.can_recive_update_documents,
                    )
            log_message(
                f"No se pudo completar en ninguna iteración la respuesta de si pueden sincronizar conmigo para el addr {addr_from} ",
                func=self.can_recive_update_documents,
            )
            return Response(
                "Error no se ha podido sincronizar", status=HTTPStatus.FORBIDDEN
            )
        except Exception as e:
            log_message(
                f"Ocurrio un error tratando de responder si puedo recibir documentos de resincronización de forma general Error:{e} \n {traceback.format_exc()}",
                func=self.can_recive_update_documents,
            )
            raise Exception(e)

    # EndPoint sync_keys_from_others
    def sync_keys_from_others(self):
        """
        Endpoint para en la resincronizacion recibir los documentos que ahora son mios

        """
        try:

            addr_from = request.remote_addr

            log_message(
                f"Se ha mandado a reinsertar un documento siendo yo el dueño actual de {addr_from} ",
                func=self.sync_keys_from_others,
            )

            data = self.get_data_from_request()  # Document
            if data is None:
                log_message(f"La data es None", func=self.sync_keys_from_others)

            tup: tuple[ChordNodeReference, Document] = pickle.loads(data)
            node, document = tup
            doc_id = document.id
            # Verificar si ya lo tengo en la db
            if db.has_document(
                doc_id
            ):  # Si esta en la base de datos comprobar que este es mas reciente o igual
                old_doc = db.get_document_by_id(doc_id)
                if not old_doc.record.can_update(
                    document.record
                ):  # Devolver que me ocupe de el
                    log_message(
                        f"El documento con id {doc_id} que se a enviado desde {node.id} es mas viejo: {document.record} que el que tengo en la db:{old_doc.record}",
                        func=self.sync_keys_from_others,
                    )
                    return (
                        jsonify(
                            {
                                f"message": f"El documento con id {doc_id} que se a enviado desde {node.id} es mas viejo: {document.record} que el que tengo en la db:{old_doc.record}"
                            }
                        ),
                        HTTPStatus.OK,
                    )
                ok_, nodes_list = self.Crud_action(
                    document, "save_document_like_replica", CrudCode.ReInsertSelf
                )
                if not ok_:
                    log_message(
                        f"No se pudo reinsertar correctamente el documento {doc_id} que viene del nodo {node.id} {addr_from} Se pudo insertar en {nodes_list}",
                        func=self.sync_keys_from_others,
                    )
                    return (
                        jsonify(
                            {
                                "message": f"No se pudo reinsertar correctamente el documento {doc_id} que viene del nodo {node.id} {addr_from} Se pudo insertar en {nodes_list}"
                            }
                        ),
                        HTTPStatus.EXPECTATION_FAILED,
                    )

                log_message(
                    f"Se pudo reinsertar correctamente el documento {doc_id} que viene del nodo {node.id} {addr_from} Se pudo insertar en {nodes_list}",
                    func=self.sync_keys_from_others,
                )
                return (
                    jsonify(
                        {
                            f"message": f"Se pudo reinsertar correctamente el documento {doc_id} que viene del nodo {node.id} {addr_from} Se pudo insertar en {nodes_list}"
                        }
                    ),
                    HTTPStatus.OK,
                )
            else:  # Si no esta el documento => que hay que insertarlo desde Cero
                log_message(
                    f"El documento {doc_id} enviado desde {node.id} {addr_from} No existe actualmente en la db => hay que insertarlo por primera vez",
                    func=self.sync_keys_from_others,
                )
                ok_, nodes_list = self.Crud_action(
                    document, "save_document_like_replica", CrudCode.Insert
                )
                if not ok_:  # Si no se completo satisfactoriamente
                    log_message(
                        f"No se pudo reinsertar correctamente el documento el cual se iba a insertar por primera vez {doc_id} que viene del nodo {node.id} {addr_from} Se pudo insertar en {nodes_list}",
                        func=self.sync_keys_from_others,
                    )
                    return (
                        jsonify(
                            {
                                "message": f"No se pudo reinsertar correctamente el documento el cual se iba a insertar por primera vez {doc_id} que viene del nodo {node.id} {addr_from} Se pudo insertar en {nodes_list}"
                            }
                        ),
                        HTTPStatus.EXPECTATION_FAILED,
                    )

                log_message(
                    f"Se pudo reinsertar correctamente el documento el cual se inserta acá por primera vez id: {doc_id} que viene del nodo {node.id} {addr_from} Se pudo insertar en {nodes_list}",
                    func=self.sync_keys_from_others,
                )
                return (
                    jsonify(
                        {
                            f"message": f"Se pudo reinsertar correctamente el documento el cual se inserta acá por primera vez id: {doc_id} que viene del nodo {node.id} {addr_from} Se pudo insertar en {nodes_list}"
                        }
                    ),
                    HTTPStatus.OK,
                )
        except Exception as e:
            log_message(
                f"Ocurrio un error en el endopoint de recibir los documentos en la resincronización  Error:{e} {traceback.format_exc()}",
                func=self.sync_keys_from_others,
            )
            raise Exception(e)

    def get_all_keys_mine_and_the_other(self) -> tuple[list[int], list[int]]:
        """
        Toma de la db todas las llaves, hace todos las filas como No Persistente y devuelve una tupla que tiene (Lista de las llaves que me pertenecen ahora, Lista de llaves que no Me pertenecen )

        Returns:
            tuple[list[int], list[int]]: (My_Keys,Other_Kyes)
        """

        log_message(
            f"Se va a revisar ahora cuales son las llaves que me pertenecen y cuales tengo que reinsertar"
        )
        my_keys: list[int] = []  # Las llaves que me pertenecen ahora
        other_owners_keys: list[int] = []  # Las llaves que le pertenecen a otro ahora
        # Tomar todas las llaves
        keys_lis: list[int] = db.get_all_docs_keys()
        # Hacer no persistente todo
        db.make_false_persist_all_rows()

        if self.i_am_alone:  # Es pq soy un unico nodo
            log_message(
                f"Estoy solo => que todas las llaves en mi db son mias",
                func=self.get_all_keys_mine_and_the_other,
            )

            return (keys_lis, [])  # Todas las llaves son mias

        for key in keys_lis:
            if self._inbetween(key, self.pred.id, self.id):  # Cuales son mis llaves
                my_keys.append(key)
            else:  # LLaves que son dueño otro nodo
                other_owners_keys.append(key)
        return (my_keys, other_owners_keys)

    def reinsert_my_keys(self, my_keys: list[int]) -> bool:
        # Mandar a reinsertar cada llave
        try:
            with self.succ_list_lock:  # Se bloquea que ningun otro hilo pueda modificarlo
                for key in my_keys:
                    try:

                        document = db.get_document_by_id(key)
                        ok_, replica = self.Crud_action(
                            document,
                            "save_document_like_replica",
                            CrudCode.ReInsertSelf,
                        )  # El va a reinsertar el documento
                        a = self.succ_list
                        a += [self.ref]
                        if not (
                            ok_ and is_equal_list(a, replica)
                        ):  # Si hubo algun error haciendo la inserccion hubo un error por tanto se tiene que volver otra vez la operacion
                            log_message(
                                f"No se pudo reinsertar correctamente la llave {key} se tenia que reinsertar en {self.succ_list} y se reinserto en {replica}",
                                func=self.reinsert_my_keys,
                            )
                            return False

                    except Exception as e:
                        log_message(
                            f"Ocurrio un error reinsertando las llave como mia {key }  Error:{e} \n {traceback.format_exc()}",
                            func=self.reinsert_my_keys,
                        )
                        return False
                return True  # Se completo la reinserccion
        except Exception as e:
            log_message(f"Error reinsenrtando mis llaves", func=self.reinsert_my_keys)
            return False

    def send_to_reinsert_others_documents(self, keys: list[id]) -> bool:
        """
        Envia a reinsertar los documentos que no son de mi propiedad, despues que tiene la aceptación si no es persistente
        el documento lo elimina

        Args:
            keys (list[id]): lista con los ids de documentos que no me pertenecen

        Returns:
            bool: True si se completo con exito  False si hubo algun problema o fallo enviar algun archivo

        """
        try:
            for key in keys:  # Iterar por las llaves

                if not self.is_stable:  # Si empece otra vez en elección retorno False
                    log_message(
                        f"Tratando de enviar la llave {key} a su dueño entramos en eleccion {self.in_election} es estable{self.is_stable}",
                        func=self.send_to_reinsert_others_documents,
                    )
                    return False
                owner = self.find_key_owner(key)  # Buscar el nodo dueño
                if (
                    owner.id == self.id
                ):  # Debe ser un error pq yo ya debo haber tomado todas mis llaves
                    log_message(
                        f"Error no puedo decir que envio a otro nodo a encargarse del documento {key} cuando yo soy el dueño {owner.id} {owner.ip}",
                        func=self.send_to_reinsert_others_documents,
                    )
                    raise Exception(
                        f"Error no puedo decir que envio a otro nodo a encargarse del documento {key} cuando yo soy el dueño {owner.id} {owner.ip}"
                    )

                document = db.get_document_by_id(key)
                if document is None:
                    log_message(
                        f"Error el documento con llave {key} es {type(document)} no puede ser None => que se elimino antes en la db ",
                        func=self.send_to_reinsert_others_documents,
                    )
                    raise Exception(
                        f"Error el documento con llave {key} es {type(document)} no puede ser None => que se elimino antes en la db "
                    )

                response_: Response = self.send_file_to_node(
                    owner, "sync_keys_from_others", pickle.dumps((self.ref, document))
                )
                if (
                    response_.status_code != 200
                ):  # Si no ocurrio un 200 hubo algun error lanzar exc
                    log_message(
                        f"Ocurrio un Error enviando al nodo {owner.id} {owner.ip} el documento {key} con fecha {document.record} codigo de estatus {response_.status_code}",
                        func=self.send_to_reinsert_others_documents,
                    )
                    raise Exception(
                        f"Ocurrio un Error enviando al nodo {owner.id} {owner.ip} el documento {key} con fecha {document.record} codigo de estatus {response_.status_code}"
                    )

                log_message(
                    f"El documento {key} fue correctamente enviado a su dueño {owner}",
                    func=self.send_to_reinsert_others_documents,
                )

                is_persistent = db.is_document_persistent(
                    key
                )  # Chequear si ya es persistente

                if is_persistent is None:
                    log_message(
                        f"Error revisando si el documento {document} es persistente en la db Este no existe en esta y deberia existir",
                        func=self.send_to_reinsert_others_documents,
                    )
                    raise Exception(
                        f"Error revisando si el documento {document} es persistente en la db Este no existe en esta y deberia existir"
                    )

                if (
                    not is_persistent
                ):  # => que yo no soy replica despues de la reinserción => que hay que eliminarlo
                    log_message(
                        f"El documento {key} no es persistente despues de enviarlo a su nuevo dueño {owner} por tanto => se va a  eliminar",
                        func=self.send_to_reinsert_others_documents,
                    )
                    db.delete_document_all(key)
                    log_message(
                        f"El documento {key} no es persistente despues de enviarlo a su nuevo dueño {owner} por tanto se acaba de eliminar",
                        func=self.send_to_reinsert_others_documents,
                    )

                log_message(
                    f"El documento {key} fue adecuadamente reinsertado",
                    func=self.send_to_reinsert_others_documents,
                )

            return True
        except Exception as e:
            log_message(
                f"Ocurrio un error tratando de enviar a los dueños actuales de las documentos sus respectivos documentos Error:{e} \n {traceback.format_exc()} ",
                func=self.send_to_reinsert_others_documents,
            )
            return False

    def sync_data(self, intent: int):
        """
        Sincronizar mi data
        """

        for i in range(intent):
            try:

                log_message(
                    f"Tratando de sincronizar la data despues de una eleccion intento {i+1}",
                    func=self.sync_data,
                )
                # Tomar las llaves que me pertenecen o no
                my_keys, other_keys = self.get_all_keys_mine_and_the_other()

                self.can_other_sync_data_with_me = True
                log_message(
                    f"Ya se puede recibir data para sincronizar", func=self.sync_data
                )

                # Reinsertar mis datos
                if not self.reinsert_my_keys(
                    my_keys
                ):  # Si no se puede reinsertar => que hay que volver a resincronizar
                    log_message(
                        f"Ocurrio un error reinsertando mis propias llaves {my_keys}",
                        func=self.sync_data,
                    )
                    continue

                if not self.send_to_reinsert_others_documents(
                   other_keys
                ):  # => Que hubo algun problema
                   log_message(
                       f"No se pudo enviar a los nuevos dueños de las llaves {other_keys} todas las llaves",
                       func=self.sync_data,
                   )
                   continue
            
                log_message(
                    f"Se completo la sincronización de la data", func=self.sync_data
                )
                return True
            except Exception as e:
                log_message(
                    f"Ocurrio un error resincronizando la data {e} \n {traceback.format_exc()}",
                    func=self.sync_data,
                )
        log_message(
            f"En ninguno de los intentos se pudo sincronizar la data ",
            func=self.sync_data,
        )
        return False

    def check_need_sync_store_data(self, time_: int = 1):
        """
        Este método chequea si se tiene que resincronizar los datos
        """
        # Si estoy en elección convoco a esto
        was_in_election = True  # Es el indicador para saber si estuve en eleccion y esperar a no estar en esta
        while True:
            try:
                time.sleep(time_)
                if (
                    was_in_election
                    and not self.in_election
                    and self.succ_list_ok
                    and self.is_stable
                ):  # Si estaba en eleccion, ahora no estoy en elección y la lista de sucesores esta ok

                    # Mandar a resincronizar la data
                    self.is_sync_data = False
                    log_message(
                        f"Se va a enviar a sincronizar los datos",
                        func=self.check_need_sync_store_data,
                    )

                    if not self.sync_data(2):
                        log_message(
                            f"No se pudo sincronizar la data",
                            func=self.check_need_sync_store_data,
                        )
                        continue
                    else:  # => Se sincronizo correctamente
                        log_message(
                            f"Se sincronizaron los datos correctemente",
                            func=self.check_need_sync_store_data,
                        )
                        self.is_sync_data = True

                    was_in_election = False  # Establecer como false para que se vuelva a poder cambiar
                    # Aca poner que estoy ready en respecto a mi data
                was_in_election = (
                    self.in_election if not was_in_election else was_in_election
                )  # Actualizar si ahora estoy en elección

            except Exception as e:
                log_message(
                    f"Ocurrio un error chequeando si debo resincronizar los datos Error:{e} \n {traceback.format_exc()}",
                    func=self.check_need_sync_store_data,
                )
                # was_in_election=True # Por si acaso mandar a resincronizar

    def check_can_other_sync_data_with_me(self, time_: float = 0.1):
        """
        Chequea que cuando haya eleccion o este algo inestable en el nodo diga que no se puede sincronizar los datos

        """
        while True:
            try:
                time.sleep(time_)
                if self.in_election or not (self.is_stable and self.succ_list_ok):
                    self.can_other_sync_data_with_me = False

            except Exception as e:
                log_message(
                    f"Ocurrio un error tratando de chequear que en caso de resincronización puedo recibir data, Error:{e} \n {traceback.format_exc()} ",
                    func=self.check_can_other_sync_data_with_me,
                )


if __name__ == "__main__":
    log_message("Hello from Sync Storage node")
    # time.sleep(10)
    ip = socket.gethostbyname(socket.gethostname())
    node = SyncStoreNode(ip, m=3)

    #node.start_threads()  # Iniciar los nodos
    node.start_node() # Iniciar el pipeline
    
    while True:
        pass
