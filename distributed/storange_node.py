from chord_lider import *
from flask import Flask, request, jsonify, Response, abort, redirect, url_for
import socket
import jsonpickle
from helper.docs_class import *
from helper.utils import *
import helper.db as db
from enum import Enum
from urllib.parse import urlencode
import requests as rq
app = Flask(__name__)
app.logger.disabled = (
    True  # Desactivar el  logguer de flask para que no haya conflicto con el mio
)
log = logging.getLogger("werkzeug")
log.disabled = True  # Desactivar los otros posibles logguers que no sea el mio


class StoreNode(Leader):
    def __init__(self, ip: str, port: int = 8001, m: int = 160):
        super().__init__(ip, port, m)

        self.setup_routes()

    def setup_routes(self):
        # Registrar la ruta y vincularla al método de instancia
        app.add_url_rule("/upload", view_func=self.upload_file, methods=["POST"])
        app.add_url_rule(
            "/get_document_by_name", view_func=self.get_file_by_name, methods=["GET"]
        )
        app.add_url_rule(
            "/save_document_like_replica",
            view_func=self.save_document_like_replica,
            methods=["POST"],
        ) # Endpoint para guardar los documentos en las replicas

    def start_threads(self):
        """Inicia todos los hilos

        Returns:
            _type_: _description_
        """

        super().start_threads()
        threading.Thread(
            target=lambda: app.run(host=self.ip, port=8000), daemon=True
        ).start()  # Iniciar servidor por el puerto 8000

    def url_from_ip(self, ip: str):
        """Dado una ip devuelve su url de flask correspondiente

        Args:
            ip (str): la ip

        Raises:
            Exception: Si la ip no es un str

        Returns:
            str: Devuelve la url para el nodo en flask que sale por el puerto 8000
        """
        if not isinstance(ip, str):
            log_message(
                f"La ip debe ser un str no un {type(ip)}, {ip}", func=self.url_from_ip
            )
            raise Exception(f"El ip debe ser un str no un {type(ip)}, {ip}")
        return f"http://{ip}:8000"

    def add_end_point_to_url(self, url: str, end_point: str) -> str:

        return f"{url}/{end_point}"

    def save_in_my_replicas(self, document: Document) -> bool:
        try:
            for replica in self.succ_list:
                if replica.id==self.id:continue # Nunca me mando a guardar como replica a mi mismo
                url = self.url_from_ip(replica.ip)
                url = self.add_end_point_to_url(url, "save_document_like_replica")
                data=pickle.dumps((self.ref,document))
                log_message(f'Se va a enviar a guarda en la replica {replica.id} el documento {document.id} url:{url}',func=self.save_in_my_replicas)
                file={'file':data}
                response = rq.post(url, files=file)
                if response.status_code !=200:
                    log_message(f'Enviando a guardadar a la replica {replica.id} el archivo {document.id} no se ha recibido 200 se recibio {response.status_code}')
                    return False
                log_message(f'Se guardo exitosamente el documento {document.id} en la replica {node.id}',func=self.save_in_my_replicas)
                
                
                
            log_message(f'Se guardó satisfactoriamente en mi replicas el documento {document.id}',func=self.save_in_my_replicas)
            return True
        except Exception as e: 
            log_message(f'Ocurrio un error guardando en las replicas el documento {document.id} Error:{e} \n {traceback.format_exc()} ',func=self.save_in_my_replicas)
            return False
    def save_document(self, document: Document) -> bool:
        """
         Realiza la transacción atomica de guardar un documento en los nodos replica

        Args:
            document (Document): _description_

        Returns:
            bool: _description_
        """
        name = document.title
        # Primero llamar a mis nodos replicas para que traten de guardar la información
        log_message(f'Llamando a mis replicas para que me guarden el archivo {document.id}',func=self.save_document)
        if not self.save_in_my_replicas(document):# Chequear que se haya guardado en mis replicas
            log_message(f'No se pudo guardar el documento {document.id} las replicas ', func=self.save_document)
            return False
        log_message(f'El documento {document.id}, titulo {document.title} se guardó exitosamente en las replicas',func=self.save_document)
            
        db.insert_document(
            document, self.id
        )  # Se trata de insertar un documento en la base de datos
        log_message(
            f"El archivo con nombre {name} se a guardado correctamente en la base de datos",
            func=self.save_document,
        )
        return True

    def get_data_from_request(self) -> bytes:
        """
            Retorna los bytes con lo que envio el cliente

        Returns:
            bytes: _description_
        """
        # El cliente manda (Nombre archivo, archivo)
        file = request.files["file"]

        data = file.stream.read()

        return data

    # EndPoint SaveDocumentReplica
    def save_document_like_replica(self):
        """
        End Point para guardar documento siendo una réplica 
        """
        addr_from = request.remote_addr
        log_message(
            f"Se a mandado a guardar un archivo como replica que envio el addr: {addr_from} ",
            func=self.save_document_like_replica,
        )

        data = self.get_data_from_request()  # Retornar los bytes con la data
        if data is None:  # Es que no se envió nada
            # Retornar error de no file
            log_message(f"La data no puede ser null", func=self.get_data_from_request)
            return jsonify({"message": 'Bad Request: Parámetro "param" requerido'}), 400

        node, doc = pickle.loads(data)
        doc: Document = doc
        node: ChordNodeReference = node
        # TODO: Primero añadirlo al evento cuando se pueda

        # Guardar en la db con ese nodo

        # Verificar que no hay otro guardado y si está guardado ver el historial
        # Si es mas antiguo que el que yo tenia lanzo exepcion y le envio 400

        # Chequear que no hay mas ninguno en la db
        doc_id = doc.id
        if db.has_document(doc_id):
            log_message(
                f"Ya se tiene guardado el documento {doc.title} con id {doc_id}",
                func=self.save_document_like_replica,
            )
            if db.update_document(
                doc, node.id
            ):  # Retornar que se hizo el cambio exitosamente
                log_message(
                    f"Se realizo el cambio del documento {doc.id} con titulo {doc.title} del que es dueño el nodo {node.id}",
                    func=self.save_document_like_replica,
                )
                return jsonify({"code": SAVE_DOC_WAITING_OK}), 200
            else:
                log_message(
                    f"Hubo un error tratando de actualizar el documento con id {doc_id} de dueño {node.id}",
                    func=self.save_document_like_replica,
                )
        else:  # Es que no existe el documento en la base de datos
            db.insert_document(
                doc, node.id, False
            )  # Se añade en la base de datos y decimos que ahora no es persistente, esperamos confirmación
            log_message(
                f"Se inserto correctamente el documento {doc.id} que es dueño el nodo {node.id}",
                func=self.save_document_like_replica,
            )
            return jsonify({"code": SAVE_DOC_WAITING_OK}), 200

    # Endpoint upload_file
    def upload_file(self):
        addr_from = request.remote_addr
        log_message(
            f"Se a mandado a guardar un archivo que envio el addr: {addr_from} ",
            func=self.upload_file,
        )
        # Nombre del archivo , str con el archivo
        doc_to_save = self.get_data_from_request()  # Tomar los bytes de la data

        if doc_to_save is None:  # Es que no se envió nada
            # Retornar error de no file
            return jsonify({"message": 'Bad Request: Parámetro "param" requerido'}), 400

        name, doc_to_save = pickle.loads(doc_to_save)
        log_message(f"La data es {name}{doc_to_save}", func=self.upload_file)

        log_message(f"El archivo tiene nombre {name}", func=self.upload_file)
        hash_name = getShaRepr(name)  # Hashear el nombre dado que esta sera la llave

        # Mandar a guardar en la base de datos

        # Comprobar que no esta en la base de datos
       
        doc = db.get_document_by_id(hash_name)
        log_message(f"El tipo de doc es {type(doc)}", func=self.upload_file)
        if doc is not None:
            log_message(
                f"El documento con nombre {name} ya estaba guardado ",
                func=self.upload_file,
            )
            return (
                jsonify(
                    {"message": f"Conflict: El archivo {name} ya existe en el servidor"}
                ),
                409,
            )

        # Guardar en la base de datos
        try:
            if self.save_document(Document(name, doc_to_save)):
                return ( jsonify(
                             {
                                 "message": f"El documento con nombre {name} se guardo correctamente"
                             }
                         ),
                         200,)

            # try:
            #     if self.save_document(
            #         Document(name, doc_to_save)
            #     ):  # funcion que hace la transaccion atómica
            #         (
            #             jsonify(
            #                 {
            #                     "message": f"El documento con nombre {name} se guardo correctamente"
            #                 }
            #             ),
            #             200,
            #         )
        
        except Exception as e:
            log_message(
                f" Hubo un error tratando de guardar el archivo {name} \n {e} \n {traceback.format_exc()}",
                func=self.upload_file,
            )

            return (
                jsonify(
                    {"message": f"Ocurrio un problema guardando el documento {name}"}
                ),
                500,
            )

    # EndPoint get_file_by_name
    def get_file_by_name(self):
        """
        Es el endpoint para devuelver un documento dado su nombre  el endpoint es "get_document_by_name"
        """
        addr_from = request.remote_addr  # La direccion desde donde se envia la petición

        log_message(
            f"Se a recibido una petición de GET para el para devolver un documento desde la dirección {addr_from}",
            func=self.get_file_by_name,
        )
        try:
            name = str(request.args.get("name", None))
            start = int(
                request.args.get("start", 1)
            )  # Paquete por donde empezar la descarga

            if name is None:  # Debe enviar al menor un nombre de archivo
                log_message(
                    f"Se envio a pedir el documento {name}, desde el paquete {start}",
                    func=self.get_file_by_name,
                )
                return jsonify(
                    {"message": "Se esperaba que el nombre no fuera None"}, 400
                )

            # Verificar que yo soy el dueño de ese id
            key = getShaRepr(name)
            node = self.find_key_owner(key)
            log_message(
                f"El nodo que debe tener el documento con nombre {name} y llave {key} es el nodo {node.id}"
            )
            if node.id != self.id:  # Redirecciono al nodo que es dueño de la llave
                red_ip = self.url_from_ip(node.ip)
                log_message(
                    f"Se va a rederigir al nodo con id {node.id} para realizar la busqueda del archivo {name} con el start {start}",
                    func=self.get_file_by_name,
                )
                url = self.add_end_point_to_url(red_ip, "get_document_by_name")
                params = {"name": name, "start": start}
                url = f"{url}?{urlencode(params)}"
                log_message(
                    f"La url para redireccionar la peticion del documento {name} con llave {key} es {url}",
                    func=self.get_file_by_name,
                )
                return redirect(url)

            doc = db.get_document_by_id(getShaRepr(name))

            if doc is None:  # Si es None es pq no está en la DB
                log_message(
                    f"El documento con nombre {name} no se encuentra en la base de datos",
                    func=self.get_file_by_name,
                )
                return jsonify(
                    {
                        "message": f"El documento con nombre {name} no se encuentra en la base de datos"
                    },
                    409,
                )

            log_message(
                f"Se a recuperado exitosamente el documento {doc.title} dado que se habia pedido el {name} desde el paquete {start}",
                func=self.get_file_by_name,
            )

            data_bytes = doc.get_in_bytes()  # Mandarlo a bytes
            log_message(
                f"El documento con nombre {name} tiene de texto {doc.text}",
                func=self.get_file_by_name,
            )
            data_bytes = pickle.dumps(doc.text)

            chunk_size = 1024  # Tamaño de cada paquete (1 KB)

            total_length = len(data_bytes)

            if total_length == 0:
                abort(404, description="No data available")

            def generate():
                part_number = 1
                for i in range(0, total_length, chunk_size):
                    chunk = data_bytes[i : i + chunk_size]
                    if part_number >= start:
                        paquete = Paquete(part_number, chunk, False)
                        yield paquete.serialize()
                    part_number += 1

            log_message(
                f"Se va a enviar el documento con nombre {name} y data {doc.text}",
                func=self.get_file_by_name,
            )
            return Response(generate(), content_type="application/octet-stream")
        except Exception as e:
            log_message(
                f" A ocurrido un error {e} tratando de dar en el endpoint de devulver un archivo por nombre {traceback.format_exc()}",
                func=self.get_file_by_name,
            )


if __name__ == "__main__":
    print("Hello from Storage node")
    # time.sleep(10)
    ip = socket.gethostbyname(socket.gethostname())
    node = StoreNode(ip, m=3)

    node.start_threads()  # Iniciar los nodos
    while True:
        pass
