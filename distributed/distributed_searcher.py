

from distributed.distributed_data_base import *
from helper.docs_class import EmbeddingDocument
from helper.embedding_generator import create_embedding
from helper.query_handle import QueryHandle
class DistributedSearcher(DistributedDataBase):
    
    def __init__(self, ip: str,func_get_embedding_list:list[np.array], port: int = 8001, flask_port: int = 8000, m: int = 160):
        super().__init__(ip, port, flask_port, m)
        self.func_get_embedding_list:list[np.array]=func_get_embedding_list
        """
        Funcion que recibe el texto y devuelve una lista de array de numpy con los embeddings
        """
        self.query_admi:ThreadingSet=ThreadingSet()
    
    
    def create_document(self,title: str, text: str, max_value: int = 16) -> Document:
        """
        crea el embeeding document apartir del titulo y texto

        Args:
            title (str): _description_
            text (str): _description_
            max_value (int, optional): _description_. Defaults to 16.

        Returns:
            Document: _description_
        """
        return EmbeddingDocument(title=title,text=text,max_value=max_value,embedding_list=self.func_get_embedding_list(text),embedding_title_list=self.func_get_embedding_list(title))

    def resolve_query(self,query:str,posibles_extensions:list[str]):
        docs:list[EmbeddingDocument]=db.get_all_documents_by_extension(posibles_extensions,self.id)
        
    
    
    
    def query(self):# Endpoint para hacer una query
        addr_from = request.remote_addr  # La direccion desde donde se envia la petición

        
        
        log_message(
            f"Se a recibido una petición de GET para el para resolver una query desde la direccion: {addr_from}",
            func=self.get_file_by_name,
        )
        try:
            query:str=str(request.arg.get("name",""))
            posibles_extensions:list[str]=request.args.getlist("extensions")
            
            self.wait_for_stability()
            
            log_message(f"Se quiere realiza una query {query}  a las extensiones {posibles_extensions}",func=self.query)
            
            
            
            
        except Exception as e:
            log_message(f"A ocurrido un error en la peticion del addr: {addr_from} Error:{e} \n {traceback.format_exc()}",func=self.query)
            return jsonify({"message":"A ocurrido un error en la peticion"}),500
        


if __name__ == "__main__":
    log_message("Hello from Distribued Seacher node")
    ip = socket.gethostbyname(socket.gethostname())
    node = DistributedSearcher(ip, m=3,func_get_embedding_list=create_embedding)
    node.start_node()  # Iniciar el pipeline

    while True:
        pass