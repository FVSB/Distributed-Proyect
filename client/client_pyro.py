
import Pyro5.api
import socket
from logguer import log_message
ip = socket.gethostbyname(socket.gethostname())
#daemon= Pyro5.server.Daemon(ip)# Crear un nuevo demonio para este hilo
#proxy=Pyro5.api.Proxy
ns = Pyro5.api.locate_ns()
log_message("Esta actuvo")
entries = ns.list()

# Muestra todas las URLs (URIs) registradas
for name, uri in entries.items():
    log_message(f"Name: {name}, URI: {uri}")
def get_remote_objet(url:str,proxy=None):
    """Devuelve el objeto remoto dada una url
        Puede lanzar excep si no la url se desconecto
    Args:
        url (str): _description_
    """
    ns = Pyro5.api.locate_ns()
    log_message("Esta activo")
    uri = ns.lookup(url)
    log_message("Encontro la uri")
    if proxy is None:
        return  Pyro5.api.Proxy(uri)
        
    return proxy(uri)




node=get_remote_objet('search.search')
log_message("Tengo el objeto")
log_message(type(node))
log_message(node.get_nodes_ips())