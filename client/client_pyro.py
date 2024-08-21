
import Pyro5.api
import socket
ip = socket.gethostbyname(socket.gethostname())
#daemon= Pyro5.server.Daemon(ip)# Crear un nuevo demonio para este hilo
#proxy=Pyro5.api.Proxy
ns = Pyro5.api.locate_ns()
print("Esta actuvo")
entries = ns.list()

# Muestra todas las URLs (URIs) registradas
for name, uri in entries.items():
    print(f"Name: {name}, URI: {uri}")
def get_remote_objet(url:str,proxy=None):
    """Devuelve el objeto remoto dada una url
        Puede lanzar excep si no la url se desconecto
    Args:
        url (str): _description_
    """
    ns = Pyro5.api.locate_ns()
    print("Esta activo")
    uri = ns.lookup(url)
    print("Encontro la uri")
    if proxy is None:
        return  Pyro5.api.Proxy(uri)
        
    return proxy(uri)




node=get_remote_objet('search.search')
print("Tengo el objeto")
print(type(node))
print(node.get_nodes_ips())