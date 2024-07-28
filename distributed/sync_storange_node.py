from storange_node import *

class SyncStoreNode(StoreNode):
    def __init__(self, ip: str, port: int = 8001, flask_port: int = 8000, m: int = 160):
        super().__init__(ip, port, flask_port, m)
        
        
        
        
        

if __name__ == "__main__":
    log_message("Hello from Sync Storage node")
    # time.sleep(10)
    ip = socket.gethostbyname(socket.gethostname())
    node = SyncStoreNode(ip, m=3)

    node.start_threads()  # Iniciar los nodos
    while True:
        pass
