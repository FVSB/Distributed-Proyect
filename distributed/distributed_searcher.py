from distributed_data_base import *

class DistributedSearcher(DistributedDataBase):
    
    def __init__(self, ip: str, port: int = 8001, flask_port: int = 8000, m: int = 160):
        super().__init__(ip, port, flask_port, m)
        
    def upload_file(self):
        


if __name__ == "__main__":
    log_message("Hello from Distribued Seacher node")
    ip = socket.gethostbyname(socket.gethostname())
    node = DistributedSearcher(ip, m=3)
    node.start_node()  # Iniciar el pipeline

    while True:
        pass