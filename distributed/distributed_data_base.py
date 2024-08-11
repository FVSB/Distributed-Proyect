from chord.sync_storange_node import *


class DistributedDataBase(SyncStoreNode):
    #################################
    #                               #
    #       Lider Region            #
    #                               #
    #################################


    def _is_all_db_stable(self)->bool:
        """
        Este es un metodo para el lider
        False si no soy el lider o si yo o mi prede no tiene la data actualizada
        True si yo y mi predecesor tienen la data sincronizada

        Returns:
            bytes: _description_
        """
        if not self.i_am_leader:
            return False
        return self.is_sync_data and self.pred.is_data_sync()
    
    
    """
    Esta clase tiene la logica de una base de datos distribuida

    """
    
    def handle_request(self, data, option: int, a) -> bytes:#Override
        
        if option==IS_DATA_SYNC:# Manda a comprobar que la data está sincronizada
            return obj_to_bytes(self._is_sub_ring_db_is_stable())
        
        if option==IS_DB_STABLE:# Es pq soy el lider => respondo si yo and mi pred tienen la data estable
            return obj_to_bytes(self._is_all_db_stable())
        
        return super().handle_request(data, option, a)
    
    
    def data_to_print(self):#Override
        super().data_to_print()
        log_message(f'Es estable la DB: {self.is_db_stable()}',func=self.data_to_print)

    def __init__(self, ip: str, port: int = 8001, flask_port: int = 8000, m: int = 160):
        super().__init__(ip, port, flask_port, m)
      
        

  
            
    def _is_sub_ring_db_is_stable(self)->bool:
        """
        Chequea si yo y mis predecesores tienen la data sincronizada
        True: Si yo y mi predecesore tienen la data sincronizada (Si soy el lider digo True o False respecto a mi propio dato)
        False: Si yo o mi predecesor no tiene sincronizada la data
    
        """
        if self.i_am_leader:# Si soy el lider solo depende de si mi data está actualizada
            return self.is_sync_data
        
        return self.is_sync_data and self.pred.is_data_sync()
    
    
    def is_db_stable(self)->bool:
        """
        Metodo para saber si la base de datos es estable
        True  si la base de datos es consistente Se pueden realizar CUD
        False si la base de datos no es consistente esperar para hacer CUD

        Returns:
            bool: _description_
        """
        try:
            return self.is_sync_data and self.leader.is_data_sync()
        except Exception as e:
            log_message(f'Hubo un error tratando de saber si la db es estable Error:{e} \n {traceback.format_exc()}',func=self.is_db_stable)
            return False
            
    
   
    
    
        
        
            

if __name__ == "__main__":
    log_message("Hello from Sync Storage node")
    ip = socket.gethostbyname(socket.gethostname())
    node = DistributedDataBase(ip, m=3)
    node.start_node()  # Iniciar el pipeline

    while True:
        pass
