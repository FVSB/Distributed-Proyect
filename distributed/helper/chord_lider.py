from chord import *

class Lider(ChordNode):
    
    
    def _check_make_election(self,time_=4):
        while True:
            time.sleep(time_)
            try:
                log_message(f'Comprobando si debo hacer una Eleccion',func=self._check_make_election)
                if not self.Is_Search_Succ: continue # Si no estoy buscando succesor continuo
                self.Is_Search_Succ=False # Se supone que entonces ahora debo hacer elecciones
                self._send_broadcast(ELECTION,self.ref) # Enviar a todos que yo Convoco Elecciones
                
            except Exception as e:
                log_message(f'Error tratando de hacer Elecciones: {e} \n {traceback.format_exc()}',func=self._check_make_election)
                
                    
                        
    def __init__(self, ip: str, port: int = 8001, m: int = 160):
        super().__init__(ip, port, m)
        self.I_am_leader=False
        self.In_election=False
        
        
    
    
    
        