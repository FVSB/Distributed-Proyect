from chord import *

class Lider(ChordNode):
    def __init__(self, ip: str, port: int = 8001, m: int = 160):
        super().__init__(ip, port, m)
        self.I_am_leader=False
        self.In_election=False
        
    
    
        