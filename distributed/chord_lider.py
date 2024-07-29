from chord import *


class Leader(ChordNode):
    
    def make_election(self):
        """Convoco hacer elecciones
        """
        log_message(f'Mandando hacer eleccion',func=self.make_election)
        self.in_election_=True
        self._send_broadcast(ELECTION,self.ref) # Enviar a todos que yo Convoco Elecciones
    
    def check_i_am_alone(self,time_=10):
        while True:
            time.sleep(time_)
            try:
                result:bool=(self.pred is None and self.succ.id==self.id)
                self.i_am_alone_=result
                if result:
                    with self.in_election_lock:
                        self.in_election_=False
            except Exception as e:
                log_message(f'Error chequeando que estoy solo',func=self.check_i_am_alone)
    
    def _check_make_election(self,time_=4):
        """Chequea que el nodo tenga que estar en eleccion si es necesario

        Args:
            time_ (int, optional): _description_. Defaults to 4.
        """
        while True:
            time.sleep(time_)
            try:
                log_message(f'Comprobando si debo hacer una Eleccion',func=self._check_make_election)
                if  self.pred or self.i_am_alone: 
                    
                    log_message(f'Tengo predecesor {self.pred} o estoy solo {self.i_am_alone} no debo hacer broadcast',func=self._check_make_election)
                    continue # Si Tengo buscando predecesor continuo
                self.leader=self.ref # Mi lider ahora soy yo
                log_message(f'Ahora yo soy mi lider',func=self._check_make_election)
                #self.Is_Search_Succ=False # Se supone que entonces ahora debo hacer elecciones
                log_message(f'No tengo sucessor debo hacer broadcast de Election',func=self._check_make_election)
                self.make_election()# Mandar hacer eleccion
                
            except Exception as e:
                log_message(f'Error tratando de hacer Elecciones: {e} \n {traceback.format_exc()}',func=self._check_make_election)
        
   
    def broadcast_handle(self, op: int, message: tuple[int, ChordNodeReference], address: str):
        super().broadcast_handle(op, message, address)
        _,node=message
        if op==ELECTION:# Es que estan en eleccion
            log_message(f'El nodo {node.id} está haciendo eleccion',func=self.broadcast_handle)
            self.Election_handler(node)
        elif op==ELECTION_WINNER:# Es que alguien gano las elecciones
            log_message(f'El ganador de las elecciones es {node.id} y el que yo creia como lider es {self.leader.id}',func=self.broadcast_handle)
            #self.Election_handler(node)       Esto funcionaba antes
            self.winner_handle(node)
                       
    def data_to_print(self):
        """Para poder añadir cosas a pintar"""
        log_message(f'Mi predecesor es {self.pred.id if self.pred else None} con ip {self.pred.ip if self.pred else None} ',level='INFO')
        log_message(f'Yo soy id:{self.id},con ip:{self.ip} ',level='INFO')
        if self.succ.id == self.id:
            if self.succ.ip !=self.ip:
                log_message('El sucesor tiene igual ID pero no tiene igual ip',level='INFO')
            log_message(f'Todavia no tengo sucesor',level='INFO'),
        else:
            log_message(f'Mi sucesor es {self.succ.id if self.succ else None} con ip {self.succ.ip  if self.succ else None}',level='INFO')
        
        log_message(f'Estoy en eleccion {self.in_election_}',func=self.show)
        
        log_message(f'Soy Estable {self.is_stable} ',func=self.show)
        
        log_message(f'El lider es {self.leader.id}',func=self.show)
        
        log_message(f'La lista de sucesores es {self.succ_list}',func=self.show)
        
        log_message(f'Se puede confiar en la lista de sucesores {self.succ_list_ok}',func=self.show)
    
    def show(self,time_:int=3, print_final:bool=True):
        """
        Show my ip and id and mi predecessor and succesors ips and ids
        """
        print(f'ENtro en print')
        """Printea quien soy yo"""
        while True:
            log_message('-'*20,level='INFO')
            
            self.data_to_print()# Printear los logs
            if print_final:log_message('*'*20,level='INFO') # Printea el final en este hilo
            
            
            time.sleep(time_) # Se presenta cada 10 segundos
    
    def check_i_am_stable(self,time_=2):
        """
        Chequea contantemente si estoy estable o no 

        Args:
            time_ (int, optional): _description_. Defaults to 2.
        """
        while True:
            time.sleep(0.1)
            try:
                in_election=True
                with self.in_election_lock:
                    in_election=self.in_election_
                        
                if in_election:
                    self.is_stable=False
                else:
                    time.sleep(time_*2)
                    with self.in_election_lock:
                        if not self.in_election_:
                            self.is_stable=True


            except Exception as e:
                log_message(f'Error en chequear si soy un nodo estable',func=self.is_stable)
                
    def start_threads(self):
        super().start_threads()
        threading.Thread(target=self._check_make_election,daemon=True).start()# Comprobar si debo hacer eleccion o no
        threading.Thread(target=self.check_election_valid,daemon=True).start()# Comprobar que estoy en eleccion
        threading.Thread(target=self.check_i_am_alone,daemon=True).start()#Chequea si estoy solo
        #threading.Thread(target=self.show,daemon=True).start()
        threading.Thread(target=self.check_i_am_stable,daemon=True).start()# Chequeo constantemente si soy un nodo estable
        threading.Thread(target=self.check_succ_list,daemon=True).start() # Chequeo de que la lista de sucesores este actualizada
        
    
            
    
    def __init__(self, ip: str, port: int = 8001, m: int = 160,succ_lis_count:int=2):
        super().__init__(ip, port, m)
        self.leader_:ChordNodeReference=self.ref
        self.leader_lock:threading.RLock=threading.RLock()#Lock para conocer quien es el lider
        self.i_am_leader_:bool=True
        self.i_am_leader_lock:threading.RLock=threading.RLock()#Lock para conocer si soy el lider
        self.in_election_:bool=True
        """
        True si estoy en eleccion False si no lo estoy
        """
        self.in_election_lock:threading.RLock=threading.RLock()
        self.i_am_alone_:bool=False # EMpiezo pensando que no para hacer descubrimiento
        self.i_am_alone_lock:threading.RLock=threading.RLock()
        self.is_stable_:bool=False
        """Dicta si estoy estable la red o no"""
        
        self.is_stable_lock:threading.RLock=threading.RLock()
        
        self.succ_list_count_:int=succ_lis_count
        """
        Dice cuantos es la lista de sucesores
        """
        self.succ_list_:list[ChordNodeReference]=[self.ref]*self.succ_list_count_
        """
        Dice mi lista de sucesores para la posterior replicación
        """
        self.succ_list_lock:threading.RLock=threading.RLock()
        """
        Lock para la lista de sucesores
        """
        self.succ_list_ok_:bool=False 
        """
        Dice si está o no actualizado la lista de sucesores
        """
        self.succ_list_ok_lock:threading.RLock=threading.RLock()        
        

        
        #Threads
    
        #self.start_threads()
        
    @property 
    def succ_list_ok(self)->bool:
        """
        Retonar si está actualizado o no la lista de sucesores

        Returns:
            bool: _description_
        """
        with self.succ_list_ok_lock:
            return self.succ_list_ok_
        
    @succ_list_ok.setter
    def succ_list_ok(self,value:bool):
        if not isinstance(value,bool):
            raise Exception(f'Value debe de ser de tipo bool no de tipo: {type(value)} value: {value}' )
        with self.succ_list_ok_lock:
            self.succ_list_ok_=value
        
    @property 
    def in_election(self)->bool:
        """Retorna con seguridad entre hilos si se está o no en eleccion

        Raises:
         
        Returns:
            bool: _description_
        """
        with self.in_election_lock:
            return self.in_election_
        
    @property 
    def succ_list(self)->list[ChordNodeReference]:
        """
        Da la lista de sucesores en vista a la replicación

        Returns:
            list[ChordNodeReference]: _description_
        """
        with self.succ_list_lock:
            return self.succ_list_ 
        
    @succ_list.setter
    def succ_list(self,value:list[ChordNodeReference]):
        if not isinstance(value,list) or not all(isinstance(item, ChordNodeReference) for item in value):
            log_message(f'Value no es una lista de ChordNodeReference para la lista de succesores',func=self.succ_list)
            raise Exception(f'Value no es una lista de ChordNodeReference para la lista de succesores')
        with self.succ_list_lock:
            self.succ_list_=value
            
        
    @property
    def is_stable(self)->bool:
        """
        Dice si estoy estable o no => que me aseguré que ningun nodo esta en elecciones
        Raises:
            
        Returns:
            _type_: _description_
        """
        with self.is_stable_lock:
            return self.is_stable_
    
    @is_stable.setter
    def is_stable(self,value:bool):
        with self.is_stable_lock:
            self.is_stable_=value
        
   
    @property 
    def i_am_alone(self)->bool:
        """
        Dice True si estoy Solo False si no 
        Es seguro ante hilos

        Returns:
            bool: _description_
        """
        with self.i_am_alone_lock:
            return self.i_am_alone_
    @i_am_alone.setter
    def i_am_alone(self,value):
        if not isinstance(value,bool):
            raise Exception(f'Value es de tipo {type(value)} no bool {value} ')
        with self.i_am_alone_lock:
            self.i_am_alone_=value 
    @property
    def leader(self):
        with self.leader_lock:
            return self.leader_
        
    @leader.setter
    def leader(self,value):
        if not isinstance(value,ChordNodeReference):
            raise Exception(f'value:{value} no es de tipo ChordNodeReference es de tipo:{type(value)}') 
        with self.leader_lock:
            self.leader_=value  
    
    @property
    def i_am_leader(self):
        with self.i_am_leader_lock:
            return self.i_am_leader_
    
    @i_am_leader.setter
    def i_am_leader(self,value):
        if not isinstance(value,bool):
            raise Exception(f'Value es de tipo {type(value)} no de tipo bool value:{value}')
        self.i_am_leader_=value
        
    
    def check_election_valid(self,time_=5,wait_election_time:int=10):
        while True: # Compruebo si estoy en eleccion
            time.sleep(time_)
            try:
                log_message(f'Chequeando si estoy en eleccion',func=self.check_election_valid)                   
                with self.in_election_lock:# Desbloqueando 
                    log_message(f'Comprobando si estoy en eleccion {self.in_election_}')
                    if  not self.in_election_:
                        log_message(f'No estoy en eleccion',func=self.check_election_valid)
                        continue
                    # Estoy en eleccion
                    time.sleep(wait_election_time)
                    self.in_election_=False 
                    log_message(f'Terminada la elección mi lider es {self.leader}',func=self.check_election_valid) 
                    if self.leader.id == self.id  :  # Es que soy el lider, mandar confirmación
                        log_message(f'Yo soy el lider por tanto mando a confirmar',func=self.check_election_valid)
                        self._send_broadcast(op=ELECTION_WINNER,data=self.leader) #Soy el ganador 
                        log_message(f'Enviado el mensaje de soy el lider',func=self.check_election_valid)
                        #Esperar y pedirle a mi predecesor que me rellene una lista con los nodos que tiene 
                                      
            except Exception as e:
                log_message(f'Error chequeando si hay eleccion {e} \n {traceback.format_exc()}',func=self.check_election_valid)
                    
    def check_succ_list(self,time_:int=1):
        check=True # Dice si hay que chequear o no la lista de sucesores
        while True:
            time.sleep(time_)
            try:

                if self.in_election:
                    check=True # Si estoy en eleccion cuando deje de estarlo tengo que chequear
                    self.succ_list_ok=False # No se debe mirar en la lista de sucesores
                
                if  self.in_election or not check:# Si está en elecciones o no hay que chequear mejor que vuelva a preguntar
                    continue 
                
                succ:ChordNodeReference=self.succ # Pongo primero a mi sucesor
                succ_list:list[ChordNodeReference]=self.succ_list # Capto la anterior primero
                for i in range(self.succ_list_count_): # Iterar por la cant de nodos que tenemos 
                    succ_list[i]=succ
                    succ=succ.succ # Ahora el sucesor es el sucesor de mi sucesor

                if not self.in_election: #
                    self.succ_list=succ_list
                    self.succ_list_ok=True # Se puede volver a confiar en la lista de sucesores

            except Exception as e:
                log_message(f'Ocurrio un error actualizando la lista de sucesores {e} \n {traceback.format_exc()}',func=self.check_succ_list)  

    
    def winner_handle(self,winner_node:ChordNodeReference):
        """Comprueba el ganador de lider 
            Si el lider ganador es menor que el que tenia propuesto lo acepto
            Si es mayor que el propuesto mando broadcast de proponerme de lider

        Args:
            winner_node (ChordNodeReference): _description_
        """
        log_message(f'EL ganador de las elecciones fue {winner_node.id}',func=self.winner_handle)
        if self.id<winner_node.id:
            log_message(f'El id del ganador de las elecciones es mayor que el mio, id_ganador{winner_node.id} mi id {self.id}',func=self.winner_handle)
            self.make_election()
            return
        if self.leader.id<winner_node.id:
            log_message(f'El id del nodo ganador es{winner_node.id} y mi lider propuesto es {self.leader.id}',func=self.winner_handle)
            self.make_election()
            return 
        self.leader=winner_node
        with self.in_election_lock:
            self.in_election_=False # Ya no estoy en elecciones
        
    def Election_handler(self,node_propose:ChordNodeReference):
        """
        Maneja las propuestas de ser el lider 

        Args:
            node_propose (ChordNodeReference): _description_
        """
        
        with self.in_election_lock:
            if not self.in_election_:# Si no estaba en eleccion ahora estoy en eleccion
                self.in_election_=True
                if self.pred and self.pred.id<self.id: # Si tengo predecesor momentaneamente este es mi lider
                    self.leader=self.pred
                else: # Si no tengo predecesor voy a proponerme de lider
                    self.leader=self.ref
            
        if node_propose.id<self.leader.id:# Si el nodo que se propuso es menor que el nodo que tengo como lider
            self.leader=node_propose # Actualizo mi lider
            log_message(f'Mi nuevo lider es {self.leader.id}',func=self.Election_handler)
    
    

if __name__ == "__main__":
    print("Hello from Lider node")
    ip = socket.gethostbyname(socket.gethostname())
    node = Leader(ip,m=3,succ_lis_count=2)
    #node.start_threads()#Iniciar los nodos
    node.start_node() # Iniciar el pipeline
    while True:
        pass


        
        
    
    
    
        