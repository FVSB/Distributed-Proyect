import Pyro5.api
import Pyro5.nameserver

@Pyro5.api.expose
class MiObjetoRemoto:
    def decir_hola(self, nombre):
        return f"Hola, {nombre}!"

# Inicia el servidor de nombres de Pyro5
ns = Pyro5.nameserver.start_ns()

# Crear un demonio Pyro
demonio = Pyro5.server.Daemon()

# Registrar el objeto remoto con el nombre "mi_objeto"
uri = demonio.register(MiObjetoRemoto, "mi_objeto")

# Registrar el objeto en el servidor de nombres
ns.register("mi_objeto", uri)

# Imprimir la URI para que el cliente pueda conectarse
print("El URI del objeto remoto es:", uri)

# Iniciar el demonio
print("Esperando conexiones...")
demonio.requestLoop()
