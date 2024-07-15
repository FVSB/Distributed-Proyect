import Pyro5.api
import subprocess
@Pyro5.api.expose
class Receiver:
    def __init__(self):
        self.received_strings = []

    def receive_string(self, string):
        self.received_strings.append(string)
        print(f"String recibido: {string}")
        return "OK"

subprocess.Popen(["python","-m","Pyro5.nameserver",'--host','0.0.0.0'])
print('Creado server ')
daemon = Pyro5.server.Daemon(host="0.0.0.0")
ns = Pyro5.api.locate_ns()
uri = daemon.register(Receiver)
ns.register("example.receiver", uri)

# Obtener todos los nombres registrados
nombres_registrados = ns.list()
print(nombres_registrados)
print("Receiver listo.")
daemon.requestLoop()
