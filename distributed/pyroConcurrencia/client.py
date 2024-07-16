# client1.py

import Pyro5.api

# Conectar con el servidor Pyro5
uri = "PYRO:obj_abc123@localhost:50001"
proxy = Pyro5.Proxy(uri)

# Realizar un aumento
resultado = proxy.aumentar_variable()
print(f"Resultado del aumento en Cliente 1: {resultado}")
