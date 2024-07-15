import Pyro5.api

class Sender:
    def send_string(self, string, receiver_uri):
        with Pyro5.api.Proxy(receiver_uri) as receiver:
            confirmation = receiver.receive_string(string)
            print(f"Confirmación del receptor: {confirmation}")

def main():
    #ns = Pyro5.api.locate_ns(host="localhost", port=9090)
    #print("Nameserver located successfully.")
    # Ubicar el servidor de nombres PyroNS
                with Pyro5.api.locate_ns() as ns:
                    # Obtener todos los nombres registrados
                    nombres_registrados = ns.list()
                    print(nombres_registrados)
    #receiver_uri = "PYRO:example.receiver@receptor:9090"  # Cambiar a 9091
    #ns.lookup("example.receivere")
    #sender = Sender()
    #sender.send_string("Hola, receptor!", receiver_uri)

if __name__ == "__main__":
    main()
