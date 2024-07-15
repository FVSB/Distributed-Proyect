from chord import *
def main():
    ip = socket.gethostbyname(socket.gethostname())
    objeto = ChordNode(ip,m=3)
    #daemon = Pyro5.server.Daemon()
    #uri = daemon.register(objeto)
    #
    #ns = Pyro5.api.locate_ns(host='127.0.0.1')
    #ns.register(objeto.url, uri)
    #
    #print(f"Objeto1 en Server1 URI: {uri}")
    #print("Servidor Pyro5 en Server1 corriendo...")
    #daemon.requestLoop()

if __name__ == "__main__":
    main()
