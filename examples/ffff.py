import zmq
import time
import threading
import zmq
print(zmq.pyzmq_version())
def publisher():
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind("tcp://*:5555")

    while True:
        message = "Nodo descubierto"
        socket.send_string(message)
        print(f"Mensaje enviado: {message}")
        time.sleep(1)

def subscriber():
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://localhost:5555")
    socket.setsockopt_string(zmq.SUBSCRIBE, "")

    while True:
        message = socket.recv_string()
        print(f"Mensaje recibido: {message}")

if __name__ == "__main__":
    pub_thread = threading.Thread(target=publisher)
    sub_thread = threading.Thread(target=subscriber)
    
    pub_thread.start()
    sub_thread.start()
    
    pub_thread.join()
    sub_thread.join()

