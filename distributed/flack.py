from flask import Flask
import socket
app = Flask(__name__)

@app.route('/')
def index():
    return 'Hola, este es mi servidor Flask!'

if __name__ == '__main__':
    ip = socket.gethostbyname(socket.gethostname())
    app.run(host=ip, port=5000)