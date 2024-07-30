
#/delete_file

import requests
import pickle

url='http://172.30.0.3:8000/delete_file'

name='hola.txt'

data=pickle.dumps(name)

files={'file':data}

   
# Envía una solicitud POST con el archivo adjunto
response = requests.post(url, files=files)

# Imprime la respuesta del servidor
print(response.status_code)
print(response.text)
