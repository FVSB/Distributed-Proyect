import requests
import pickle

url='http://172.31.0.8:8000/upload'



document='393993939'
name='hola.txt'

data=pickle.dumps((name,document))

files={'file':data}

   
# Envía una solicitud POST con el archivo adjunto
response = requests.post(url, files=files)

# Imprime la respuesta del servidor
print(response.status_code)
print(response.text)



# Hacer retrival

url='http://172.31.0.8:8000/'