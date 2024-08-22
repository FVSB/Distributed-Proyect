import requests
import pickle
ip='172.18.0.5'
url=f'http://{ip}:8000/upload'



document='Primer archivo'
name='tyrion.txt'

data=pickle.dumps((name,document))

files={'file':data}

   
# Envía una solicitud POST con el archivo adjunto
response = requests.post(url, files=files,timeout=200)

# Imprime la respuesta del servidor
print(response.status_code)
print(response.text)



# Hacer retrival

url='http://172.18.0.7 :8000/'