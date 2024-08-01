import requests
import pickle

url='http://172.18.0.2:8000/upload'



document='3er archivo'
name='holakfkfkfmvkfemrk.txt'

data=pickle.dumps((name,document))

files={'file':data}

   
# Envía una solicitud POST con el archivo adjunto
response = requests.post(url, files=files)

# Imprime la respuesta del servidor
print(response.status_code)
print(response.text)



# Hacer retrival

url='http://172.31.0.8:8000/'