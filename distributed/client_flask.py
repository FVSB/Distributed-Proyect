import requests

# URL de la aplicación Flask
url = 'http://172.17.0.5:5000/'

# Hacer una solicitud GET a la aplicación Flask
response = requests.get(url)
# Imprimir la respuesta
print(response.text)
print(response.json())
print(response.status_code)
print(response)