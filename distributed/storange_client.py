import requests

# URL de tu servidor Flask
url = 'http://localhost:5000/subir_archivo'

# Ruta al archivo PDF que quieres enviar
ruta_archivo = 'ruta/a/tu/archivo.pdf'

# Abre el archivo en modo binario
with open(ruta_archivo, 'rb') as archivo:
    # Crea un diccionario con los archivos a enviar
    archivos = {'archivo': ('nombre_archivo.pdf', archivo, 'application/pdf')}
    
    # Realiza la petición POST
    respuesta = requests.post(url, files=archivos)

# Imprime la respuesta del servidor
print(respuesta.text)

# Verifica el código de estado de la respuesta
if respuesta.status_code == 200:
    print("El archivo se subió exitosamente.")
else:
    print(f"Hubo un error al subir el archivo. Código de estado: {respuesta.status_code}")