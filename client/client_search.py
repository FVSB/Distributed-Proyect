import requests
import pickle
from logguer import log_message
ip='172.18.0.5'
url_upload=f'http://{ip}:8000/upload'

url_update=f'http://{ip}:8000/update'

url_query=f'http://{ip}:8000/query'


name="novero primer archivo"
max_results=10
min_score=0
posibles_extensions=["txt"]
params={"query":name,"max_results":max_results,"min_score":min_score,"extensions":posibles_extensions}

try:
    # Realizar la solicitud GET con los parámetros
    response = requests.get(url_query, params=params, stream=True)
    response.raise_for_status()


    # Leer la respuesta como JSON
    data = response.json()
    log_message(data)

    # O leer la respuesta como texto
    text = response.text
    #log_message(text)
    log_message(len(data['results']))
except Exception as e:
    log_message(f'Error')