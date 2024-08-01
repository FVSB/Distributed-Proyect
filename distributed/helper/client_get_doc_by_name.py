import requests
import os
from helper.utils import *


def download_file(url, file):
    chunk_size = 1024  # Tamaño de cada paquete
    start_part = 1
    folder = "/app/logs"
    save_path = os.path.join(folder, file)
    file_exists = os.path.exists(save_path)

    if file_exists:
        # Abre el archivo en modo binario para leer su contenido
        with open(file, "rb") as f:
            # Lee el archivo en bloques de 1024 bytes
            block_size = 1024
            blocks_count = 0
            while True:
                # Lee el siguiente bloque del archivo
                block = f.read(block_size)

                # Si el bloque leído es vacío, hemos llegado al final del archivo
                if not block:
                    break

                # Incrementa el contador de bloques
                blocks_count += 1

                # Imprime el número total de bloques de 1024 bytes
            print(blocks_count)
            start_part = blocks_count + 1

    try:
        print(f"La start_part es {start_part}")
        # Definir los parámetros de la solicitud
        params = {"start": start_part, "name": file}

        # Realizar la solicitud GET con los parámetros
        response = requests.get(url, params=params, stream=True)
        response.raise_for_status()

        with open(save_path, "ab" if file_exists else "wb") as f:
            print("aca")
            for chunk in response.iter_content(chunk_size=None):
                print("acaaa")
                if chunk:
                    # f.write(chunk)
                    paquete = Paquete.deserialize(chunk)
                    print("ff")
                    a: str = pickle.loads(paquete.bytes_datos)
                    print(a)
                    f.write(a.encode())
        print(f"File downloaded successfully and saved to {file}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    file_url = "http://172.30.0.5:8000/get_document_by_name"  # URL del servidor Flask
    file = "hola.txt"  # Nombre del archivo de salida

    download_file(file_url, file)
