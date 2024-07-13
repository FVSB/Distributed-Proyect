import logging
import json
import time
import os
from datetime import datetime

# Crea la carpeta 'logs' si no existe
logs_dir = "logs"
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

# Configura el manejador de archivos
file_handler = logging.FileHandler(os.path.join(logs_dir, "my_logs.txt"))
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - %(lineno)d - %(message)s')
file_handler.setFormatter(formatter)

# Configura el manejador de consola
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Configura el logger raíz
logging.basicConfig(handlers=[file_handler, console_handler], level=logging.DEBUG)

# Define un diccionario para almacenar los logs en JSON
logs_json = {}

# Función para serializar los logs en JSON
def serialize_logs(logs_json, filename="logs.json"):
    full_path = os.path.join(logs_dir, filename)
    with open(full_path, 'w') as f:
        json.dump(logs_json, f, indent=4)

# Crea una función para registrar mensajes con información adicional
def log_message(message, level="INFO", extra_data={}):
    """
    Registra un mensaje de log con información adicional.
    """
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "level": level,
        "message": message,
        "extra_data": extra_data
    }
    logs_json[time.time()] = log_entry
    logging.log(logging.getLevelName(level), message, extra=extra_data)


if __name__=='main':
## Ejemplo de uso
#log_message("Este es un mensaje de información.")
#log_message("Este es un mensaje de advertencia.", level="WARNING")
#log_message("Este es un mensaje de error.", level="ERROR", extra_data={"error_code": 123, "details": "Detalles del error"})

    # Serializa los logs en JSON cada cierto tiempo
    while True:
        serialize_logs(logs_json)
        time.sleep(1)  # Serializa cada 60 segundos (1 minuto)