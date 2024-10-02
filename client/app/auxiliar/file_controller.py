import requests
import pickle
from logguer import log_message
import streamlit as st
from client_ import *
from logguer import log_message
import logging
import json
import time
import os
from datetime import datetime
import inspect
import traceback

# Simulación de documentos con score
log_message(f"Iniciado")

# Función para realizar la búsqueda
def realizar_busqueda(query, extensiones):
    documentos = make_query(query=query, posibles_extensions=extensiones)
    return documentos
  
def subir_o_actualizar_archivo(subir:bool):
    name="Subir" if subir else "Actualizar"
    st.title(f"📤 {name} un Archivo") 

    # Especificar el nombre del archivo a buscar
    nombre_archivo = st.text_input("Introduce el nombre del archivo que deseas buscar en 'app/database':")
    
    # Ruta de la carpeta donde se buscará el archivo
    ruta_archivo = os.path.join("app", "database", nombre_archivo)

    # Botón para buscar y subir el archivo
    if st.button(f"Buscar y {name} Archivo"):
        if os.path.isfile(ruta_archivo):
            # Si el archivo existe, permitir subirlo
            with open(ruta_archivo, "rb") as file:
                st.download_button(label="Descargar Archivo", data=file, file_name=nombre_archivo)
                st.success(f"El archivo '{nombre_archivo}' ha sido encontrado y está listo para descargar.")
                contenido_archivo = file.read()  # Leer el contenido del archivo como str
                contenido_str = contenido_archivo.decode("utf-8") 
                st.text_area("Contenido del Archivo:", contenido_str, height=300)  # Mostrar contenido en un área de texto
                response=""
                if subir:
                    response=insert_document(title=nombre_archivo,text=contenido_str)
                else:
                    response=update_document(title=nombre_archivo,text=contenido_str)
                st.success(f"El archivo '{nombre_archivo} {response}' .")
        else:
            st.error(f"El archivo '{nombre_archivo}' no existe en la carpeta 'app/database'.")
    
    # Opción para escribir un nuevo archivo
    st.write("O también puedes escribir el contenido de un nuevo archivo:")
    contenido_nuevo = st.text_area("Escribe el contenido del nuevo archivo:")
    
    # Botón para subir el nuevo archivo
    if st.button(f"{name} Nuevo Archivo"):
        #if contenido_nuevo:
        #    # Guardar el nuevo archivo en la carpeta especificada
        #    with open(ruta_archivo, "w") as new_file:
        #        new_file.write(contenido_nuevo)
        #    st.success(f"El nuevo archivo '{nombre_archivo}' ha sido creado y subido a 'app/database'.")
        response=""
        if contenido_nuevo:
            if subir:
                    response=insert_document(title=nombre_archivo,text=contenido_nuevo)
            else:
                    response=update_document(title=nombre_archivo,text=contenido_nuevo) 
           
            st.success(f"El nuevo archivo '{nombre_archivo}' {response}'.")
        else:
            st.warning("Por favor, escribe algo en el contenido del nuevo archivo.")
    
# Configuración de la página
st.set_page_config(page_title="Motor de Búsqueda", page_icon="🔍")

# Menú de selección de páginas
pagina = st.sidebar.selectbox("Selecciona una página:", ["Inicio", "Motor de Búsqueda", "Solicitar Archivo","Subir un archivo","Actualizar un archivo","Eliminar un archivo"])

# Página de Inicio
if pagina == "Inicio":
    st.title("Bienvenido al Motor de Búsqueda")
    st.write("Esta aplicación te permite buscar documentos y solicitar archivos.")
    st.write("Utiliza el menú en la barra lateral para navegar a las diferentes secciones.")

# Página de Motor de Búsqueda
elif pagina == "Motor de Búsqueda":
    st.title("🔍 Motor de Búsqueda Personalizado")

    # Entrada de la consulta
    query = st.text_input("Introduce tu consulta:")

    # Entrada de extensiones
    extensiones = st.text_input("Introduce extensiones posibles separadas por comas:")
    extensiones = extensiones.replace(',', ' ').split()
    extensiones = [ext for ext in extensiones if ext]

    # Botón para realizar la búsqueda
    if st.button("Realizar Búsqueda"):
        if query:
            resultados = realizar_busqueda(query, extensiones)
            
            if resultados and len(resultados) > 0:
                for doc in resultados:
                    title = doc['title']
                    snippet = doc["snipet"]
                    id_ = doc["id"]
                    score = doc["score"]
                
                    with st.expander(f"{title} (Score: {score})"):
                        st.write(snippet)
                        
                        # Crear una clave única para cada botón
                        button_key = f"button_{id_}"
                        if st.button(f"Mostrar contenido completo de {title}", key=button_key):
                            st.session_state[button_key] = True
                    
                        # Mostrar el contenido completo si se presionó el botón
                        if st.session_state.get(button_key):
                            st.write(f"Buscando el documento completo de {title}")
                            text = download_file(title, True)
                            st.write(text)
            else:
                st.info("No se encontraron documentos que coincidan con la consulta.")
        else:
            st.warning("Por favor, introduce una consulta.")

# Página para solicitar archivos
elif pagina == "Solicitar Archivo":
    st.title("📁 Solicitar Archivo")

    # Entrada del nombre del archivo
    file_name = st.text_input("Introduce el nombre del archivo:")

    # Botón para enviar la solicitud
    if st.button("Enviar Solicitud"):
        if file_name:
            # Lógica para enviar la solicitud
            st.success(f"Solicitud enviada para el archivo '{file_name}'.")
            text=download_file(file_name,False)
            st.write(f"El texto es \n {text}")
        else:
            st.warning("Por favor, completa todos los campos.")
# Página para subir un archivo
elif pagina == "Subir un archivo":
   subir_o_actualizar_archivo(True)
elif pagina== "Actualizar un archivo":
    subir_o_actualizar_archivo(False)
elif  pagina== "Eliminar un archivo":
    st.title("🗑️ Eliminar un Archivo")

    # Entrada del nombre del archivo
    nombre_archivo = st.text_input("Introduce el nombre del archivo que deseas eliminar en 'app/database':")
    
    response=delete_document(nombre_archivo)
    
    st.write(f"Respuesta \n {response}")

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

logs_dir = "app/logs"
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)


# Configura el manejador de archivos
log_file_name = f"my_logs_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt"


log_file_path = os.path.join(logs_dir, log_file_name)
file_handler = logging.FileHandler(log_file_path)
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(filename)s - %(lineno)d - %(custom_funcName)s - %(message)s"
)
file_handler.setFormatter(formatter)

# Configura el manejador de consola
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Configura el logger raíz
logging.basicConfig(handlers=[file_handler, console_handler], level=logging.DEBUG)

# Define un diccionario para almacenar los logs en JSON
logs_json = {}


# Función para serializar los logs en JSON
def serialize_logs(logs_json, filename="logs_container.json"):
    full_path = os.path.join(logs_dir, filename)
    with open(full_path, "w") as f:
        json.dump(logs_json, f, indent=4)


# Crea una función para registrar mensajes con información adicional
def log_message(message, level="INFO", extra_data={}, func=None):
    """
    Registra un mensaje de log con información adicional.
    Si se proporciona 'func', se usa como nombre de la función.
    Si no, se detecta automáticamente.
    """
    # Obtiene información sobre el llamador
    caller_frame = inspect.currentframe().f_back
    caller_line = caller_frame.f_lineno

    if func is None:
        caller_method = caller_frame.f_code.co_name
    else:
        caller_method = func.__name__ if callable(func) else str(func)

    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "level": level,
        "message": f"{message} Error: {traceback.format_exc()}",
        "extra_data": extra_data,
        "method": caller_method,
        "line": caller_line,
    }
    logs_json[time.time()] = log_entry

    # Añade el nombre del método y el número de línea al diccionario extra_data
    extra_data["custom_funcName"] = caller_method
    extra_data["line"] = caller_line

    logger = logging.getLogger(__name__)
    logger.log(logging.getLevelName(level), message, extra=extra_data)
    #serialize_logs(logs_json)

if __name__ == "__main__":
    # Ejemplo de uso
    log_message("Este es un mensaje de información desde el nivel principal.")

    # Serializa los logs en JSON cada cierto tiempo
    while True:
        serialize_logs(logs_json)
        time.sleep(30)  # Serializa cada 30 segundos
