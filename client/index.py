import streamlit as st
from client import *
from logguer import log_message
# Simulación de documentos con score
log_message(f"Iniciado")

def obtener_documentos():
    return [
        {
            "id": 1,
            "titulo": "Documento 1",
            "snippet": "Este es un ejemplo del primer documento.",
            "score": 0.9,
            "contenido": "Contenido completo del Documento 1."
        },
        {
            "id": 2,
            "titulo": "Documento 2",
            "snippet": "Este es un ejemplo del segundo documento.",
            "score": 0.85,
            "contenido": "Contenido completo del Documento 2."
        },
        {
            "id": 3,
            "titulo": "Documento 3",
            "snippet": "Este es un ejemplo del tercer documento.",
            "score": 0.8,
            "contenido": "Contenido completo del Documento 3."
        },
    ]

# Función para realizar la búsqueda
def realizar_busqueda(query, extensiones):
    documentos = make_query(query=query,posibles_extensions=extensiones)
    return documentos
    # Filtrar por query (simulado)
    documentos_filtrados = [doc for doc in documentos if query.lower() in doc['titulo'].lower()]
    
    # Ordenar por score
    documentos_ordenados = sorted(documentos_filtrados, key=lambda x: x['score'], reverse=True)
    
    return documentos_ordenados

# Configuración de la página
st.set_page_config(page_title="Motor de Búsqueda", page_icon="🔍")

st.title("🔍 Motor de Búsqueda Personalizado")

# Entrada de la consulta
query = st.text_input("Introduce tu consulta:")

# Entrada de extensiones
extensiones:str = st.text_input("Introduce extensiones posibles separadas por comas:")

extensiones=extensiones.replace(',',' ')

extensiones=extensiones.split(' ')

temp=[]

for ext in extensiones:
    if ext not in ['', ' ']:
        temp.append(ext)

extensiones=temp
# Botón para realizar la búsqueda
if st.button("Realizar Búsqueda"):
    if query:
        resultados = realizar_busqueda(query, extensiones)
        
        if resultados:
            for doc in resultados:
                #{"id":result.id,"title":result.title,"snipet":result.snippet,"score":result.score}
                with st.expander(f"{doc['title']} (Score: {doc['score']})"):
                    st.write(doc["snipet"])
                    if st.button(f"Mostrar contenido completo de {doc['title']}", key=doc['id']):
                        st.write(doc["snipet"])
        else:
            st.info("No se encontraron documentos que coincidan con la consulta.")
    else:
        st.warning("Por favor, introduce una consulta.")
