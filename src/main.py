from To_Test.documents import add_random_documents
from Pre_Processing.Processor import process_documents, prepare_corpus
from Pre_Processing.Corpus import load_all_dates, DOCUMENTS_UBICATION
from Search.Search import search
import streamlit as st



corpus = prepare_corpus()
def home_page():
    st.title("Buscador")

def search_page():
    query = st.text_input("Introduce your query")
    if query:
        for d in search(query,corpus):
            st.write(d.title)

def edit_page():
    up_file = st.file_uploader("Cargar archivo", type=["pdf","txt","csv"])
    if up_file is not None:
        file_content_bytes = up_file.getvalue()
        try:
            file_content = file_content_bytes.decode('utf-8')
        except UnicodeDecodeError:
            st.error("El archivo seleccionado contiene informacion que no esta expresada en forma de text, imposible abrirlo")
            file_content = ""
        ta = st.text_area(up_file.name,value=file_content, height=500,disabled=file_content=="")
        new_file_content_bytes = ta.encode('utf-8')

        if st.button("Save"):
            value = file_content_bytes if file_content == "" else new_file_content_bytes
            save(up_file.name,value)
def save(name, value):
    with open(DOCUMENTS_UBICATION + name,mode='wb') as file:
        file.write(value)

options = st.sidebar.selectbox("Menu",["Home", "Busqueda", "Acceso a DB"])
if options == "Home":
    home_page()
elif options == "Busqueda":
    search_page()
elif options == "Acceso a DB":
    edit_page()





        