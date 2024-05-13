from To_Test.documents import add_random_documents
from Pre_Processing.Processor import process_documents, read_documents
from Pre_Processing.Corpus import load_all_dates
from Search.Search import search
import streamlit as st

corpus = load_all_dates()
query = st.text_input("Introduce your query")

if query:
    for d in search(query,corpus):
        st.write(d.title)
        