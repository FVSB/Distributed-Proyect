import os
import spacy
from typing import List, Tuple
import gensim
from Pre_Processing.Document import Document, DOCUMENTS_UBICATION, DATA_UBICATION
from Pre_Processing.TF_IDF import build_tf_idf_model
from Pre_Processing.Similarity_Matrix import build_similarity_matrix
from Pre_Processing.Doc_Term_Matrix import build_doc_term_matrix
import json

def prepare_corpus():
    docs = read_documents()
    
def read_documents():
    docs = []
    with os.scandir(DOCUMENTS_UBICATION) as files:
        for file in files:
            docs.append(read_document(file.name))
    return docs
def read_document(title):
    with open(DOCUMENTS_UBICATION+title,'r') as file:
        text = file.read()
    return Document(title, text)

def process_documents(docs : List[Document]):
    word_id_dictionary, doc_term_matrix = build_doc_term_matrix(docs)
    model = build_tf_idf_model(docs)
    build_similarity_matrix(model, doc_term_matrix)

def save_docs_names(docs : List[Document]):
    pass




