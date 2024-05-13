import os
import spacy
from typing import List, Tuple
import gensim
from Pre_Processing.Document import Document, DOCUMENTS_UBICATION, DATA_UBICATION
from Pre_Processing.TF_IDF import build_tf_idf_model
from Pre_Processing.Similarity_Matrix import build_similarity_matrix
from Pre_Processing.Doc_Term_Matrix import build_doc_term_matrix
from Pre_Processing.Corpus import Corpus, load_all_dates, save_all_dates
from Pre_Processing.Document_Folder_Access import read_documents
import json
import utils

def prepare_corpus():
    docs = read_documents()
    save_docs_names(docs)
    current_titles = [d.title for d in docs]
    procesed_titles = load_docs_names()
    is_eq = utils.is_eq(current_titles,procesed_titles)
    if not is_eq:
        process_documents(docs)
        save_docs_names(docs)
    return load_all_dates()


def process_documents(docs : List[Document]):
    word_id_dictionary, doc_term_matrix = build_doc_term_matrix(docs)
    model = build_tf_idf_model(docs)
    sim_matrix = build_similarity_matrix(model, doc_term_matrix)
    corpus = Corpus(docs,model,word_id_dictionary,sim_matrix)
    save_all_dates(corpus)
def save_docs_names(docs : List[Document]):
    with open(DATA_UBICATION + "docs_names",'w') as file:
        json.dump([d.title for d in docs],file)
def load_docs_names():
    with open(DATA_UBICATION + "docs_names",'r') as file:
        return json.load(file)

