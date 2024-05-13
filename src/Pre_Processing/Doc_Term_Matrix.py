from typing import List, Tuple
import gensim
from Pre_Processing.Document_Cleaner import clean_document
from Pre_Processing.Document import Document
from Pre_Processing.Word_Id_Dictionary import build_words_id_dictionary
def to_BoW(doc : Document, word_id_dictionary : gensim.corpora.Dictionary):
    doc.representation = word_id_dictionary.doc2bow(doc.representation)

def build_doc_term_matrix(docs : List[Document]):
    for doc in docs:
        clean_document(doc)
    word_id_dictionary = build_words_id_dictionary(docs)
    for doc in docs:
        to_BoW(doc,word_id_dictionary)
    doc_term_matrix = [doc.representation for doc in docs]
    return word_id_dictionary, doc_term_matrix