from Pre_Processing.Document import Document, DATA_UBICATION
from typing import List
import gensim

def build_tf_idf_model(documents : List[Document]):
    corpus = [doc.representation for doc in documents]
    tfidf_model = gensim.models.TfidfModel(corpus, normalize = True)
    return tfidf_model