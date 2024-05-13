from typing import List, Tuple
import gensim
from Pre_Processing.Document import Document, DATA_UBICATION
from Pre_Processing.Document_Folder_Access import read_documents
class Corpus:
    def __init__(self,docs : List[Document], model : gensim.models.TfidfModel, dictionary : gensim.corpora.Dictionary, sim_matrix : gensim.similarities.MatrixSimilarity):
        self.docs = docs
        self.model = model
        self.dictionary = dictionary
        self.sim_matrix = sim_matrix
    
    def add_documents(self, docs : List[Document]):
        self.dictionary.add_documents([d.representation for d in docs])

def load_all_dates() -> Corpus :
    sim_matrix = gensim.similarities.MatrixSimilarity.load(DATA_UBICATION + "sim_matrix")
    dictionary = gensim.corpora.Dictionary.load(DATA_UBICATION + "word_id_dic")
    model = gensim.models.TfidfModel.load(DATA_UBICATION + "tfidf_model")
    docs = read_documents()
    return Corpus(docs, model, dictionary, sim_matrix)

def save_all_dates(corpus : Corpus):
    corpus.sim_matrix.save(DATA_UBICATION + "sim_matrix")
    corpus.dictionary.save(DATA_UBICATION + "word_id_dic")
    corpus.model.save(DATA_UBICATION + "tfidf_model")