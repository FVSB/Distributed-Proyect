from typing import List, Tuple
import gensim
from Pre_Processing.Document import Document, DATA_UBICATION
from Pre_Processing.Processor import read_documents
class Corpus:
    def __init__(self,docs : List[Document], model , dictionary : gensim.corpora.Dictionary, sim_matrix : List[Tuple[int,int]]):
        self.docs = docs
        self.model = model
        self.dictionary = dictionary
        self.sim_matrix = sim_matrix

def load_all_dates() -> Corpus :
    sim_matrix = gensim.similarities.MatrixSimilarity.load(DATA_UBICATION + "sim_matrix")
    dictionary = gensim.corpora.Dictionary.load(DATA_UBICATION + "word_id_dic")
    model = gensim.models.TfidfModel.load(DATA_UBICATION + "tfidf_model")
    news = read_documents()
    return Corpus(news, model, dictionary, sim_matrix)