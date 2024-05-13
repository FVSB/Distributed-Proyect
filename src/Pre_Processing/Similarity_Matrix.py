from typing import List, Tuple
import gensim
from Pre_Processing.Document import DATA_UBICATION

def build_similarity_matrix(model : gensim.models.TfidfModel, doc_term_matrix : List[Tuple[int,int]]) -> gensim.similarities.MatrixSimilarity:
    #sim_matrix = gensim.similarities.MatrixSimilarity(model[doc_term_matrix])
    sim_matrix = gensim.similarities.MatrixSimilarity(doc_term_matrix)
    return sim_matrix