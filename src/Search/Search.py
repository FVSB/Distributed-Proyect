from typing import List
from Pre_Processing.Document import Document
from Pre_Processing.Corpus import Corpus
from Pre_Processing.Document_Cleaner import clean_document

def search(query : str, corpus : Corpus) -> List[Document]:
    query = Document('query', query)
    clean_document(query)
    query_bow = corpus.dictionary.doc2bow(query.representation)
    #sims = corpus.sim_matrix[corpus.model[query_bow]]
    sims = corpus.sim_matrix[query_bow]
    searched = []
    for index in sorted(enumerate(sims), key=lambda item: -item[1])[:3]:
        searched.append(corpus.docs[index[0]])
    return searched