from Pre_Processing.Document import Document, DATA_UBICATION
from typing import List
import gensim
def build_words_id_dictionary(docs : List[Document]) -> gensim.corpora.Dictionary:
    words_id_dictionary = gensim.corpora.Dictionary([doc.representation for doc in docs])
    words_id_dictionary.save(DATA_UBICATION+"word_id_dic")
    return words_id_dictionary