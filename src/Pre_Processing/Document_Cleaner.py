from Pre_Processing.Document import Document
import spacy
nlp = spacy.load("en_core_web_sm")
def clean_document(doc : Document):
    tokenize_doc(doc)
    remove_noise(doc)
    remove_stop_words(doc)
    morphological_reduce(doc)    

def tokenize_doc(doc : Document):
    doc.representation = [token for token in nlp(doc.text)]

def remove_noise(doc : Document):
    doc.representation = [token for token in doc.representation if token.is_alpha]
    
def remove_stop_words(doc : Document):
    stop_words = spacy.lang.en.stop_words.STOP_WORDS
    doc.representation = [token for token in doc.representation if token.text not in stop_words]

def morphological_reduce(doc : Document):
        doc.representation = [token.lemma_ for token in doc.representation]