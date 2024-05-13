import ir_datasets
import random
from Pre_Processing.Document import Document, DOCUMENTS_UBICATION

def get_documents(dataset_name : str):
    dataset = ir_datasets.load(dataset_name)
    return [Document(doc.title, doc.text) for doc in dataset.docs_iter()]

def get_random_documents(num_docs = 1,dataset_name = "cranfield"):
    document = get_documents(dataset_name)
    return random.sample(document,num_docs)

def add_document(doc : Document):
    with open(DOCUMENTS_UBICATION+doc.title,'w') as file:
        file.write(doc.text)

def add_random_documents(num_docs):
    docs = get_random_documents(num_docs)
    for d in docs: add_document(d)