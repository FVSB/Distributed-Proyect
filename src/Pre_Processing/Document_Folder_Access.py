from Pre_Processing.Document import Document ,DOCUMENTS_UBICATION
import os
def read_documents():
    docs = []
    with os.scandir(DOCUMENTS_UBICATION) as files:
        for file in files:
            docs.append(read_document(file.name))
    return docs
def read_document(title):
    with open(DOCUMENTS_UBICATION+title,'r') as file:
        text = file.read()
    return Document(title, text)