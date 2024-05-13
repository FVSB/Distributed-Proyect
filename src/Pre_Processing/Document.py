DOCUMENTS_UBICATION = "data/documents/"
DATA_UBICATION = "data/data/"
class Document:
    def __init__(self, title : str, text : str):
        self.title = title
        self.text = text
        self.representation = None
