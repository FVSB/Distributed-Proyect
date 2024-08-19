import heapq
import threading

import heapq
import threading

class DocsClassification:
    def __init__(self, id: int, title: str, snippet: str, score: float) -> None:
        self.id = id
        self.title = title
        self.snippet = snippet
        self.score = score

    def __lt__(self, other):
        # Invertir la comparación para que el heapq funcione como un max-heap
        return self.score > other.rank

class MaxHeap:
    def __init__(self):
        self.heap = []
        self.lock = threading.RLock()

    def push(self, item: DocsClassification):
        with self.lock:
            heapq.heappush(self.heap, item)

    def pop(self) -> DocsClassification:
        with self.lock:
            return heapq.heappop(self.heap)

    def top(self) -> DocsClassification:
        with self.lock:
            return self.heap[0]

    def __len__(self):
        with self.lock:
            return len(self.heap)




class QueryHandle:
    def __init__(self,query:str,posibles_extensions:list[str],guid:str,min_score:float=0) -> None:
        self.query:str=query
        self.posibles_extensions:list[str]=posibles_extensions
        self.guid:str=guid
        self.posible_docs:MaxHeap=MaxHeap()
        self.min_score:float=min_score
    def add_posible(self,doc_class:DocsClassification)->bool:
        if doc_class.score<self.min_score:
            return False
        
        self.posible_docs.push(doc_class)
        return True
    