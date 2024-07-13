import hashlib
import socket
import time
import multiprocessing
import json
import threading

HOST = '0.0.0.0'
PUB_PORT = '8002'
ID = str(socket.gethostbyname(socket.gethostname()))
DIFFICULTY = 6

print(f"Running on {ID}")

class BlockchainNode:
    def __init__(self, id, port):
        self.id = id
        self.port = int(port)
        self.chain = []
        self.create_genesis_block()
        self.p = None

    def create_genesis_block(self):
        genesis_block = {
            'index': 0,
            'timestamp': time.time(),
            'data': 'Genesis Block',
            'previous_hash': '0',
            'nonce': 0,
            'hash': self.hash_block(0, time.time(), 'Genesis Block', '0', 0)
        }
        self.chain.append(genesis_block)

    def hash_block(self, index, timestamp, data, previous_hash, nonce):
        sha = hashlib.sha256()
        sha.update(f'{index}{timestamp}{data}{previous_hash}{nonce}'.encode())
        return sha.hexdigest()

    def create_new_block(self, data):
        previous_block = self.chain[-1]
        new_block = {
            'index': previous_block['index'] + 1,
            'timestamp': time.time(),
            'data': data,
            'previous_hash': previous_block['hash'],
            'nonce': 0,
            'hash': ''
        }
        new_block['nonce'], new_block['hash'] = self.proof_of_work(new_block)
        return new_block

    def proof_of_work(self, block):
        nonce = 0
        while True:
            hash_value = self.hash_block(block['index'], block['timestamp'], block['data'], block['previous_hash'], nonce)
            if hash_value[:DIFFICULTY] == '0' * DIFFICULTY:
                return nonce, hash_value
            nonce += 1

    def add_block(self, block):
        if self.is_valid_block(block, self.chain[-1]):
            self.chain.append(block)
            return True
        return False

    def is_valid_block(self, block, previous_block):
        if previous_block['index'] + 1 != block['index']:
            return False
        if previous_block['hash'] != block['previous_hash']:
            return False
        if self.hash_block(block['index'], block['timestamp'], block['data'], block['previous_hash'], block['nonce']) != block['hash']:
            return False
        return True

    def is_valid_chain(self, chain):
        for i in range(1, len(chain)):
            if not self.is_valid_block(chain[i], chain[i-1]):
                return False
        return True

    def broadcast_block(self, block, request_chain=False):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        if request_chain:
            message = json.dumps({'request_chain': True}).encode()
        else:
            message = json.dumps(block).encode()
        s.sendto(message, (str(socket.INADDR_BROADCAST), self.port))
        s.close()

    def request_chain(self):
        # Broadcast a request to get the chain from other nodes
        self.broadcast_block(None, request_chain=True)

    def start(self):
        listener_thread = threading.Thread(target=self.listen_for_blocks)
        listener_thread.start()
        self.p = multiprocessing.Process(target=make_pow, args=(self,))
        self.p.start()

    def listen_for_blocks(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind((HOST, int(self.port)))
        while True:
            msg, _ = s.recvfrom(1024)
            try:
                data = json.loads(msg.decode())
                if 'request_chain' in data:
                    # Respond with the entire chain
                    self.broadcast_chain()
                elif 'chain' in data:
                    # Received a chain
                    new_chain = data['chain']
                    if len(new_chain) > len(self.chain) and self.is_valid_chain(new_chain):
                        self.chain = new_chain
                        print("Chain synchronized with the network")
                    else:
                        print("Received invalid chain")
                else:
                    # Received a single block
                    if data['index'] == len(self.chain):
                        if self.add_block(data):
                            print(f'New block added: {data}')
                            self.p.terminate()
                            self.p.join()
                            self.p = multiprocessing.Process(target=make_pow, args=(self,))
                            self.p.start()
                        else:
                            print('Received invalid block', msg.decode())
                    elif data['index'] > len(self.chain):
                        print('Received block is too far ahead, requesting chain synchronization')
                        self.request_chain()
            except json.JSONDecodeError:
                pass

    def broadcast_chain(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        message = json.dumps({'chain': self.chain}).encode()
        s.sendto(message, (str(socket.INADDR_BROADCAST), self.port))
        s.close()

def make_pow(node):
    while True:
        new_block = node.create_new_block('Some transaction data')
        node.broadcast_block(new_block)

if __name__ == '__main__':
    node = BlockchainNode(ID, PUB_PORT)
    node.start()
