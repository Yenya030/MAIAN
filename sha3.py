from eth_hash.auto import keccak

class keccak_256:
    def __init__(self, data=b''):
        self._data = bytearray(data)
    def update(self, data):
        self._data.extend(data)
    def digest(self):
        return keccak(bytes(self._data))
    def hexdigest(self):
        return keccak(bytes(self._data)).hex()
