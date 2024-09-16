import hashlib

def compute_checksum(data):
    data_str = str(data).encode('utf-8')
    return hashlib.md5(data_str).hexdigest()
