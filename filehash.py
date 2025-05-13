import hashlib
import blake3
from timeit import timeit

def hash_file(file, algorithm):
    hash = hashlib.new(algorithm)
    with open(file, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            hash.update(chunk)
    digest = hash.digest()
    #print(" ", digest)
    return digest

def hash_file_blake3(file):
    file_hasher = blake3.blake3(max_threads=4)
    file_hasher.update_mmap(file)
    digest = file_hasher.digest()
    #print(" ", digest)
    return digest


if __name__ == "__main__":
    print("blake3")
    f = min(timeit(lambda: hash_file_blake3("/tmp/bigfile"), number=1)
            for i in range(5))
    print("\t", f)
    for digest in hashlib.algorithms_available:
        print(digest)
        try:
            f = min(timeit(lambda : hash_file("/tmp/bigfile", digest), number=1)
                    for i in range(5))
            print("\t", f)
        except Exception:
            continue

