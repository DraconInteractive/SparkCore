#!/usr/bin/env python3
"""Independent envelope check using Python cryptography (test-only dependency)."""
import hashlib,os,subprocess,tempfile
from pathlib import Path
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
password='test password 🔐'.encode();chunk=1048576
plain=bytes(i%251 for i in range(1048577))
def key(header):return hashlib.scrypt(password,salt=header[16:32],n=32768,r=8,p=3,maxmem=64*1024*1024,dklen=32)
with tempfile.TemporaryDirectory(prefix='spark-crypto-interop-') as directory:
 path=Path(directory)/'cipher.bin'
 subprocess.run(['./build/file_crypto_tests','--encode',str(path)],check=True)
 encoded=path.read_bytes();header=encoded[:48]
 assert header[:16]==b'SPARKENC\x01'+b'\0'*7
 assert int.from_bytes(header[40:48],'big')==len(plain)
 aes=AESGCM(key(header));recovered=b'';at=48
 for index,start in enumerate(range(0,len(plain),chunk)):
  n=min(chunk,len(plain)-start);counter=index.to_bytes(4,'big')
  recovered+=aes.decrypt(header[32:40]+counter,encoded[at:at+n+16],header+counter);at+=n+16
 assert recovered==plain and at==len(encoded)
 header=b'SPARKENC\x01'+b'\0'*7+os.urandom(24)+len(plain).to_bytes(8,'big');aes=AESGCM(key(header));encoded=header
 for index,start in enumerate(range(0,len(plain),chunk)):
  counter=index.to_bytes(4,'big');encoded+=aes.encrypt(header[32:40]+counter,plain[start:start+chunk],header+counter)
 path.write_bytes(encoded)
 subprocess.run(['./build/file_crypto_tests','--decode',str(path)],check=True)
print('PASS: C++ encryption independently decrypted by Python; Python encryption independently decrypted by C++')
