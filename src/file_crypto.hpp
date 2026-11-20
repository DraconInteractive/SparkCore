#pragma once
#include <cstdint>
#include <functional>
#include <string>
namespace spark {
struct FileCipherInfo { bool encrypted=false; uint64_t plaintext_bytes=0; };
// SPARKENC v1: scrypt key derivation and independently authenticated AES-256-GCM chunks.
FileCipherInfo file_cipher_info(int fd);
void encrypt_file_bytes(uint64_t size,const std::string& password,
    const std::function<void(unsigned char*,size_t)>& read,
    const std::function<void(const unsigned char*,size_t)>& write);
void decrypt_file_bytes(int fd,const std::string& password,
    const std::function<void(const unsigned char*,size_t)>& write);
}
