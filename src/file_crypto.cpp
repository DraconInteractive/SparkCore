#include "file_crypto.hpp"
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/crypto.h>
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
namespace spark {
namespace {
constexpr size_t chunk=1048576,header_size=48;
using Header=std::array<unsigned char,header_size>;
constexpr unsigned char magic[16]={'S','P','A','R','K','E','N','C',1,0,0,0,0,0,0,0};
void check(bool ok){if(!ok)throw std::runtime_error("encryption_unavailable");}
void read_at(int fd,unsigned char* out,size_t count,uint64_t offset){while(count){auto n=pread(fd,out,count,offset);if(n<0&&errno==EINTR)continue;if(n<=0)throw std::runtime_error("file_read_failed");out+=n;offset+=n;count-=n;}}
uint64_t length(const Header& h){uint64_t n=0;for(size_t i=40;i<48;++i)n=(n<<8)|h[i];return n;}
uint64_t count_chunks(uint64_t size){auto n=size/chunk+(size%chunk!=0);if(n>UINT32_MAX)throw std::invalid_argument("file_too_large");return std::max(uint64_t(1),n);}
struct Key {std::array<unsigned char,32> bytes{};~Key(){OPENSSL_cleanse(bytes.data(),bytes.size());}Key(const std::string& password,const Header& h){check(EVP_PBE_scrypt(password.data(),password.size(),h.data()+16,16,32768,8,3,64ULL*1024*1024,bytes.data(),bytes.size())==1);}};
struct Buffer {std::vector<unsigned char> bytes;Buffer():bytes(chunk+16){}~Buffer(){OPENSSL_cleanse(bytes.data(),bytes.size());}};
Header header(int fd){Header h{};read_at(fd,h.data(),h.size(),0);if(std::memcmp(h.data(),magic,16))throw std::invalid_argument("unsupported_encryption_format");return h;}
void cipher(bool encrypt,const Key& key,const Header& h,uint32_t index,const unsigned char* input,size_t size,unsigned char* output,unsigned char* tag){
 std::array<unsigned char,12> nonce{};std::copy(h.begin()+32,h.begin()+40,nonce.begin());for(int i=0;i<4;++i)nonce[8+i]=(index>>(24-i*8))&255;
 std::unique_ptr<EVP_CIPHER_CTX,decltype(&EVP_CIPHER_CTX_free)> ctx(EVP_CIPHER_CTX_new(),EVP_CIPHER_CTX_free);check(bool(ctx));
 check(EVP_CipherInit_ex(ctx.get(),EVP_aes_256_gcm(),nullptr,key.bytes.data(),nonce.data(),encrypt?1:0)==1);
 int n=0;check(EVP_CipherUpdate(ctx.get(),nullptr,&n,h.data(),h.size())==1);check(EVP_CipherUpdate(ctx.get(),nullptr,&n,nonce.data()+8,4)==1);
 check(EVP_CipherUpdate(ctx.get(),output,&n,input,size)==1);check(n==int(size));
 if(!encrypt)check(EVP_CIPHER_CTX_ctrl(ctx.get(),EVP_CTRL_GCM_SET_TAG,16,tag)==1);
 int tail=0;auto ok=EVP_CipherFinal_ex(ctx.get(),output+n,&tail);
 if(!encrypt&&ok!=1)throw std::invalid_argument("incorrect_password_or_damaged_file");check(ok==1&&tail==0);
 if(encrypt)check(EVP_CIPHER_CTX_ctrl(ctx.get(),EVP_CTRL_GCM_GET_TAG,16,tag)==1);
}
}
FileCipherInfo file_cipher_info(int fd){
 struct stat st{};if(fstat(fd,&st)||!S_ISREG(st.st_mode)||st.st_size<0)throw std::runtime_error("file_read_failed");
 if(st.st_size<8)return {};unsigned char prefix[8];read_at(fd,prefix,8,0);if(std::memcmp(prefix,magic,8))return {};
 if(st.st_size<int64_t(header_size))throw std::invalid_argument("damaged_encrypted_file");auto h=header(fd);auto size=length(h);auto count=count_chunks(size);
 if(uint64_t(st.st_size)!=header_size+size+count*16)throw std::invalid_argument("damaged_encrypted_file");return {true,size};
}
void encrypt_file_bytes(uint64_t size,const std::string& password,const std::function<void(unsigned char*,size_t)>& read,const std::function<void(const unsigned char*,size_t)>& write){
 auto count=count_chunks(size);Header h{};std::copy(std::begin(magic),std::end(magic),h.begin());check(RAND_bytes(h.data()+16,24)==1);for(int i=0;i<8;++i)h[40+i]=(size>>(56-i*8))&255;
 Key key(password,h);write(h.data(),h.size());Buffer input,output;
 for(uint64_t i=0;i<count;++i){size_t n=std::min(uint64_t(chunk),size-i*chunk);read(input.bytes.data(),n);std::array<unsigned char,16> tag{};cipher(true,key,h,uint32_t(i),input.bytes.data(),n,output.bytes.data(),tag.data());write(output.bytes.data(),n);write(tag.data(),tag.size());}
}
void decrypt_file_bytes(int fd,const std::string& password,const std::function<void(const unsigned char*,size_t)>& write){
 auto info=file_cipher_info(fd);if(!info.encrypted)throw std::invalid_argument("file_not_encrypted");auto h=header(fd);Key key(password,h);Buffer input,output;auto count=count_chunks(info.plaintext_bytes);uint64_t offset=header_size;
 for(uint64_t i=0;i<count;++i){size_t n=std::min(uint64_t(chunk),info.plaintext_bytes-i*chunk);read_at(fd,input.bytes.data(),n,offset);offset+=n;std::array<unsigned char,16> tag{};read_at(fd,tag.data(),tag.size(),offset);offset+=tag.size();cipher(false,key,h,uint32_t(i),input.bytes.data(),n,output.bytes.data(),tag.data());write(output.bytes.data(),n);}
}
}
