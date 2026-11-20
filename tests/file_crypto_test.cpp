#include "file_crypto.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <fstream>
#include <stdexcept>
#include <vector>
using namespace spark;
void check(bool yes,const char* why){if(!yes)throw std::runtime_error(why);}
struct File {int fd;File(){char name[]="/tmp/spark-crypto-test-XXXXXX";fd=mkstemp(name);check(fd>=0,"temporary file");unlink(name);}~File(){close(fd);}void set(const std::string& data){check(ftruncate(fd,0)==0&&lseek(fd,0,SEEK_SET)==0,"truncate");size_t at=0;while(at<data.size()){auto n=write(fd,data.data()+at,data.size()-at);check(n>0,"write");at+=n;}}};
std::string encode(const std::string& input){std::string out;size_t at=0;encrypt_file_bytes(input.size(),"test password 🔐",[&](unsigned char* buf,size_t n){std::memcpy(buf,input.data()+at,n);at+=n;},[&](const unsigned char* buf,size_t n){out.append(reinterpret_cast<const char*>(buf),n);});return out;}
std::string decode(const std::string& input,const std::string& password="test password 🔐"){File f;f.set(input);std::string out;decrypt_file_bytes(f.fd,password,[&](const unsigned char* p,size_t n){out.append(reinterpret_cast<const char*>(p),n);});return out;}
template<class F>void fails(F fn){bool bad=false;try{fn();}catch(const std::exception&){bad=true;}check(bad,"expected authenticated rejection");}
int main(int argc,char** argv){try{
 if(argc==3){std::string known(1048577,'\0');for(size_t i=0;i<known.size();++i)known[i]=char(i%251);if(std::string(argv[1])=="--encode"){auto encoded=encode(known);std::ofstream out(argv[2],std::ios::binary);out.write(encoded.data(),encoded.size());check(bool(out),"write interoperability fixture");}else{std::ifstream in(argv[2],std::ios::binary);std::string encoded((std::istreambuf_iterator<char>(in)),{});check(decode(encoded)==known,"independent implementation decrypt");}return 0;}

 for(size_t size:{size_t(0),size_t(1),size_t(1048576),size_t(1048577),size_t(3*1048576+17)}){
  std::string plain(size,'\0');for(size_t i=0;i<size;++i)plain[i]=char(i%256);
  auto encrypted=encode(plain);check(encrypted!=plain,"ciphertext differs");check(decode(encrypted)==plain,"roundtrip exact binary bytes");check(encrypted!=encode(plain),"fresh salt and nonce on every encryption");
  File f;f.set(encrypted);auto info=file_cipher_info(f.fd);check(info.encrypted&&info.plaintext_bytes==size,"envelope metadata");
  fails([&]{decode(encrypted,"wrong password");});
  for(size_t offset:{size_t(16),size_t(32),size_t(47),encrypted.size()-1}){auto bad=encrypted;bad[offset]^=1;fails([&]{decode(bad);});}
  fails([&]{decode(encrypted.substr(0,encrypted.size()-1));});fails([&]{decode(encrypted+"extra");});
 }
 std::string plain(2*1048576,'x');auto encrypted=encode(plain);auto reordered=encrypted;auto block=1048576+16;reordered.replace(48,block,encrypted.substr(48+block,block));reordered.replace(48+block,block,encrypted.substr(48,block));fails([&]{decode(reordered);});
 File f;f.set("ordinary text");check(!file_cipher_info(f.fd).encrypted,"plaintext recognized");f.set("SPARKENC");fails([&]{file_cipher_info(f.fd);});
 std::cout<<"Encrypted file format: binary/empty/multi-chunk roundtrips, randomness, wrong passwords, tampering, truncation and reordering passed.\n";return 0;
 }catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
