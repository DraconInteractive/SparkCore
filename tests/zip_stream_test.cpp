#include "zip_stream.hpp"
#include <stdexcept>
#include <iostream>
using namespace spark;
uint64_t le(const std::string& s,size_t offset,int n){uint64_t out=0;for(int i=0;i<n;++i)out|=uint64_t(static_cast<unsigned char>(s.at(offset+i)))<<(8*i);return out;}
void check(bool b){if(!b)throw std::runtime_error("ZIP64 layout mismatch");}
int main(){try{
    check(zip_crc(0,"123456789",9)==0xcbf43926);auto c=zip_crc(0,"1234",4);check(zip_crc(c,"56789",5)==0xcbf43926);
    ZipLayout layout({{"large.bin",uint64_t(5)<<30,123},{"next",10,456}});
    check(layout.parts[1].size==(uint64_t(5)<<30));check(layout.parts[2].offset>UINT32_MAX);
    auto& central=layout.parts[4].bytes;check(le(central,20,4)==UINT32_MAX);check(le(central,46+9+4,8)==(uint64_t(5)<<30));
    auto second=46+9+28;check(le(central,second+46+4+20,8)==layout.parts[2].offset);
    std::vector<ZipEntry> many(65536,{"a",0,0});ZipLayout large(many);auto& tail=large.parts.back().bytes;check(le(tail,24,8)==65536);check(le(tail,32,8)==65536);
    std::cout<<"CRC32, >4 GiB offsets and >65535 ZIP64 entries passed.\n";
}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
