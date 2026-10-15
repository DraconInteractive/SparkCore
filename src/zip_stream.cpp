#include "zip_stream.hpp"
#include <array>
#include <limits>
#include <stdexcept>
namespace spark {
namespace {
void put(std::string& s,uint64_t n,int width){for(int i=0;i<width;++i){s.push_back(char(n&255));n>>=8;}}
}
uint32_t zip_crc(uint32_t crc,const char* data,size_t size){
    static const auto table=[] {std::array<uint32_t,256> t{};for(unsigned i=0;i<256;++i){uint32_t c=i;for(int j=0;j<8;++j)c=(c>>1)^((c&1)?0xedb88320u:0);t[i]=c;}return t;}();
    crc=~crc;for(size_t i=0;i<size;++i)crc=table[(crc^static_cast<unsigned char>(data[i]))&255]^(crc>>8);return ~crc;
}
ZipLayout::ZipLayout(const std::vector<ZipEntry>& entries){
    auto add=[&](std::string bytes,int file,uint64_t count){if(count>uint64_t(std::numeric_limits<int64_t>::max())-size)throw std::runtime_error("archive_too_large");parts.push_back({size,count,std::move(bytes),file});size+=count;};
    std::string central;
    for(size_t i=0;i<entries.size();++i){const auto& e=entries[i];if(e.name.empty()||e.name.size()>65535)throw std::runtime_error("invalid_archive_name");auto offset=size;
        std::string h;put(h,0x04034b50,4);put(h,45,2);put(h,0x800,2);put(h,0,2);put(h,0,2);put(h,33,2);put(h,e.crc,4);put(h,0xffffffff,4);put(h,0xffffffff,4);put(h,e.name.size(),2);put(h,20,2);h+=e.name;put(h,1,2);put(h,16,2);put(h,e.size,8);put(h,e.size,8);auto n=h.size();add(std::move(h),-1,n);if(e.size)add({},int(i),e.size);
        put(central,0x02014b50,4);put(central,45,2);put(central,45,2);put(central,0x800,2);put(central,0,2);put(central,0,2);put(central,33,2);put(central,e.crc,4);put(central,0xffffffff,4);put(central,0xffffffff,4);put(central,e.name.size(),2);put(central,28,2);put(central,0,2);put(central,0,2);put(central,0,2);put(central,e.name.back()=='/'?16:0,4);put(central,0xffffffff,4);central+=e.name;put(central,1,2);put(central,24,2);put(central,e.size,8);put(central,e.size,8);put(central,offset,8);
    }
    auto start=size,length=uint64_t(central.size());add(std::move(central),-1,length);auto end=size;std::string tail;
    put(tail,0x06064b50,4);put(tail,44,8);put(tail,45,2);put(tail,45,2);put(tail,0,4);put(tail,0,4);put(tail,entries.size(),8);put(tail,entries.size(),8);put(tail,length,8);put(tail,start,8);
    put(tail,0x07064b50,4);put(tail,0,4);put(tail,end,8);put(tail,1,4);
    put(tail,0x06054b50,4);put(tail,0,2);put(tail,0,2);put(tail,65535,2);put(tail,65535,2);put(tail,0xffffffff,4);put(tail,0xffffffff,4);put(tail,0,2);auto n=tail.size();add(std::move(tail),-1,n);
}
}
