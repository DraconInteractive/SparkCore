#pragma once
#include <cstdint>
#include <string>
#include <vector>
namespace spark {
struct ZipEntry {std::string name;uint64_t size=0;uint32_t crc=0;};
struct ZipPart {uint64_t offset=0,size=0;std::string bytes;int file=-1;};
struct ZipLayout {std::vector<ZipPart> parts;uint64_t size=0;explicit ZipLayout(const std::vector<ZipEntry>& entries);};
uint32_t zip_crc(uint32_t crc,const char* data,size_t size);
}
