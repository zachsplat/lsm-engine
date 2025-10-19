#include "lsm/wal.h"
#include <cstring>

namespace lsm {

WAL::WAL(const std::string& path)
    : path_(path), out_(path, std::ios::binary | std::ios::app) {}

WAL::~WAL() {
    out_.close();
}

void WAL::write_record(uint8_t type, const std::string& key, const std::string& value) {
    uint32_t klen = key.size();
    uint32_t vlen = value.size();
    out_.write(reinterpret_cast<char*>(&type), 1);
    out_.write(reinterpret_cast<char*>(&klen), 4);
    out_.write(key.data(), klen);
    out_.write(reinterpret_cast<char*>(&vlen), 4);
    out_.write(value.data(), vlen);
    out_.flush();  // sync every write, slow but safe
}

void WAL::log_put(const std::string& key, const std::string& value) {
    write_record(0x01, key, value);
}

void WAL::log_delete(const std::string& key) {
    write_record(0x02, key, "");
}

void WAL::replay(std::function<void(bool, const std::string&, const std::string&)> cb) {
    std::ifstream in(path_, std::ios::binary);
    if (!in.is_open()) return;

    while (in.peek() != EOF) {
        uint8_t type;
        uint32_t klen, vlen;
        in.read(reinterpret_cast<char*>(&type), 1);
        if (in.eof()) break;
        in.read(reinterpret_cast<char*>(&klen), 4);
        std::string key(klen, '\0');
        in.read(key.data(), klen);
        in.read(reinterpret_cast<char*>(&vlen), 4);
        std::string val(vlen, '\0');
        in.read(val.data(), vlen);

        cb(type == 0x02, key, val);
    }
}

void WAL::reset() {
    out_.close();
    out_.open(path_, std::ios::binary | std::ios::trunc);
}

}
