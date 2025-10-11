#include "lsm/sstable.h"
#include <cstring>
#include <algorithm>

namespace lsm {

// --- Writer ---

SSTableWriter::SSTableWriter(const std::string& path)
    : file_(path, std::ios::binary), bloom_(10240, 3), offset_(0) {}

void SSTableWriter::add(const std::string& key, const std::string& value) {
    bloom_.add(key);
    index_.push_back({key, offset_});

    // write key_len(4) key val_len(4) val
    uint32_t klen = key.size();
    uint32_t vlen = value.size();
    file_.write(reinterpret_cast<char*>(&klen), 4);
    file_.write(key.data(), klen);
    file_.write(reinterpret_cast<char*>(&vlen), 4);
    file_.write(value.data(), vlen);
    offset_ += 4 + klen + 4 + vlen;
}

void SSTableWriter::finish() {
    uint64_t index_offset = offset_;

    // write index: count(4), then entries
    uint32_t count = index_.size();
    file_.write(reinterpret_cast<char*>(&count), 4);
    for (auto& [key, off] : index_) {
        uint32_t klen = key.size();
        file_.write(reinterpret_cast<char*>(&klen), 4);
        file_.write(key.data(), klen);
        file_.write(reinterpret_cast<char*>(&off), 8);
    }

    // write bloom
    uint64_t bloom_offset = file_.tellp();
    std::string bloom_data = bloom_.serialize();
    uint32_t blen = bloom_data.size();
    file_.write(reinterpret_cast<char*>(&blen), 4);
    file_.write(bloom_data.data(), blen);

    // footer: index_offset(8) bloom_offset(8) magic(4)
    uint32_t magic = 0xDEADBEEF;
    file_.write(reinterpret_cast<char*>(&index_offset), 8);
    file_.write(reinterpret_cast<char*>(&bloom_offset), 8);
    file_.write(reinterpret_cast<char*>(&magic), 4);

    file_.flush();
    file_.close();
}

// --- Reader ---

SSTableReader::SSTableReader(const std::string& path)
    : path_(path), bloom_(1, 1) {
    load_metadata();
}

void SSTableReader::load_metadata() {
    std::ifstream f(path_, std::ios::binary | std::ios::ate);
    auto fsize = f.tellg();
    if (fsize < 20) return;  // too small, corrupted?

    // read footer
    f.seekg(fsize - std::streamoff(20));
    uint64_t index_offset, bloom_offset;
    uint32_t magic;
    f.read(reinterpret_cast<char*>(&index_offset), 8);
    f.read(reinterpret_cast<char*>(&bloom_offset), 8);
    f.read(reinterpret_cast<char*>(&magic), 4);

    if (magic != 0xDEADBEEF) return;  // corrupt

    // read index
    f.seekg(index_offset);
    uint32_t count;
    f.read(reinterpret_cast<char*>(&count), 4);
    for (uint32_t i = 0; i < count; i++) {
        uint32_t klen;
        f.read(reinterpret_cast<char*>(&klen), 4);
        std::string key(klen, '\0');
        f.read(key.data(), klen);
        uint64_t off;
        f.read(reinterpret_cast<char*>(&off), 8);
        index_.push_back({key, off});
    }

    // read bloom
    f.seekg(bloom_offset);
    uint32_t blen;
    f.read(reinterpret_cast<char*>(&blen), 4);
    std::string bdata(blen, '\0');
    f.read(bdata.data(), blen);
    bloom_ = BloomFilter::deserialize(bdata);
}

std::optional<std::string> SSTableReader::get(const std::string& key) {
    if (!bloom_.maybe_contains(key)) return std::nullopt;

    // binary search index
    auto it = std::lower_bound(index_.begin(), index_.end(), key,
        [](const std::pair<std::string, uint64_t>& p, const std::string& k) {
            return p.first < k;
        });

    if (it == index_.end() || it->first != key) return std::nullopt;

    std::ifstream f(path_, std::ios::binary);
    f.seekg(it->second);
    uint32_t klen, vlen;
    f.read(reinterpret_cast<char*>(&klen), 4);
    f.seekg(klen, std::ios::cur);  // skip key, we already know it
    f.read(reinterpret_cast<char*>(&vlen), 4);
    std::string val(vlen, '\0');
    f.read(val.data(), vlen);
    return val;
}

std::vector<SSTableEntry> SSTableReader::scan_all() {
    std::vector<SSTableEntry> entries;
    std::ifstream f(path_, std::ios::binary);

    for (auto& [_, off] : index_) {
        f.seekg(off);
        uint32_t klen, vlen;
        f.read(reinterpret_cast<char*>(&klen), 4);
        std::string key(klen, '\0');
        f.read(key.data(), klen);
        f.read(reinterpret_cast<char*>(&vlen), 4);
        std::string val(vlen, '\0');
        f.read(val.data(), vlen);
        entries.push_back({key, val});
    }
    return entries;
}

}
