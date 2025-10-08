#include "lsm/bloom.h"
#include <cstring>

namespace lsm {

BloomFilter::BloomFilter(size_t num_bits, int num_hashes)
    : bits_(num_bits, false), num_hashes_(num_hashes) {}

// not a great hash but whatever, it's a toy
uint64_t BloomFilter::hash(const std::string& key, int seed) const {
    uint64_t h = seed * 0x9e3779b97f4a7c15ULL;
    for (char c : key) {
        h ^= c;
        h *= 0x517cc1b727220a95ULL;
        h ^= h >> 32;
    }
    return h;
}

void BloomFilter::add(const std::string& key) {
    for (int i = 0; i < num_hashes_; i++) {
        size_t idx = hash(key, i) % bits_.size();
        bits_[idx] = true;
    }
}

bool BloomFilter::maybe_contains(const std::string& key) const {
    for (int i = 0; i < num_hashes_; i++) {
        size_t idx = hash(key, i) % bits_.size();
        if (!bits_[idx]) return false;
    }
    return true;
}

std::string BloomFilter::serialize() const {
    std::string out;
    uint32_t nb = bits_.size();
    uint32_t nh = num_hashes_;
    out.append(reinterpret_cast<char*>(&nb), 4);
    out.append(reinterpret_cast<char*>(&nh), 4);
    // pack bits into bytes
    for (size_t i = 0; i < bits_.size(); i += 8) {
        uint8_t byte = 0;
        for (int j = 0; j < 8 && i + j < bits_.size(); j++) {
            if (bits_[i + j]) byte |= (1 << j);
        }
        out.push_back(byte);
    }
    return out;
}

BloomFilter BloomFilter::deserialize(const std::string& data) {
    uint32_t nb, nh;
    std::memcpy(&nb, data.data(), 4);
    std::memcpy(&nh, data.data() + 4, 4);
    BloomFilter bf(nb, nh);
    size_t off = 8;
    for (size_t i = 0; i < nb && off < data.size(); i += 8, off++) {
        uint8_t byte = data[off];
        for (int j = 0; j < 8 && i + j < nb; j++) {
            bf.bits_[i + j] = (byte >> j) & 1;
        }
    }
    return bf;
}

}
