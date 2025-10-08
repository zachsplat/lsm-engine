#pragma once
#include <vector>
#include <string>
#include <cstdint>

namespace lsm {

// basic bloom filter. nothing clever, just murmur-ish hashes
class BloomFilter {
public:
    BloomFilter(size_t num_bits, int num_hashes);

    void add(const std::string& key);
    bool maybe_contains(const std::string& key) const;

    // serialization
    std::string serialize() const;
    static BloomFilter deserialize(const std::string& data);

private:
    std::vector<bool> bits_;
    int num_hashes_;
    uint64_t hash(const std::string& key, int seed) const;
};

}
