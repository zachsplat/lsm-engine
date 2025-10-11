#pragma once
#include <string>
#include <optional>
#include <vector>
#include <fstream>
#include "lsm/bloom.h"

namespace lsm {

// on-disk sorted string table
// format: [data block][index block][bloom block][footer]
// super simplified, no compression

struct SSTableEntry {
    std::string key;
    std::string value;
};

class SSTableWriter {
public:
    SSTableWriter(const std::string& path);
    void add(const std::string& key, const std::string& value);
    void finish();  // writes index + bloom + footer

private:
    std::ofstream file_;
    BloomFilter bloom_;
    std::vector<std::pair<std::string, uint64_t>> index_;  // key -> offset
    uint64_t offset_;
};

class SSTableReader {
public:
    SSTableReader(const std::string& path);
    std::optional<std::string> get(const std::string& key);
    std::string path() const { return path_; }

    // for compaction: iterate all entries
    std::vector<SSTableEntry> scan_all();

private:
    std::string path_;
    BloomFilter bloom_;
    std::vector<std::pair<std::string, uint64_t>> index_;
    void load_metadata();
};

}
