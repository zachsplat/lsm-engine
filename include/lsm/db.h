#pragma once
#include <string>
#include <optional>
#include <vector>
#include <memory>
#include <mutex>
#include "lsm/memtable.h"
#include "lsm/sstable.h"

namespace lsm {

struct Options {
    std::string dir = "/tmp/lsm_data";
    size_t memtable_size = 4 * 1024 * 1024;
    int max_sstables_before_compact = 4;
};

class DB {
public:
    DB(const Options& opts);
    ~DB();

    void put(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key);
    void remove(const std::string& key);

    // force flush memtable to disk
    void flush();

private:
    Options opts_;
    Memtable memtable_;
    std::vector<std::unique_ptr<SSTableReader>> sstables_;
    std::mutex mu_;  // lazy, just lock everything
    int next_sst_id_;

    void maybe_flush();
    std::string sst_path(int id);
    void load_existing();
    void maybe_compact();  // TODO
};

}
