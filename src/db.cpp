#include "lsm/db.h"
#include <filesystem>
#include <algorithm>

namespace fs = std::filesystem;

namespace lsm {

DB::DB(const Options& opts)
    : opts_(opts), memtable_(opts.memtable_size), next_sst_id_(0) {
    fs::create_directories(opts.dir);
    load_existing();
}

DB::~DB() {
    // flush remaining memtable
    if (memtable_.count() > 0) {
        flush();
    }
}

void DB::put(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mu_);
    memtable_.put(key, value);
    maybe_flush();
}

std::optional<std::string> DB::get(const std::string& key) {
    std::lock_guard<std::mutex> lock(mu_);

    // check memtable first
    auto val = memtable_.get(key);
    if (val) return val;

    // check sstables newest to oldest
    for (auto it = sstables_.rbegin(); it != sstables_.rend(); ++it) {
        auto v = (*it)->get(key);
        if (v) return v;
    }

    return std::nullopt;
}

void DB::remove(const std::string& key) {
    std::lock_guard<std::mutex> lock(mu_);
    memtable_.remove(key);
    maybe_flush();
}

void DB::flush() {
    if (memtable_.count() == 0) return;

    std::string path = sst_path(next_sst_id_++);
    SSTableWriter writer(path);

    for (auto it = memtable_.begin(); it != memtable_.end(); ++it) {
        writer.add(it->first, it->second);
    }
    writer.finish();

    sstables_.push_back(std::make_unique<SSTableReader>(path));
    memtable_.clear();

    // maybe_compact();  // not done yet
}

void DB::maybe_flush() {
    if (memtable_.is_full()) {
        flush();
    }
}

std::string DB::sst_path(int id) {
    return opts_.dir + "/sst_" + std::to_string(id) + ".dat";
}

void DB::load_existing() {
    if (!fs::exists(opts_.dir)) return;
    std::vector<std::string> paths;
    for (auto& entry : fs::directory_iterator(opts_.dir)) {
        if (entry.path().extension() == ".dat") {
            paths.push_back(entry.path().string());
        }
    }
    std::sort(paths.begin(), paths.end());
    for (auto& p : paths) {
        sstables_.push_back(std::make_unique<SSTableReader>(p));
        next_sst_id_++;
    }
}

void DB::maybe_compact() {
    // TODO: size-tiered compaction
    // merge overlapping sstables, remove tombstones
}

}
