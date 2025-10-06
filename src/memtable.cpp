#include "lsm/memtable.h"

namespace lsm {

Memtable::Memtable(size_t max_size)
    : current_size_(0), max_size_(max_size) {}

void Memtable::put(const std::string& key, const std::string& value) {
    auto it = data_.find(key);
    if (it != data_.end()) {
        // update: adjust size
        current_size_ -= it->second.size();
        it->second = value;
    } else {
        data_[key] = value;
        current_size_ += key.size();
    }
    current_size_ += value.size();
}

std::optional<std::string> Memtable::get(const std::string& key) const {
    auto it = data_.find(key);
    if (it == data_.end()) return std::nullopt;
    if (it->second == TOMBSTONE) return std::nullopt;
    return it->second;
}

void Memtable::remove(const std::string& key) {
    put(key, TOMBSTONE);
}

bool Memtable::is_full() const {
    return current_size_ >= max_size_;
}

void Memtable::clear() {
    data_.clear();
    current_size_ = 0;
}

}
