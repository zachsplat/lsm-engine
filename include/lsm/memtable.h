#pragma once
#include <map>
#include <string>
#include <optional>
#include <cstddef>

namespace lsm {

class Memtable {
public:
    Memtable(size_t max_size = 4 * 1024 * 1024);  // 4MB default

    void put(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key) const;
    void remove(const std::string& key);  // tombstone

    bool is_full() const;
    size_t size() const { return current_size_; }
    size_t count() const { return data_.size(); }

    // iterate in sorted order for flushing
    using iterator = std::map<std::string, std::string>::const_iterator;
    iterator begin() const { return data_.begin(); }
    iterator end() const { return data_.end(); }

    void clear();

private:
    std::map<std::string, std::string> data_;
    size_t current_size_;
    size_t max_size_;
    static constexpr const char* TOMBSTONE = "__TOMB__";
};

}  // namespace lsm
