#pragma once
#include <string>
#include <fstream>
#include <functional>

namespace lsm {

// write-ahead log so we don't lose memtable data on crash
class WAL {
public:
    WAL(const std::string& path);
    ~WAL();

    void log_put(const std::string& key, const std::string& value);
    void log_delete(const std::string& key);

    // replay into a callback
    void replay(std::function<void(bool is_del, const std::string& key, const std::string& val)> cb);

    void reset();  // truncate after flush

private:
    std::string path_;
    std::ofstream out_;
    void write_record(uint8_t type, const std::string& key, const std::string& value);
};

}
