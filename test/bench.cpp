#include "lsm/db.h"
#include <chrono>
#include <cstdio>
#include <string>
#include <filesystem>
#include <random>

int main() {
    std::filesystem::remove_all("/tmp/lsm_bench");
    lsm::Options opts;
    opts.dir = "/tmp/lsm_bench";
    opts.memtable_size = 1024 * 1024;  // 1MB

    lsm::DB db(opts);

    auto start = std::chrono::high_resolution_clock::now();
    int n = 100000;
    for (int i = 0; i < n; i++) {
        db.put("key" + std::to_string(i), std::string(100, 'x'));
    }
    auto end = std::chrono::high_resolution_clock::now();
    double secs = std::chrono::duration<double>(end - start).count();
    printf("write %d keys: %.3fs (%.0f ops/s)\n", n, secs, n / secs);

    // random reads
    std::mt19937 rng(42);
    start = std::chrono::high_resolution_clock::now();
    int found = 0;
    for (int i = 0; i < n; i++) {
        auto v = db.get("key" + std::to_string(rng() % n));
        if (v) found++;
    }
    end = std::chrono::high_resolution_clock::now();
    secs = std::chrono::duration<double>(end - start).count();
    printf("read %d keys: %.3fs (%.0f ops/s), found=%d\n", n, secs, n / secs, found);

    return 0;
}
