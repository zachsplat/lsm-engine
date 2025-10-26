#include "lsm/db.h"
#include "lsm/sstable.h"
#include <algorithm>
#include <filesystem>

namespace lsm {

// basic size-tiered compaction
// merge the oldest N sstables into one
// TODO: actually implement this properly with levels

/*
void DB::maybe_compact() {
    if (sstables_.size() < (size_t)opts_.max_sstables_before_compact)
        return;

    // merge first 2 sstables
    auto& s1 = sstables_[0];
    auto& s2 = sstables_[1];

    auto entries1 = s1->scan_all();
    auto entries2 = s2->scan_all();

    // merge sorted
    std::vector<SSTableEntry> merged;
    size_t i = 0, j = 0;
    while (i < entries1.size() && j < entries2.size()) {
        if (entries1[i].key <= entries2[j].key) {
            if (entries1[i].key == entries2[j].key) {
                // newer wins (s2 is newer)
                merged.push_back(entries2[j]);
                i++; j++;
            } else {
                merged.push_back(entries1[i]);
                i++;
            }
        } else {
            merged.push_back(entries2[j]);
            j++;
        }
    }
    while (i < entries1.size()) merged.push_back(entries1[i++]);
    while (j < entries2.size()) merged.push_back(entries2[j++]);

    // write merged sstable
    std::string path = sst_path(next_sst_id_++);
    SSTableWriter writer(path);
    for (auto& e : merged) {
        if (!Memtable::is_tombstone(e.value)) {
            writer.add(e.key, e.value);
        }
    }
    writer.finish();

    // swap
    auto old1 = s1->path();
    auto old2 = s2->path();
    sstables_.erase(sstables_.begin(), sstables_.begin() + 2);
    sstables_.push_back(std::make_unique<SSTableReader>(path));
    std::filesystem::remove(old1);
    std::filesystem::remove(old2);
}
*/

// placeholder until I actually test this
// the merge logic above is probably right but I don't trust it yet

}
