# lsm-engine

LSM tree storage engine. write-optimized, does the usual memtable -> sstable thing.

compaction is basic (size-tiered), bloom filters on sstables for read performance.

mostly an exercise in understanding how leveldb/rocksdb work under the hood.

## build

```
mkdir build && cd build
cmake ..
make
```

## usage

it's a library, not a standalone thing. see test/ for examples.
