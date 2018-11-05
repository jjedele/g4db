# Testing

## RecordReaderTest

`RecordReader` is a class we wrote to encapsulate the logic of reading delimited records from a binary data stream. In `RecordReaderTest` we make sure that it works as expected for small and large amounts of data.

## ProtocolTest

`Protocol` encapsulates the logic of how KVMessages are encoded into binary format. In `ProtocolTest` we make sure that it correctly encodes and decodes all types of messages.

## DiskStorageTest

`DiskStorage` is contains the code with which data is persisted to disk. In the corresponding test we make sure that the basic functionality works as expected.

## CacheTest

`LFUCache`, `LRUCache` and `FIFOCache` are caching a configured number of items so that not every GET request leads to a disk access. In `CacheTest` we make sure that elements are ejected from the cache in the right order.