# Dione
Dione - an indexing Library for data on HDFS.

The main offering is a tool for building an index for HDFS data and querying the index in following:
- _Single-row_ - `get(key)` with relatively low latency (a few seconds) depended only on HDFS.
- _Multi-row_ - (typically up to ~1% of the size of key space) -
  relying on some processing engine (e.g Spark), retrieve values in a few minutes without processing all the data.

### Assumptions on the Data
* Large key-value datasets.
* Large values (payloads): images, large text docs, web pages, blobs, large number of values per key, etc.
* Large key space - where it is not practical to save the keys to local disk or to memory.
* Append-only access pattern - no updates are allowed to already saved data.

### The problem we solve
- _Single-row_ fetch - enable fast key-value fetches for data in HDFS.
- _Multi-row_ fetch - avoid shuffling and deserializing all the data if users want only a small number of keys.

### Main Benefits
- Same technology for both batch processing and ad-hoc tasks.
- Ownership - we do not need to "own" the data and can thus avoid duplication.
- Support multiple keys for the same data.
- both data and index are accessible in Hive.

## Main Components
This library contains three main components:
- **HDFS Indexer** - runs through the "real" data and saves metadata on each row to quickly fetch back the data upon request.
  currently, supports Avro, Sequence and Parquet files.

- **AvroBtreeStorageFile** - A way to store a file on HDFS that by a given key can quickly get its value.
  Leverages Avro so can be read in Hive, Spark, etc.

- **SparkIndexer**/**IndexManager** - Spark APIs to leverage Spark for distribution of both index creation and _multi-row_ fetch.

Notes:
* HDFSIndexer and AvroBtreeStorageFile are completely decoupled and each could be used on its own.
* Spark here is used for metadata management and distribution. Similar capabilities could easily be written in Hive, Pig, etc.

### HDFS Indexer
Given the data, the index should contain the relevant metadata to retrieve the data.
e.g. the filename and line number in which the data exist.
With that information we can directly read from the place where the specific line we need exists without going through
all the huge data, deserializing it, etc.

Note here that the data's file format should support this "random access" (`seek` method).
Example files that support this are Text files, Sequence files and AVRO (which supports block level random access).
We also support Parquet files even-though they only support block level random access.

### AvroBtreeStorageFile
An Avro file that is saved as a b-tree. Each Avro block is mapped to a node in the B-Tree, the rows inside a block
are sorted and each row can point to another node in the b-tree.
This special structure in addition to Avro's block level fast random-access capability lets us traverse the file quickly
to find a given key with minimum number of hops.
