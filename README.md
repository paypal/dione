# Dione
Dione - an indexing Library for data on HDFS.

The main offering is tools for building an index for HDFS data and querying the index in both:
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
- _Multi-row_ fetch - Avoid shuffling and deserializing all the data if users want only a small number of keys.

### Main Benefits
- Same technology for both batch processing and ad-hoc tasks.
- Ownership - we do not need to "own" the data and can thus can avoid duplication.
- Support multiple keys for the same data.
- both data and index are accessible in Hive.


## Main Components
This library contains three main components:
- **HDFS Indexer** - runs through the "real" data and saves metadata on each row to quickly fetch back the data upon request.
currently, supports Avro, Sequence and Parquet files.

- **AvroBtreeStorageFile** - A way to store a file on HDFS that given a key can quickly get its value.
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

# Quick Start
Examples could also be found in our [tests](src/test/scala/com/paypal/dione/spark/index).

## IndexManager API
This API is intended for an end-to-end managed solution.
All relevant metadata is stored in the index table's `TBLPROPERTIES`.

### Creating an Index
Define a new index on table `my_db.my_big_table` with key field `key1` and adding field `val1` also to the index table:
```
import com.paypal.dione.spark.index.{IndexManager, IndexSpec}

IndexManager.createNew(IndexSpec(dataTableName = "my_db.my_big_table",
                                 indexTableName = "my_db.my_index",
                                 keys = Seq("key1"),
                                 moreFields = Seq("val1")
                      ))(spark)
```
the index table `my_db.my_index` can be read as a regular Hive table. It will contain the relevant metadata per key and is
saved by our special Avro B-Tree format. 

Start to index partitions:
```
import com.paypal.dione.spark.index.IndexManager

// file mask
spark.conf.set("index.manager.file.filter", "%.avro")

val indexManager = IndexManager.load("my_db.my_index")(spark)

// assuming `my_db.my_big_table` is partitioned by `dt` 
indexManager.appendNewPartitions(Seq(Seq("dt" -> "2020-10-04"), Seq("dt" -> "2020-10-05")))
```

### Use the index to fetch the data
Query the index and use it to fetch the data:
```
import com.paypal.dione.spark.index.IndexManager

val indexManager = IndexManager.load("my_db.my_index")(spark)

// same as - spark.table("my_db.my_index").where("dt='2020-10-04'").limit(20)
val queryDF = indexManager.getIndex().where("dt='2020-10-04'").limit(20)

val payloadDF = indexManager.loadByIndex(queryDF, Some(Seq("col1", "col2")))
``` 
`queryDF` could be any manipulation on the index table - for example join it with another table.
`loadByIndex` function just needs the relevant metadata fields: `data_filename`, `data_offset`, `data_sub_offset`, `data_size`.


Fetch a specific key:
```
val dataOpt = indexManager.fetch(Seq("key1"), Seq("dt" -> "2020-10-01"))
```

## SparkIndexer API
This API is for manual calls for specific features. Users are responsible on preparing the inputs in the correct way.
Need to create the correct object according to the data being indexed (e.g `AvroSparkIndexer`, `ParquetSparkIndexer`, etc.)
Available functions are `createIndexDF`, `readPayload`, `loadByIndex`.

For example, indexing data:
```
import org.apache.spark.sql.types.{StringType, StructField, StructType}

val fieldsSchema = StructType(Seq("id", "var1").map(p => StructField(p, StringType)))
val indexedDF = AvroSparkIndexer(spark).createIndexDF(filesDF, fieldsSchema)
```
Reading data using the index:
```
val payloadSchema = StructType(Seq("var1", "var2").map(p => StructField(p, StringType)))
val df1 = spark.table("indexed_df").where("id like '123%'")
AvroSparkIndexer(spark).loadByIndex(df1, payloadSchema).show()
```
