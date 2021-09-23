# Quick Start
Examples could also be found in our [tests](dione-spark/src/test/scala/com/paypal/dione/spark/index).

## Init
- Add Dione's jars to `spark.jars`:
    - `dione-spark-*.jar`
    - `dione-hadoop-*.jar`
- Also add Avro related jars to `spark.jars`:
    - `spark-avro_2.11-2.4.8.jar`
    - `parquet-avro-1.10.0.jar`
    - `avro-1.8.2.jar`
    
## IndexManager API
This API is intended for an end-to-end managed solution.
All relevant metadata is stored in the index table's `TBLPROPERTIES`.

### Creating an Index
Define a new index on table `my_db.my_big_table` with key field `key1` and adding field `col1` also to the index table:
```scala
import com.paypal.dione.spark.index.{IndexManager, IndexSpec}

IndexManager.createNew(IndexSpec(dataTableName = "my_db.my_big_table",
                                 indexTableName = "my_db.my_index",
                                 keys = Seq("key1"),
                                 moreFields = Seq("col1")
                      ))(spark)
```
the index table `my_db.my_index` can be read as a regular Hive table. It will contain the relevant metadata per key and is
saved by our special Avro B-Tree format.

Start to index partitions:
```scala
import com.paypal.dione.spark.index.IndexManager

// file mask
spark.conf.set("index.manager.file.filter", "%.avro")

val indexManager = IndexManager.load("my_db.my_index")(spark)

// assuming `my_db.my_big_table` is partitioned by `dt` 
indexManager.appendNewPartitions(Seq(Seq("dt" -> "2020-10-04"), Seq("dt" -> "2020-10-05")))
```

### Using the index
#### Multi-Row
Query the index and use it to fetch the data:
```scala
import com.paypal.dione.spark.index.IndexManager

val indexManager = IndexManager.load("my_db.my_index")(spark)

// same as - spark.table("my_db.my_index").where("dt='2020-10-04'").limit(20)
val queryDF = indexManager.getIndex().where("dt='2020-10-04'").limit(20)

val payloadDF = indexManager.loadByIndex(queryDF, Some(Seq("col1", "col2")))
``` 
`queryDF` could be any manipulation on the index table - for example join it with another table.
`loadByIndex` function just needs the relevant metadata fields: `data_filename`, `data_offset`, `data_sub_offset`, `data_size`.

#### Single-Row
Fetch a specific key:
```scala
val dataOpt = indexManager.fetch(Seq("key_bar"), Seq("dt" -> "2020-10-01"))
```

## SparkIndexer API
This API is for manual calls for specific features. Users are responsible on preparing the inputs in the correct way.
Need to create the correct object according to the data being indexed (e.g `AvroSparkIndexer`, `ParquetSparkIndexer`, etc.)
Available functions are `createIndexDF`, `readPayload`, `loadByIndex`.

For example, indexing data:
```scala
import org.apache.spark.sql.types.{StringType, StructField, StructType}

val fieldsSchema = StructType(Seq("id", "col1").map(p => StructField(p, StringType)))
val indexedDF = AvroSparkIndexer(spark).createIndexDF(filesDF, fieldsSchema)
```
Reading data using the index:
```scala
val payloadSchema = StructType(Seq("col1", "col2").map(p => StructField(p, StringType)))
val df1 = spark.table("indexed_df").where("col1 like '123%'")
AvroSparkIndexer(spark).loadByIndex(df1, payloadSchema).show()
```
