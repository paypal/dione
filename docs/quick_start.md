# Quick Start
Examples could also be found in our [tests](dione-spark/src/test/scala/com/paypal/dione/spark/index).

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
