# IndexManager Python API
Examples could also be found in our [tests](dione-spark/src/test/python/index_manager_test.py).

## Init
- Add Dione's jars to `spark.jars`:
  - `dione-spark-*.jar`
  - `dione-hadoop-*.jar`
- Also add Avro related jars to `spark.jars`:
  - `spark-avro_2.11-2.4.8.jar`
  - `parquet-avro-1.10.0.jar`
  - `avro-1.8.2.jar`
- add `dione-spark-*.jar` (which contains the python API) to:
  - `PYTHONPATH`
  - `spark.executorEnv.PYTHONPATH`
  - `spark.yarn.appMasterEnv.PYTHONPATH` (if relevant)

## Creating an Index
Define a new index on table `my_db.my_big_table` with key field `key1` and adding field `col1` also to the index table:
```python
from dione import IndexManager

IndexManager.create_new(spark, "my_db.my_big_table", "my_db.my_index", ["key1"], ["col1"])
```
the index table `my_db.my_index` can be read as a regular Hive table. It will contain the relevant metadata per key and is
saved by our special Avro B-Tree format.

Start to index partitions:
```python
# optional - file mask
spark.conf.set("index.manager.file.filter", "%.avro")

indexManager = IndexManager.load(spark, "my_db.my_index")

# assuming `my_db.my_big_table` is partitioned by `dt` 
indexManager.append_new_partitions([[("dt", "2020-10-04")], [("dt", "2020-10-05")]])
```

## Using the index
### Multi-Row
Query the index and use it to fetch the data:
```python
indexManager = IndexManager.load(spark, "my_db.my_index")

# example query
queryDF = spark.table("my_db.my_index").where("col1 like '%foo%'")

payload_DF = indexManager.load_by_index(queryDF, ["col1", "col2"])
``` 
`queryDF` could be any manipulation on the index table - for example join it with another table.
`load_by_index` function just needs the relevant metadata fields: `data_filename`, `data_offset`, `data_sub_offset`, `data_size`.

### Single-Row
Fetch a specific key:
```python
# fetch column `col3` of some key
data_val = indexManager.fetch(["key_bar"], [("dt", "2020-10-01")], ["col3"])
```
