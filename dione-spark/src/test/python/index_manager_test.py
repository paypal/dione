from pyspark.sql import SparkSession
from dione import IndexManager
import shutil
import os


def remove_folder(path):
    if os.path.exists(path):
        shutil.rmtree(path)


remove_folder('metastore_db')
remove_folder('spark-warehouse')

dione_jars = [
    "dione-hadoop/target/dione-hadoop-0.6.0-SNAPSHOT.jar",
    "dione-spark/target/dione-spark-0.6.0-SNAPSHOT.jar"
]

spark_jars = [
    "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.11/2.4.8/spark-avro_2.11-2.4.8.jar",
    "https://repo1.maven.org/maven2/org/apache/parquet/parquet-avro/1.10.0/parquet-avro-1.10.0.jar",
    "https://repo1.maven.org/maven2/org/apache/avro/avro/1.8.2/avro-1.8.2.jar"
]

spark = SparkSession.builder \
    .appName("dione_python_test") \
    .enableHiveSupport() \
    .config("spark.sql.shuffle.partitions", 3) \
    .config("spark.jars", ",".join(dione_jars + spark_jars)) \
    .getOrCreate()

spark.sql("drop database if exists test_db cascade")
spark.sql("create database test_db")
spark.sql("use test_db")

# create dummy data
local_data = []
num_cols = 10

for i in range(0,1000):
    local_data.append(tuple(["c"+str(i)+"_"+str(c) for c in range(0,num_cols)]))

cols = ["col"+str(c) for c in range(0,num_cols)]
local_df = spark.createDataFrame(local_data, cols)
spark.sql("CREATE TABLE `local_tbl_p` ("+" string,".join(cols)+" string) partitioned by (dt string) stored as parquet")
local_df.createOrReplaceTempView("tmp_local")
spark.sql("""
insert overwrite table local_tbl_p partition (dt='2021-09-14')
select * from tmp_local
""")

spark.table("local_tbl_p").show()


# index owner
def test_create():
    IndexManager.create_new(spark, "local_tbl_p", "local_tbl_p_idx", ["col0"], ["col1"])


def test_load():
    IndexManager.load(spark, "local_tbl_p_idx")


def test_append_missing_partitions():
    im = IndexManager.load(spark, "local_tbl_p_idx")
    im.append_missing_partitions()


# index client
## Multi-Row
def test_load_by_index():
    query_df = spark.table("local_tbl_p_idx").where("hash(col1) % 10 = 7")
    im = IndexManager.load(spark, "local_tbl_p_idx")
    im.load_by_index(query_df, ["col3", "col4"]).show()


## Single-Row
def test_fetch():
    im = IndexManager.load(spark, "local_tbl_p_idx")
    r = im.fetch(["c6_0"], [("dt", "2021-09-14")], ["col7"])
    r.get().toString()
