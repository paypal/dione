import pytest
from pyspark.sql import SparkSession
import os
import shutil
import sys

print(sys.path)


def remove_folder(path):
    if os.path.exists(path):
        shutil.rmtree(path)


@pytest.fixture(scope="session")
def spark_session():

    remove_folder('metastore_db')
    remove_folder('spark-warehouse')

    dione_jars = [
        "dione-hadoop/target/dione-hadoop-0.6.2-SNAPSHOT.jar",
        "dione-spark/target/dione-spark-0.6.2-SNAPSHOT.jar"
    ]

    spark_jars = [
        "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.11/2.4.8/spark-avro_2.11-2.4.8.jar",
        "https://repo1.maven.org/maven2/org/apache/parquet/parquet-avro/1.10.0/parquet-avro-1.10.0.jar",
        "https://repo1.maven.org/maven2/org/apache/avro/avro/1.8.2/avro-1.8.2.jar"
    ]

    spark = (SparkSession.builder
             .appName("dione_python_test")
             .master("local[1]")
             .enableHiveSupport()
             .config("spark.sql.shuffle.partitions", 3)
             .config("spark.jars", ",".join(dione_jars + spark_jars))
             .getOrCreate()
             )

    return spark