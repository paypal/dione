import pytest
from pyspark.sql import SparkSession
import os
import shutil
import sys
import re

print(sys.path)


def remove_folder(path):
    if os.path.exists(path):
        shutil.rmtree(path)


def parse_pom_property(pom_path, property_name):
    with open(pom_path) as f:
        content = f.read()
    match = re.search(rf"<{property_name}>([^<]+)</{property_name}>", content)
    if not match:
        raise ValueError(f"Property '{property_name}' not found in {pom_path}")
    return match.group(1)


ROOT_POM = os.path.join(os.path.dirname(__file__), "../../../../pom.xml")
SPARK_POM = os.path.join(os.path.dirname(__file__), "../../../pom.xml")

dione_version = parse_pom_property(ROOT_POM, "version")
spark_version = parse_pom_property(SPARK_POM, "spark.version")
scala_binary_version = parse_pom_property(ROOT_POM, "scala.binary.version")
parquet_avro_version = parse_pom_property(ROOT_POM, "parquet-avro.version")

MAVEN = "https://repo1.maven.org/maven2"


@pytest.fixture(scope="session")
def spark_session():

    remove_folder('metastore_db')
    remove_folder('spark-warehouse')

    dione_jars = [
        f"dione-hadoop/target/dione-hadoop-{dione_version}.jar",
        f"dione-spark/target/dione-spark-{dione_version}.jar"
    ]

    spark_jars = [
        f"{MAVEN}/org/apache/spark/spark-avro_{scala_binary_version}/{spark_version}/spark-avro_{scala_binary_version}-{spark_version}.jar",
        f"{MAVEN}/org/apache/parquet/parquet-avro/{parquet_avro_version}/parquet-avro-{parquet_avro_version}.jar"
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
