package com.paypal.dione.spark.index.avro

import com.paypal.dione.SparkCleanTestDB
import com.paypal.dione.hdfs.index.HdfsIndexContants._
import com.paypal.dione.hdfs.index.HdfsIndexerMetadata
import com.paypal.dione.hdfs.index.avro.AvroIndexer
import com.paypal.dione.kvstorage.hadoop.avro.AvroHashBtreeStorageFolderReader
import com.paypal.dione.spark.avro.btree.SparkAvroBtreeUtils
import com.paypal.dione.spark.index.avro.TestAvroIndexManager.spark
import com.paypal.dione.spark.index.{IndexManager, IndexSpec}
import org.apache.hadoop.fs.Path
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api._

object TestAvroIndexManagerNonUnique extends SparkCleanTestDB {

  override val baseTestPath: String = "TestData/TestAvroIndexManagerNonUnique/"
  override val dbName: String = "TestAvroIndexManagerNonUnique"

  @BeforeAll
  def initData(): Unit = {
    import spark.implicits._

    val varsDF = (1 to 30).map(i => ("k_" + (i-(1-i%2)), "v_" + i))
      .toDF("key", "val").repartition(3)

    varsDF.createOrReplaceTempView("t")
    spark.sql(s"create table tbl (key string, val string) partitioned by (dt string) stored as avro")
    spark.sql(s"insert overwrite table tbl partition (dt='2021-10-04') select * from t")

    spark.table("tbl").show(100, false)
  }

}

@TestMethodOrder(classOf[OrderAnnotation])
class TestAvroIndexManagerNonUnique {

  import TestAvroIndexManager._

  @Test
  @Order(1)
  def testCreateIndexManager(): Unit = {
    IndexManager.createNew(IndexSpec("tbl", "index_tbl", Seq("key")))(spark)
    spark.sql("desc formatted index_tbl").show(100, false)
  }

  @Test
  @Order(2)
  def testAppendNewPartitions(): Unit = {
    val indexManager = IndexManager.load("index_tbl")(spark)
    indexManager.appendMissingPartitions()

    Assertions.assertEquals(30, spark.table("index_tbl").count())
    Assertions.assertEquals(15, spark.table("index_tbl").select("key").distinct().count())
  }

  @Order(3)
  @Test
  def testFetch(): Unit = {
    val indexManager = IndexManager.load("index_tbl")(spark)
    val vars = indexManager.fetch(Seq("k_11"), Seq("dt" -> "2021-10-04"))
    // the order of the values within specific key is not guaranteed
    Assertions.assertTrue(Seq("v_11", "v_12").contains(vars.get("val").toString))
  }

  @Order(4)
  @Test
  def testFetchAll(): Unit = {
    val indexManager = IndexManager.load("index_tbl")(spark)
    val vars = indexManager.fetchAll(Seq("k_11"), Seq("dt" -> "2021-10-04"))
    Assertions.assertEquals(List("v_11", "v_12"), vars.map(_("val").toString).toList.sorted)
  }

}
