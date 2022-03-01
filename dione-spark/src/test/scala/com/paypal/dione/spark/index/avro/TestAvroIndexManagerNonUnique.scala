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
    spark.sql(s"insert overwrite table tbl partition (dt='2018-10-04') select * from t")

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
  @Order(3)
  def testAppendNewPartitions(): Unit = {
    val indexManager = IndexManager.load("index_tbl")(spark)
    indexManager.appendMissingPartitions()

    Assertions.assertEquals(30, spark.table("index_tbl").count())
    Assertions.assertEquals(15, spark.table("index_tbl").select("key").distinct().count())
  }

  @Test
  @Order(4)
  def testLoadByIndex(): Unit = {
    val indexManager = IndexManager.load("index_tbl")(spark)
    val queryDF = indexManager.getIndex().where("key like '%11%'")
    val payloadDF = indexManager.loadByIndex(queryDF)
      .select("key", "val")

    Assertions.assertEquals(List("[msg_20,sub_msg_20,2018-10-04 12:34:20,var_a_20,20]"),
      payloadDF.collect().toList.map(_.toString()))
  }

//  @Order(5)
//  @Test
//  def testNoSparkGetAndFetch(): Unit = {
//    val basePath = baseTestPath + "hive/index_t3/"
//    val specificIndexFolder = basePath + "dt=2018-10-04"
//    val avroHashBtreeFolderReader = AvroHashBtreeStorageFolderReader(specificIndexFolder)
//    val valueOpt = avroHashBtreeFolderReader.get(Seq("msg_20", "sub_msg_20"))
//    val gr = valueOpt.get
//    Assertions.assertEquals(419L, gr.get(OFFSET_COLUMN))
//
//    val grData = AvroIndexer(new Path(gr.get(FILE_NAME_COLUMN).toString), 0, 1<<30, fs.getConf)
//      .fetch(HdfsIndexerMetadata(gr))
//    Assertions.assertEquals("var_a_20", grData.get("var1").toString)
//  }
//
//  @Order(6)
//  @Test
//  def testFetch(): Unit = {
//    val indexManager = IndexManager.load("index_t3")(spark)
//    val vars = indexManager.fetch(Seq("msg_20", "sub_msg_20"), Seq("dt" -> "2018-10-04"))
//    Assertions.assertEquals("var_a_20", vars.get("var1").toString)
//  }
//
//  @Order(7)
//  @Test
//  def testKVstoreNoIndex(): Unit = {
//    val kvFolder = baseTestPath + "kv_no_index"
//    SparkAvroBtreeUtils.writeDFasAvroBtree(spark.table("t3"), Seq("message_id"), kvFolder,
//      5, 5, 2)(spark)
//
//    val kvGetter = AvroHashBtreeStorageFolderReader(kvFolder)
//
//    Assertions.assertEquals(19, kvGetter.get(Seq("msg_19")).get.get("var2"))
//    Assertions.assertEquals(None, kvGetter.get(Seq("msg_119")))
//  }

}
