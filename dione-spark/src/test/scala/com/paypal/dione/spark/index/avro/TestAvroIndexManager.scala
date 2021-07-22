package com.paypal.dione.spark.index.avro

import com.paypal.dione.SparkCleanTestDB
import com.paypal.dione.hdfs.index.HdfsIndexContants._
import com.paypal.dione.hdfs.index.HdfsIndexerMetadata
import com.paypal.dione.hdfs.index.avro.AvroIndexer
import com.paypal.dione.kvstorage.hadoop.avro.AvroHashBtreeStorageFolderReader
import com.paypal.dione.spark.avro.btree.SparkAvroBtreeUtils
import com.paypal.dione.spark.index.{IndexManager, IndexManagerUtils, IndexSpec}
import org.apache.hadoop.fs.Path
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api._

object TestAvroIndexManager extends SparkCleanTestDB {

  override val baseTestPath: String = "TestData/TestAvroIndexManager/"
  override val dbName: String = "TestAvroIndexManager"

  @BeforeAll
  def initData(): Unit = {
    import spark.implicits._

    val varsDF = (1 to 30).map(i => ("msg_" + i, "sub_msg_"+i, "var_a_" + i, i, "2018-10-04 12:34:" + (i % 60).toString.padTo(2, '0')))
      .toDF("message_id", "sub_message_id", "var1", "var2", "time_result_created").repartition(3)

    varsDF.createOrReplaceTempView("t")
    spark.sql(s"create table t3 (message_id string, sub_message_id string, var1 string, var2 int, time_result_created string) " +
      s"partitioned by (dt string) stored as avro")
    spark.sql(s"insert overwrite table t3 partition (dt='2018-10-04') select * from t")

    spark.table("t3").show()
  }

}

@TestMethodOrder(classOf[OrderAnnotation])
class TestAvroIndexManager {

  import TestAvroIndexManager._

  @Test
  @Order(1)
  def testCreateIndexManager(): Unit = {
    IndexManager.createNew(IndexSpec("t3", "index_t3", Seq("message_id", "sub_message_id"), Seq("time_result_created")))(spark)
    spark.sql("desc formatted index_t3").show(100, false)
  }

  @Test
  @Order(2)
  def testLoadIndexManager(): Unit = {
    IndexManager.load("index_t3")(spark)
  }

  @Test
  @Order(2)
  def testMissingPartitons1(): Unit = {
    val im = IndexManager.load("index_t3")(spark)
    Assertions.assertEquals("(dt,2018-10-04)", im.getMissingPartitions().map(_.mkString("")).mkString(","))
  }

  @Test
  @Order(3)
  def testAppendNewPartitions(): Unit = {
    val indexManager = IndexManager.load("index_t3")(spark)
    indexManager.appendMissingPartitions()

    //spark.table("index_t3").show(100, false)

    Assertions.assertEquals(30, spark.table("index_t3").count())

    Assertions.assertEquals(List("[msg_20,sub_msg_20,2018-10-04 12:34:20,419,0,51]"),
      spark.table("index_t3").drop("path", "file", FILE_NAME_COLUMN, "dt", "metadata")
        .where("message_id='msg_20'").collect().toList.map(_.toString()))
  }

  @Test
  @Order(4)
  def testMissingPartitons2(): Unit = {
    val im = IndexManager.load("index_t3")(spark)
    Assertions.assertEquals("", im.getMissingPartitions().map(_.mkString("")).mkString(","))
  }

  @Test
  @Order(4)
  def testLoadByIndex(): Unit = {
    val indexManager = IndexManager.load("index_t3")(spark)
    val queryDF = indexManager.getIndex().where("message_id like '%2%' and dt='2018-10-04'")
    val payloadDF = indexManager.loadByIndex(queryDF, Some(Seq("var1", "var2")))
      .select("message_id", "sub_message_id", "time_result_created", "var1", "var2")

    Assertions.assertEquals(List("[msg_20,sub_msg_20,2018-10-04 12:34:20,var_a_20,20]"),
      payloadDF.where("message_id like '%20'").collect().toList.map(_.toString()))
  }

  @Order(5)
  @Test
  def testNoSparkGetAndFetch(): Unit = {
    val basePath = baseTestPath + "hive/index_t3/"
    val specificIndexFolder = basePath + "dt=2018-10-04"
    val avroHashBtreeFolderReader = AvroHashBtreeStorageFolderReader(specificIndexFolder)
    val valueOpt = avroHashBtreeFolderReader.get(Seq("msg_20", "sub_msg_20"))
    val gr = valueOpt.get
    Assertions.assertEquals(419L, gr.get(OFFSET_COLUMN))

    val grData = AvroIndexer(new Path(gr.get(FILE_NAME_COLUMN).toString), 0, 1<<30, fs.getConf)
      .fetch(HdfsIndexerMetadata(gr))
    Assertions.assertEquals("var_a_20", grData.get("var1").toString)
  }

  @Order(6)
  @Test
  def testFetch(): Unit = {
    val indexManager = IndexManager.load("index_t3")(spark)
    val vars = indexManager.fetch(Seq("msg_20", "sub_msg_20"), Seq("dt" -> "2018-10-04"))
    Assertions.assertEquals("var_a_20", vars.get("var1").toString)
  }

  @Order(7)
  @Test
  def testKVstoreNoIndex(): Unit = {
    val kvFolder = baseTestPath + "kv_no_index"
    SparkAvroBtreeUtils.writeDFasAvroBtree(spark.table("t3"), Seq("message_id"), kvFolder,
      5, 5, 2)(spark)

    val kvGetter = AvroHashBtreeStorageFolderReader(kvFolder)

    Assertions.assertEquals(19, kvGetter.get(Seq("msg_19")).get.get("var2"))
    Assertions.assertEquals(None, kvGetter.get(Seq("msg_119")))
  }

}
