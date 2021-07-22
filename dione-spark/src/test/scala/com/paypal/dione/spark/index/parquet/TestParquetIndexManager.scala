package com.paypal.dione.spark.index.parquet

import com.paypal.dione.SparkCleanTestDB
import com.paypal.dione.hdfs.index.HdfsIndexContants._
import com.paypal.dione.hdfs.index.HdfsIndexerMetadata
import com.paypal.dione.hdfs.index.parquet.ParquetIndexer
import com.paypal.dione.kvstorage.hadoop.avro.AvroHashBtreeStorageFolderReader
import com.paypal.dione.spark.index.{IndexManager, IndexSpec}
import org.apache.hadoop.fs.Path
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api._

object TestParquetIndexManager extends SparkCleanTestDB {

  override val baseTestPath: String = "TestData/TestParquetIndexManager/"
  override val dbName: String = "TestParquetIndexManager"

  @BeforeAll
  def initData(): Unit = {
    import spark.implicits._

    val sc = spark.sparkContext
    sc.hadoopConfiguration.setInt("parquet.block.size", 100)

    spark.sql(s"create table parquet_tbl (message_id string, sub_message_id string, var1 string, var2 int, time_result_created string) " +
      s"partitioned by (dt string) stored as parquet")

    (1 to 1000).map(i => ("msg_" + i, "sub_msg_"+i, "var_a_" + i, i, "2018-10-04 12:34:" + (i % 60).toString.padTo(2, '0')))
      .toDF("message_id", "sub_message_id", "var1", "var2", "time_result_created")
      .repartition(3)
      .createOrReplaceTempView("t")

    spark.sql(s"insert overwrite table parquet_tbl partition (dt='2018-10-04') select * from t")

    //spark.table("parquet_tbl").show()
  }

}

@TestMethodOrder(classOf[OrderAnnotation])
class TestParquetIndexManager {

  import TestParquetIndexManager._

  @Test
  @Order(1)
  def testCreateIndexManager(): Unit = {
    IndexManager.createNew(IndexSpec("parquet_tbl", "index_parquet_tbl", Seq("message_id", "sub_message_id"), Seq("time_result_created")))(spark)
    //spark.sql("desc formatted parquet_tbl").show(100, false)
  }

  @Test
  @Order(2)
  def testLoadIndexManager(): Unit = {
    IndexManager.load("index_parquet_tbl")(spark)
  }

  @Test
  @Order(3)
  def testAppendNewPartitions(): Unit = {
    val indexManager = IndexManager.load("index_parquet_tbl")(spark)
    indexManager.appendMissingPartitions()

    //spark.table("index_parquet_tbl").show(100, false)

    Assertions.assertEquals(1000, spark.table("index_parquet_tbl").count())

    Assertions.assertEquals(List("[msg_25,sub_msg_25,2018-10-04 12:34:25,1,99,-1,null,2018-10-04]"),
      indexManager.getIndex().drop("path", "file", FILE_NAME_COLUMN)
        .where("message_id='msg_25'").collect().toList.map(_.toString()))
  }

  @Test
  @Order(4)
  def testLoadByIndex(): Unit = {
    val indexManager = IndexManager.load("index_parquet_tbl")(spark)
    val queryDF = indexManager.getIndex().where("message_id like '%g_2%' and dt='2018-10-04'")
    //queryDF.show(1000, false)

    val payloadDF = indexManager.loadByIndex(queryDF, Some(Seq("var1", "var2")))
      .select("message_id", "sub_message_id", "time_result_created", "var1", "var2")

    Assertions.assertEquals(queryDF.count, queryDF.join(payloadDF, "message_id").count)

    Assertions.assertEquals(List("[msg_23,sub_msg_23,2018-10-04 12:34:23,var_a_23,23]"),
      payloadDF.where("message_id like 'msg_23'").collect().toList.map(_.toString()))
  }

  @Order(5)
  @Test
  def testNoSparkGetAndFetch(): Unit = {
    val basePath = baseTestPath + "hive/index_parquet_tbl/"
    val specificIndexFolder = basePath + "dt=2018-10-04"
    val avroHashBtreeFolderReader = AvroHashBtreeStorageFolderReader(specificIndexFolder)
    val valueOpt = avroHashBtreeFolderReader.get(Seq("msg_25", "sub_msg_25"))
    val gr = valueOpt.get
    Assertions.assertEquals(1L, gr.get(OFFSET_COLUMN))
    Assertions.assertEquals(99, gr.get(SUB_OFFSET_COLUMN))

    val grData = new ParquetIndexer(new Path(gr.get(FILE_NAME_COLUMN).toString), fs.getConf).fetch(HdfsIndexerMetadata(gr))
    Assertions.assertEquals("var_a_25", grData.get("var1").toString)
  }

  @Order(6)
  @Test
  def testFetch(): Unit = {
    val indexManager = IndexManager.load("index_parquet_tbl")(spark)
    val vars = indexManager.fetch(Seq("msg_25", "sub_msg_25"), Seq("dt" -> "2018-10-04"))
    Assertions.assertEquals("var_a_25", vars.get("var1").toString)
  }

}