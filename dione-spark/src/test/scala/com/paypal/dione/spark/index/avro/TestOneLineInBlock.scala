package com.paypal.dione.spark.index.avro

import com.paypal.dione.SparkCleanTestDB
import com.paypal.dione.hdfs.index.HdfsIndexContants._
import com.paypal.dione.hdfs.index.HdfsIndexerMetadata
import com.paypal.dione.hdfs.index.avro.AvroIndexer
import com.paypal.dione.kvstorage.hadoop.avro.AvroHashBtreeStorageFolderReader
import com.paypal.dione.spark.avro.btree.SparkAvroBtreeUtils
import com.paypal.dione.spark.index.{IndexManager, IndexSpec}
import org.apache.hadoop.fs.Path
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api._

object TestOneLineInBlock extends SparkCleanTestDB {

  override val baseTestPath: String = "TestData/TestOneLineInBlock/"
  override val dbName: String = "TestOneLineInBlock"

  @BeforeAll
  def initData(): Unit = {
    import spark.implicits._

    //    val sc = spark.sparkContext
    //    sc.hadoopConfiguration.setInt("avro.mapred.sync.interval", 100)

    val varsDF = (1 to 30).map(i => ("msg_" + i, "sub_msg_"+i, "var_a_" + i, i, "2018-10-04 12:34:" + (i % 60).toString.padTo(2, '0'),
//      (1 to 2).mkString("")))
      (1 to 20000).mkString("")))
      .toDF("message_id", "sub_message_id", "var1", "var2", "time_result_created", "blah").repartition(3)

    varsDF.createOrReplaceTempView("t")
    spark.sql(s"create table t3 (message_id string, sub_message_id string, var1 string, var2 int, time_result_created string, blah string) " +
      s"partitioned by (dt string) stored as avro")
    spark.sql(s"insert overwrite table t3 partition (dt='2018-10-04') select * from t")


    spark.table("t3").show()
  }

}

@TestMethodOrder(classOf[OrderAnnotation])
class TestOneLineInBlock {

  import TestAvroIndexManager._

  @Test
  @Order(1)
  def testCreateIndexManager(): Unit = {
    IndexManager.createNew(IndexSpec("t3", "index_t3", Seq("message_id", "sub_message_id"), Seq("time_result_created")))(spark)
    spark.sql("desc formatted index_t3").show(100, false)
    // ensure one row per block
    import spark.implicits._
    assert(spark.table("index_t3").filter($"data_sub_offset" > 0).count == 0)

  }

  @Test
  @Order(2)
  def testAppendNewPartitions(): Unit = {
    val indexManager = IndexManager.load("index_t3")(spark)
    indexManager.appendMissingPartitions()

    //spark.table("index_t3").show(100, false)

    Assertions.assertEquals(30, spark.table("index_t3").count())

    Assertions.assertEquals(List("[msg_1,sub_msg_1,2018-10-04 12:34:10,356354,0,88948]"),
      spark.table("index_t3").drop("path", "file", FILE_NAME_COLUMN, "dt", "metadata")
        .where("message_id='msg_1'").collect().toList.map(_.toString()))
  }

}
