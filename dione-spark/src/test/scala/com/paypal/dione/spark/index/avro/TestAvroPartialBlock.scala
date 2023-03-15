package com.paypal.dione.spark.index.avro

import com.paypal.dione.SparkCleanTestDB
import com.paypal.dione.spark.avro.btree.SparkAvroBtreeUtils
import com.paypal.dione.spark.index.{IndexManager, IndexSpec}
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api._


object TestAvroPartialBlock extends SparkCleanTestDB {

  override val baseTestPath: String = "TestData/TestAvroPartialBlock/"
  override val dbName: String = "TestAvroPartialBlock"

  @BeforeAll
  def initData(): Unit = {
    import spark.implicits._

    val N = 100000
    println("ohad: " + N)
    val varsDF = (1 to N).map(i => (i, "v1_" + i, "asdasd_"+i+"_blahblah", ("asd"+i)*10))
      .toDF("key", "val1", "strstr", "strstr2")

    varsDF.createOrReplaceTempView("t")
    spark.table("t").write.saveAsTable("tbl_data")

  }

}

@TestMethodOrder(classOf[OrderAnnotation])
class TestAvroPartialBlock {

  import TestAvroPartialBlock.spark

  @Test
  @Order(1)
  def test1(): Unit = {
    val im = IndexManager.createNew(IndexSpec("tbl_data", "index_tbl_data", Seq("key"), Seq("val1", "strstr", "strstr2")))(spark)
    SparkAvroBtreeUtils.writeDFasAvroBtree(spark.table("tbl_data"),
      Seq("key"), im.indexFolder, 1, 10, 2)(spark)
    spark.table("index_tbl_data").show()
  }

}