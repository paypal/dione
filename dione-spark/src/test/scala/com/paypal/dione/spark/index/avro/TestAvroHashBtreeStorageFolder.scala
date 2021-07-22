package com.paypal.dione.spark.index.avro

import com.paypal.dione.SparkWithTestFolder
import com.paypal.dione.kvstorage.hadoop.avro.AvroHashBtreeStorageFolderReader
import com.paypal.dione.spark.avro.btree.SparkAvroBtreeUtils
import org.apache.spark.sql.functions._
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api.{Assertions, Order, Test, TestMethodOrder}

object TestAvroHashBtreeStorageFolder extends SparkWithTestFolder {
  override val baseTestPath: String = "TestData/TestAvroHashBtreeStorageFolder/"
}

@TestMethodOrder(classOf[OrderAnnotation])
class TestAvroHashBtreeStorageFolder {

  import TestAvroHashBtreeStorageFolder._

  @Test
  @Order(1)
  def testWriter(): Unit = {

    val data = spark.range(100)
      .withColumn("prt", expr("if(id<50, 'small', 'big')"))
      .withColumn("id", expr("cast(id as long)"))
      .withColumn("val1", expr("floor(id*2)"))

    SparkAvroBtreeUtils.writePartitionedDFasAvroBtree(data, Seq("id"), baseTestPath + "data", 3,
      3, Seq((Seq("prt" -> "small"), 10), (Seq("prt" -> "big"), 10)))(spark)
  }

  @Test
  @Order(2)
  def testReadNoSpark(): Unit = {
    val kvGetterSmall = AvroHashBtreeStorageFolderReader(baseTestPath + "data/prt=small")
    1.to(50, 5).foreach(i => {
      println("trying to 'get': " + i)
      Assertions.assertEquals(i*2L, kvGetterSmall.get(Seq(i.toLong)).get.get("val1"))
    })

    val kvGetterBig = AvroHashBtreeStorageFolderReader(baseTestPath + "data/prt=big")
    50.to(99, 5).foreach(i => {
      println("trying to 'get': " + i)
      Assertions.assertEquals(i*2L, kvGetterBig.get(Seq(i.toLong)).get.get("val1"))
    })
  }

}
