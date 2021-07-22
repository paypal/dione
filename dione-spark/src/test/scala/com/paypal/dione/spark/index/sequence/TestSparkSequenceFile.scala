package com.paypal.dione.spark.index.sequence

import com.paypal.dione.SparkTestBase
import com.paypal.dione.spark.index.{IndexManager, IndexSpec}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api.{BeforeAll, Order, Test, TestMethodOrder}

object TestSparkSequenceFile extends SparkTestBase {

  @BeforeAll
  def init(): Unit = {

    if (fs.exists(new Path("TestData/TestSparkSequenceFile")))
      fs.delete(new Path("TestData/TestSparkSequenceFile"), true)

    spark.sql("drop database if exists TestSparkSequenceFile CASCADE")
    spark.sql("create database TestSparkSequenceFile LOCATION 'TestData/TestSparkSequenceFile/hive/'")
    spark.sql("use TestSparkSequenceFile")

    val testPath = new Path("TestData/foobar")
    spark.sql("drop table if exists foo purge")
    if (fs.exists(testPath)) fs.delete(testPath, true)
    spark.sql(
      s"""CREATE TABLE `foo`(
         |  `id` string,
         |  `col0` string,
         |  `col1` int,
         |  `col2` string,
         |  `col3` long)
         |PARTITIONED BY (
         |  `dt` string,
         |  `seq` string)
         |ROW FORMAT DELIMITED
         |  FIELDS TERMINATED BY '\u0010'
         |STORED AS INPUTFORMAT
         |  'org.apache.hadoop.mapred.SequenceFileInputFormat'
         |OUTPUTFORMAT
         |  'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'
         |TBLPROPERTIES (
         |  'last_modified_by'='dm_hdp_batch',
         |  'last_modified_time'='1440806187',
         |  'transient_lastDdlTime'='1440806187')""".stripMargin)

    spark.sql("desc formatted foo").show(99, false)

    insertSourcePartition("2020-01-01", "00", 0 until 100)
    insertSourcePartition("2020-01-01", "01", 50 until 150)
    insertSourcePartition("2020-01-02", "00", 100 until 200)
    assertEquals(300, spark.table("foo").count())

  }

  private def insertSourcePartition(dt: String, seq: String, data: Seq[Int]) = {
    createData(data).createTempView("data")

    spark.sql("insert overwrite table foo partition (dt='" + dt + "', seq='" + seq + "') select * from data")
    spark.sql("drop view data")
  }

  private def createData(data: Seq[Int]) = {
    import spark.implicits._

    data.toDF("id").select(
      $"id" cast "string",
      lit(null.asInstanceOf[String]) as "col0",
      when($"id" % 3 === 0, $"id") otherwise null.asInstanceOf[Int] as "col1",
      concat(lit("col2_"), $"id") as "col2",
      ($"id" * 2) as "col3"
    )
  }
}


import com.paypal.dione.spark.index.sequence.TestSparkSequenceFile._

@TestMethodOrder(classOf[OrderAnnotation])
class TestSparkSequenceFile {

  @Order(1)
  @Test
  def createNewIndex: Unit = {
    // dummy test to ensure initialization
    val indexer = IndexManager.createNew(IndexSpec("foo", "index", Seq("id"), Seq("col0", "col1")))(spark)

    assertEquals(0, spark.table(indexer.indexTableName).count())
  }

  @Order(2)
  @Test
  def addNewPartition: Unit = {
    val indexer = IndexManager.load("index")(spark)

    spark.conf.set("index.manager.btree.parts", 5)

    // add single partition:
    indexer.appendNewPartitions(Seq(
      Seq("dt" -> "2020-01-01", "seq" -> "00")
    ))

    val indexTable = spark.table("index")
    indexTable.show(false)
    assertEquals(100, indexTable.count())

    // add two more:
    indexer.appendMissingPartitions()
    assertEquals(300, indexTable.count())

    // validate index partitions:
    import spark.implicits._
    assertEquals(0, indexTable.filter($"dt" < "2020").count())
    assertEquals(100, indexTable.filter($"dt" === "2020-01-01" and $"seq" === "00").count())
    assertEquals(100, indexTable.filter($"dt" === "2020-01-01" and $"seq" === "01").count())
    assertEquals(100, indexTable.filter($"dt" === "2020-01-02").count())

  }

  @Order(3)
  @Test
  def testLoadByIndex(): Unit = {
    import spark.implicits._
    val indexManager = IndexManager.load("index")(spark)
    val indexTable = indexManager.getIndex()


    indexTable.printSchema()
    indexTable.show()


    { // read entire partition:
      val fullIndex = indexTable.filter($"dt" === "2020-01-02" and $"seq" === "00")

      val expected = createData(100 until 200)
      val res = indexManager.loadByIndex(fullIndex).cache()
      assertEquals(100, res.count())
      assertArrayEquals(Array[Long](), res.select("data_size").as[Long].collect().filter(_ == 0))

      import scala.collection.JavaConversions._
      val actual = res.select(expected.columns.map(col): _*).collect().map(_.toString()).sorted.toList
      val expected2 = expected.collect().map(_.toString()).sorted.toList
      assertIterableEquals(expected2, actual)
    }

    { // read random data:
      val index = indexTable.filter($"dt" === "2020-01-02" and $"seq" === "00").sample(0.5)
      indexManager.loadByIndex(index, None).collect().foreach(assertRow)
    }

    { // project specific columns:
      val index = indexTable.filter($"dt" === "2020-01-02" and $"seq" === "00").sample(0.5)
      val df = indexManager.loadByIndex(index, Some(Seq("col3")))
      assertFalse(df.columns.contains("col2"))
      assertTrue(df.columns.contains("col3"))
      df.withColumn("col2", concat(lit("col2_"), $"id")).collect().foreach(assertRow)
    }

  }

  @Order(4)
  @Test
  def testFetch(): Unit = {
    // ...
  }


  private def assertRow(row: Row) = {
    val id = row.getAs("id").toString
    val col0 = row.getAs[String]("col0")
    val col1 = row.getAs[Integer]("col1")
    val col2 = row.getAs[String]("col2")
    val col3 = row.getAs[Long]("col3")
    assertNull(col0)
    if (id.toInt % 3 == 0) assertEquals(id.toInt, col1)
    else assertNull(col1)
    assertEquals("col2_" + id, col2)
    assertEquals(id.toLong * 2, col3)
  }
}
