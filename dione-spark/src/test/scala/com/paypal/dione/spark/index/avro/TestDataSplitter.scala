package com.paypal.dione.spark.index.avro

import com.paypal.dione.SparkCleanTestDB
import com.paypal.dione.spark.index.{IndexManager, IndexManagerUtils, IndexSpec}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api.{Assertions, Order, Test, TestMethodOrder}


object LongString {
  val value = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore"
}

case class DataSplitTester(spark: SparkSession, expectedNumChunks: Int, dataTable: String, indexTable: String) {

  import spark.implicits._

  def createData() = {
    // will generate ~2.2MB for both avro and seq.
    // setting indexer.files.chunkMB=1 will split to 3 for both.
    spark.range(10 << 10).select(
      $"id",
      concat_ws("_", lit("col1"), $"id", lit(LongString.value)) as "col1",
      concat_ws("_", lit("col2"), $"id", lit(LongString.value)) as "col2"
    ).coalesce(1).createOrReplaceTempView("t")
  }

  def sanity_testSplitCount(): Unit = {
    spark.conf.set("indexer.files.chunkMB", 1)

    val location = spark.sessionState.catalog.listPartitions(TableIdentifier(dataTable)).head.location
    val files = IndexManagerUtils.listFilesDF(Seq((Map("dt" -> "foo"), location.getPath)), spark).cache()
    Assertions.assertEquals(expectedNumChunks, files.count())
  }

  def fullE2ETest(): Unit = {
    spark.conf.set("indexer.files.chunkMB", 1)

    val manager = IndexManager.createNew(IndexSpec(dataTable, indexTable, Seq("id"), Seq("col1")))(spark)
    manager.appendMissingPartitions()
    val index = manager.getIndex()

    def duplicatedIds(df: DataFrame) = df.groupBy("id").count().filter($"count" > 1).count()

    // assert created index
    Assertions.assertEquals(10 << 10, spark.table(indexTable).count())
    // no duplicated keys:
    Assertions.assertEquals(0, duplicatedIds(index))
    // no duplicated indexes:
    Assertions.assertEquals(0, index.groupBy("data_offset", "data_sub_offset").count().filter($"count" > 1).count())

    // fetch entire index:
    val df = manager.loadByIndex(index, None).cache()
    Assertions.assertEquals(10 << 10, df.count)
    Assertions.assertEquals(0, duplicatedIds(df))
    df.collect().foreach { row =>
      val id = row.getAs[Long]("id")
      val c1 = row.getAs[String]("col1")
      val c2 = row.getAs[String]("col2")
      Assertions.assertEquals(s"col1_${id}_${LongString.value}", c1)
      Assertions.assertEquals(s"col2_${id}_${LongString.value}", c2)
    }
  }
}


object AvroSplitTest extends SparkCleanTestDB {
  override val dbName: String = "avro_split_db"
  override val baseTestPath: String = "TestData/avro_data"
}

@TestMethodOrder(classOf[OrderAnnotation])
class AvroSplitTest {

  import AvroSplitTest._

  def tester() = DataSplitTester(spark, 3, "avro_data", "avro_index")

  @Test
  @Order(0)
  def createData: Unit = {
    tester().createData()
    spark.sql(s"create table avro_data (id long, col1 string, col2 string) partitioned by (dt string) stored as avro ")
    spark.sql(s"insert overwrite table avro_data partition (dt='foo') select * from t")
  }

  @Test
  @Order(1)
  def sanity = tester().sanity_testSplitCount()

  @Test
  @Order(2)
  def e2eTest = tester().fullE2ETest()
}

object SeqSplitTest extends SparkCleanTestDB {
  override val dbName: String = "seq_split_db"
  override val baseTestPath: String = "TestData/seq_data"
}

@TestMethodOrder(classOf[OrderAnnotation])
class SeqSplitTest {

  import SeqSplitTest._

  def tester() = DataSplitTester(spark, 3, "seq_data", "seq_index")

  @Test
  @Order(0)
  def createData: Unit = {
    tester().createData()
    spark.sql(
      s"""create table seq_data (id long, col1 string, col2 string) partitioned by (dt string)
         |STORED AS INPUTFORMAT
         |   'org.apache.hadoop.mapred.SequenceFileInputFormat'
         |OUTPUTFORMAT
         |  'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'
         |""".stripMargin)
    spark.sql(s"insert overwrite table seq_data partition (dt='foo') select * from t")

    assert(spark.table("seq_data").count() == 10 << 10)

  }

  @Test
  @Order(1)
  def sanity = tester().sanity_testSplitCount

  @Test
  @Order(2)
  def e2eTest = tester().fullE2ETest
}