package com.paypal.dione.spark.index.avro

import java.io.File

import com.paypal.dione.SparkCleanTestDB
import com.paypal.dione.hdfs.index.HdfsIndexContants._
import com.paypal.dione.hdfs.index.HdfsIndexerMetadata
import com.paypal.dione.hdfs.index.avro.AvroIndexer
import com.paypal.dione.kvstorage.hadoop.avro.AvroHashBtreeStorageFolderReader
import com.paypal.dione.spark.avro.btree.SparkAvroBtreeUtils
import com.paypal.dione.spark.index.IndexManagerUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api._

object TestAvroSparkIndexer extends SparkCleanTestDB {

  override val baseTestPath: String = "TestData/TestAvroSparkIndexer/"
  override val dbName: String = "TestAvroSparkIndexer"

  @BeforeAll
  def initData(): Unit = {
    import spark.implicits._

    val varsDF = (1 to 30).map(i => ("msg_" + i, "var_a_" + i, "var_b_" + i, "2018-10-04 12:34:" + (i % 60).toString.padTo(2, '0')))
      .toDF("message_id", "var1", "var2", "time_result_created").repartition(3)

    varsDF.createOrReplaceTempView("t")
    spark.sql(s"drop table if exists t2")
    spark.sql(s"create table t2 stored as avro as select * from t")

    spark.table("t2").show()
  }

}

@TestMethodOrder(classOf[OrderAnnotation])
class TestAvroSparkIndexer {

  import TestAvroSparkIndexer._

  @Order(1)
  @Test
  def testAvroSparkIndexer(): Unit = {
    val showPartition = spark.sql("desc formatted t2").collect()
    val prtLocationStr = showPartition.find(row => row.getString(0).contains("Location")).get.getString(1)

    val filesDF = IndexManagerUtils.listFilesDF(Seq((Map.empty, prtLocationStr)), spark).drop("partition_def")
    val fieldsSchema = StructType(Seq("message_id", "time_result_created").map(p => StructField(p, StringType)))
    val indexedDF = AvroSparkIndexer(spark).createIndexDF(filesDF, fieldsSchema)

    //indexedDF.show()
    indexedDF.write.mode("overwrite").format("csv").saveAsTable("indexed_df")

    Assertions.assertEquals(List("[msg_20,2018-10-04 12:34:20,356,2,47]"),
      spark.table("indexed_df").drop("path", "file", FILE_NAME_COLUMN, "start_position", "end_position", "metadata")
        .where("message_id='msg_20'").collect().toList.map(_.toString()))
  }

  @Order(2)
  @Test
  def testSparkFetchWithIndex(): Unit = {
    val payloadSchema = StructType(Seq("var1", "var2").map(p => StructField(p, StringType)))
    val df1 = spark.table("indexed_df").where("message_id like '%2%'")
    val df2 = AvroSparkIndexer(spark).loadByIndex(df1, payloadSchema)
    val vars = df2.where("message_id like '%20'")
    spark.table("indexed_df").show()
    vars.cache.show()

    Assertions.assertEquals(List("[msg_20,2018-10-04 12:34:20,356,2,47,var_a_20,var_b_20]"),
      vars.drop("path", "file", FILE_NAME_COLUMN, "row_size", "start_position", "end_position", "metadata")
        .collect().toList.map(_.toString()))
  }

  @Order(3)
  @Test
  def testSparkStoringIndexInKV(): Unit = {
    val folderPath = baseTestPath + "spark_index"

    SparkAvroBtreeUtils.writeDFasAvroBtree(spark.table("indexed_df"),
      Seq("message_id"), folderPath, 3, 10, 2)(spark)

    val location = new File(folderPath).getAbsoluteFile.toString

    spark.sql(
      s"""
         | create external table avro_index_btree (
         | a string
         | )
         | TBLPROPERTIES ("avro.schema.url"="$location/.btree.avsc")
         | stored as avro
         | location '$location'
         |
         |""".stripMargin)

    spark.table("avro_index_btree").show()
    Assertions.assertEquals(30, spark.table("avro_index_btree").count())
  }

  @Order(4)
  @Test
  def testSparkLoadByIndex(): Unit = {
    val payloadSchema = StructType(Seq("var1", "var2").map(p => StructField(p, StringType)))
    val vars = AvroSparkIndexer(spark).loadByIndex(
      spark.table("avro_index_btree").where("message_id like '%20'"),
      payloadSchema)

    Assertions.assertEquals(List("[msg_20,2018-10-04 12:34:20,356,2,47,var_a_20,var_b_20]"),
      vars.drop("path", "file", FILE_NAME_COLUMN, "start_position", "end_position", "metadata")
        .collect().toList.map(_.toString()))
  }

  @Order(5)
  @Test
  def testSparkLoadByIndexDoesntExist(): Unit = {
    val payloadSchema = StructType(Seq("var1", "var2").map(p => StructField(p, StringType)))

    val df = spark.sql(
      """ select max(data_filename) as data_filename, count(*) as data_offset, 1 as data_sub_offset, 1 as data_size
        |from avro_index_btree where data_filename like '%part-00002%'""".stripMargin)

    df.show(false)

    spark.conf.set("indexer.reader.ignore.failures", "true")

    val vars = AvroSparkIndexer(spark).loadByIndex(df, payloadSchema)

    spark.conf.set("indexer.reader.ignore.failures", "false")

    Assertions.assertEquals(List.empty, vars.collect().toList.map(_.toString()))
  }

  @Order(6)
  @Test
  def testFetchWithKVIndexDirectly(): Unit = {
    val avroHashBtreeFolderReader = AvroHashBtreeStorageFolderReader(baseTestPath + "spark_index")

    val valueOpt = avroHashBtreeFolderReader.get(Seq("msg_20"))
    val gr = valueOpt.get
    Assertions.assertEquals(356L, gr.get(OFFSET_COLUMN))

    val grData = AvroIndexer(new Path(gr.get(FILE_NAME_COLUMN).toString), 0, 1<<30, fs.getConf).fetch(HdfsIndexerMetadata(gr))
    Assertions.assertEquals("var_a_20", grData.get("var1").toString)
  }
}