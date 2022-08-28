package com.paypal.dione.spark.index

import com.paypal.dione.SparkCleanTestDB
import com.paypal.dione.hdfs.index.HdfsIndexContants._
import com.paypal.dione.hdfs.index.HdfsIndexerMetadata
import com.paypal.dione.kvstorage.hadoop.avro.AvroHashBtreeStorageFolderReader
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._


@TestInstance(Lifecycle.PER_CLASS)
abstract class TestIndexManagerBase() extends SparkCleanTestDB {

  override val dbName: String = "index_manager_base"
  override val baseTestPath: String = "TestData/TestIndexManagerBase/"

  def indexSpec: IndexSpec

  val samplePartition: String

  case class SampleTest(key: String, moreFields: Seq[String], varValue: String, offset: Long, subOffset: Int, size: Int)
  val testSamples: Seq[SampleTest]

  def initDataTable(fieldsSchema: String, partitionFieldName: String): Unit

  @Test
  @Order(0)
  def initData(): Unit = {

    import spark.implicits._

    initDataTable("id_col string, var1 string, var2 int, meta_field string",
      "dt")

    (1 to 300).map(i => ("msg_" + i, "var_a_" + i, i, "meta_"+i))
      .toDF("id_col", "var1", "var2", "meta_field")
      .repartition(3)
      .createOrReplaceTempView("t")

    spark.sql(s"insert overwrite table ${indexSpec.dataTableName} partition (dt=${samplePartition}) select * from t")
  }


  @Test
  @Order(1)
  def testCreateIndexManager(): Unit = {
    IndexManager.createNew(indexSpec)(spark)
  }

  @Test
  @Order(2)
  def testLoadIndexManager(): Unit = {
    IndexManager.load(indexSpec.indexTableName)(spark)
  }

  @Test
  @Order(3)
  def testAppendNewPartitions(): Unit = {
    val indexManager = IndexManager.load(indexSpec.indexTableName)(spark)
    indexManager.appendMissingPartitions()

    Assertions.assertEquals(spark.table(indexSpec.dataTableName).count(), spark.table(indexSpec.indexTableName).count())
  }

  @Test
  @Order(4)
  def testLoadByIndex(): Unit = {
    val indexManager = IndexManager.load(indexSpec.indexTableName)(spark)
    val sampleKeys = testSamples.map("'" + _.key + "'").mkString("(", "," ,")")
    val queryDF = indexManager.getIndex().where(s"pmod(hash(id_col), 10) == 7 or id_col in $sampleKeys")
    val payloadDF = indexManager.loadByIndex(queryDF, Some(Seq("var1")))

    Assertions.assertEquals(queryDF.count, queryDF.join(payloadDF, indexSpec.keys).count)

    testSamples.foreach(sample => {
      Assertions.assertEquals(List(Row(Seq(sample.key) ++ sample.moreFields ++ Seq(sample.varValue):_*)).toString,
        payloadDF.where(s"id_col in $sampleKeys")
          .select((indexSpec.keys ++ indexSpec.moreFields ++ Seq("var1")).map(col):_*)
          .collect().toList.toString)
    })
  }

  @Test
  @Order(4)
  def testLoadByIndexDuplicateRequests(): Unit = {
    val indexManager = IndexManager.load(indexSpec.indexTableName)(spark)
    val sampleDF = spark.createDataFrame(testSamples.map(x=>(x.key, x.size))).toDF("id_col", "some_size")
    // duplicate the row a few times
    val queryDF = indexManager.getIndex().join(sampleDF, "id_col")
      .withColumn("num", expr("explode(array(1,2,3))"))
    val payloadDF = indexManager.loadByIndex(queryDF, Some(Seq("var1")))

    Assertions.assertEquals(testSamples.size * 3, payloadDF.select("id_col").count)
    Assertions.assertEquals(testSamples.size, payloadDF.select("id_col").distinct().count)
  }

  @Order(5)
  @Test
  def testNoSparkGetAndFetch(): Unit = {
    val showPartition = spark.sql(s"desc formatted ${indexSpec.indexTableName} partition (dt="+samplePartition+")").collect()
    val specificIndexFolder = showPartition.find(row => row.getString(0).contains("Location")).get.getString(1)
    val avroHashBtreeFolderReader = AvroHashBtreeStorageFolderReader(specificIndexFolder)

    testSamples.foreach(sample => {
      val gr = avroHashBtreeFolderReader.get(Seq(sample.key)).get
      Assertions.assertEquals((sample.offset, sample.subOffset, sample.size),
        (gr.get(OFFSET_COLUMN), gr.get(SUB_OFFSET_COLUMN), gr.get(SIZE_COLUMN)))

      val indexManager = IndexManager.load(indexSpec.indexTableName)(spark)
      val payloadSchema = StructType(Seq(StructField("var1", StringType)))
      val hdfsIndexer = indexManager.sparkIndexer.initHdfsIndexer(new Path(gr.get(FILE_NAME_COLUMN).toString), fs.getConf, payloadSchema)
      val tData = hdfsIndexer.fetch(HdfsIndexerMetadata(gr))
      val grData = indexManager.sparkIndexer.convertMap(tData)
      Assertions.assertEquals(Some(sample.varValue), grData.get("var1").map(_.toString))
    })
  }

  @Order(6)
  @Test
  def testFetchAll(): Unit = {
    val indexManager = IndexManager.load(indexSpec.indexTableName)(spark)
    testSamples.foreach(sample => {
      // to support also non string partition types we added "'" to the partition value (not the cleanest way)
      val samplePartitionClean = samplePartition.replace("'", "")
      val vars = indexManager.fetch(Seq(sample.key), Seq("dt" -> samplePartitionClean))
      Assertions.assertEquals(sample.varValue, vars.get("var1").toString)
      Assertions.assertEquals("List((id_col,msg_100), (meta_field,meta_100), (var1,var_a_100), (var2,100))",
        vars.get.toList.sortBy(_._1).toString)
    })
  }

  @Order(7)
  @Test
  def testFetch(): Unit = {
    val indexManager = IndexManager.load(indexSpec.indexTableName)(spark)
    testSamples.foreach(sample => {
      val samplePartitionClean = samplePartition.replace("'", "")
      val vars = indexManager.fetch(Seq(sample.key), Seq("dt" -> samplePartitionClean), Some(Seq("var1")))
      Assertions.assertEquals(sample.varValue, vars.get("var1"))
      Assertions.assertFalse(vars.get.contains("var2"))
      Assertions.assertEquals("List((id_col,msg_100), (var1,var_a_100))",
        vars.get.toList.sortBy(_._1).toString)
    })
  }
}
