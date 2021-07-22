package com.paypal.dione.spark.index.avro

import com.paypal.dione.SparkCleanTestDB
import com.paypal.dione.spark.index.{IndexManager, IndexSpec}
import org.junit.jupiter.api.{Assertions, Test}
import org.junit.jupiter.api.function.Executable


object TestAvroDecimalConverter extends SparkCleanTestDB {
  override val dbName: String = "decimal_test"
  override val baseTestPath: String = "TestData/decimal_test"
}

class TestAvroDecimalConverter {

  // we use seq file just as source format,
  // the actual test is for the avro (btree) converter

  import TestAvroDecimalConverter._

  @Test
  def testE2E: Unit = {
    import spark.implicits._

    // Create source table with decimal types:
    spark.sql(
      s"""CREATE TABLE `foo`(
         |  `id` decimal(10, 5),
         |  `col1` decimal(10, 5),
         |  `col2` decimal(10, 5),
         |  `col3` int,
         |  `data` string)
         |PARTITIONED BY (
         |  `dt` decimal(10, 5)
         |  )
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

    import org.apache.spark.sql.functions._
    spark.range(20).select(
      $"id" cast "decimal(10, 5)" as "id", // regular decimal as key
      $"id" cast "decimal(10, 5)" as "col1", // regular decimal as additional field
      lit(null.asInstanceOf[Int]) cast "decimal(10, 10)" as "col2", // null decimal
      $"id" as "col3",
      concat(lit("data_"), $"id") as "data"
    )
      .createTempView("v")
    spark.sql("insert overwrite table foo partition (dt=0.42) select * from v")
    spark.table("foo").show(false)
    spark.table("foo").printSchema()

    def create() = IndexManager.createNew(IndexSpec("foo", "index", Seq("id"), Seq("col1", "col2", "col3")))(spark)

    // without setting indexer.castDecimalToDouble=true, should fail:
    Assertions.assertThrows(classOf[IllegalArgumentException], new Executable {
      def execute = create()
    })

    spark.conf.set("indexer.castDecimalToDouble", true)
    val manager = create()

    // index table should have double types instead of decimal:
    Seq("id", "col1")
      .map(c => spark.table("index").schema.find(_.name == c).get)
      .map(_.dataType.typeName)
      .foreach(Assertions.assertEquals("double", _))

    // index data, load with index, and make check the results.
    // the results should be in the original source format (decimals)
    manager.appendNewPartitions(Seq(Seq("dt" -> "0.42")))
    val result = manager.loadByIndex(manager.getIndex())
    Seq("id", "col1")
      .map(c => result.schema.find(_.name == c).get)
      .map(_.dataType.typeName)
      .foreach(Assertions.assertEquals("decimal(10,5)", _))

    // convert to double, only because its easier to test...
    val result2 = result
      .withColumn("id", $"id" cast "double")
      .withColumn("col1", $"col1" cast "double")
      .collect()
    println("ids: " + result2.map(_.getAs[Double]("id")).mkString(", "))
    Assertions.assertEquals(20, result2.length)
    result2.foreach { row =>

      val index = row.getAs[Int]("col3")
      val id = row.getAs[Double]("id")
      val col1 = row.getAs[Double]("col1")
      val data = row.getAs("data").toString

      Assertions.assertEquals(index.doubleValue(), id)
      Assertions.assertEquals(index.doubleValue(), col1)
      Assertions.assertTrue(row.isNullAt(row.fieldIndex("col2")))
      Assertions.assertEquals("data_" + index, data)
    }
  }
}