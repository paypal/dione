package com.paypal.dione.spark.index.avro

import com.paypal.dione.SparkCleanTestDB
import com.paypal.dione.SparkTestBase.dfToStr
import com.paypal.dione.spark.index.{IndexManager, IndexSpec}
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api.{Assertions, _}


object TestAvroIndexManagerJoin extends SparkCleanTestDB {

  override val baseTestPath: String = "TestData/TestAvroIndexManagerJoin/"
  override val dbName: String = "TestAvroIndexManagerJoin"

  @BeforeAll
  def initData(): Unit = {
    import spark.implicits._


    spark.sql(s"create table t_join (key int, sub_key string, var1 string, var2 int, ts string) " +
      s"partitioned by (dt string) stored as avro")

    0.to(59, 2).map(i => (i, "sub_key_"+i, "var_a_" + i, i, "2018-10-04 12:34:%02d".format(i % 60)))
      .toDF("key", "sub_key", "var1", "var2", "ts").repartition(3).createOrReplaceTempView("t")
    spark.sql(s"insert overwrite table t_join partition (dt='2018-10-04') select * from t")

    30.to(90, 2).map(i => (i, "sub_key_"+i, "var_b_" + i, i, "2018-11-04 12:34:%02d".format(i % 60)))
      .toDF("key", "sub_key", "var1", "var2", "ts").repartition(2).createOrReplaceTempView("t2")
    spark.sql(s"insert overwrite table t_join partition (dt='2018-11-04') select * from t2")

    spark.table("t_join").show()
  }

}

@TestMethodOrder(classOf[OrderAnnotation])
class TestAvroIndexManagerJoin {

  import TestAvroIndexManager._

  @Test
  @Order(1)
  def testCreateIndexManager(): Unit = {
    IndexManager.createNew(IndexSpec("t_join", "t_join_index", Seq("key"), Seq("ts")))(spark)
  }

  @Test
  @Order(2)
  def testAppendNewPartitions(): Unit = {
    val indexManager = IndexManager.load("t_join_index")(spark)
    spark.conf.set("index.manager.btree.num.parts", "2")
    spark.conf.set("index.manager.btree.interval", "3")
    spark.conf.set("index.manager.btree.height", "2")
    indexManager.appendNewPartitions(Seq(Seq("dt" -> "2018-10-04")))

    spark.conf.set("index.manager.btree.num.parts", "3")
    spark.conf.set("index.manager.btree.interval", "4")
    spark.conf.set("index.manager.btree.height", "2")
    indexManager.appendNewPartitions(Seq(Seq("dt" -> "2018-11-04")))

    spark.conf.unset("index.manager.btree.num.parts")
    spark.conf.unset("index.manager.btree.interval")
    spark.conf.unset("index.manager.btree.height")

    //spark.table("t_join_index").show(100, false)
    Assertions.assertEquals(61, spark.table("t_join_index").count())
  }

  @Test
  @Order(3)
  def testJoinWithDS(): Unit = {
    val indexManager = IndexManager.load("t_join_index")(spark)
    val dsDF = spark.table("t_join_index")
      .where("key % 2 == 0")

    val joinedDF = indexManager.joinWithIndex(dsDF.select("key", "dt").repartition(2))

    Assertions.assertEquals(dfToStr(dsDF, Some(joinedDF.columns)), dfToStr(joinedDF))
  }

  @Test
  @Order(4)
  def testJoinWithNonExistentId(): Unit = {
    val indexManager = IndexManager.load("t_join_index")(spark)
    val joinedDF = indexManager.joinWithIndex(spark.sql("select 7 as key, '2018-10-04' as dt"))

    Assertions.assertEquals(List(), joinedDF.collect().toList.map(_.toString()))
  }

  @Test
  @Order(5)
  def testJoinWithNonExistentPartition(): Unit = {
    val indexManager = IndexManager.load("t_join_index")(spark)
    val joinedDF = indexManager.joinWithIndex(spark.sql("select 2 as key, '2018-10-05' as dt"))

    Assertions.assertEquals(List(), joinedDF.collect().toList.map(_.toString()))
  }

  @Test
  @Order(6)
  def testJoinWithNonExistentIdMix(): Unit = {
    val indexManager = IndexManager.load("t_join_index")(spark)

    val dsDF = spark.table("t_join_index").where("key % 3 == 1")

    val joinedDF = indexManager.joinWithIndex(dsDF.select("key", "dt")
      .union(spark.sql("select 9 as key, '2018-10-04' as dt")).repartition(1))

    Assertions.assertEquals(dfToStr(dsDF, Some(joinedDF.columns)), dfToStr(joinedDF))
  }

  @Test
  @Order(7)
  def testJoinWithNonExistentPartitionMix(): Unit = {
    val indexManager = IndexManager.load("t_join_index")(spark)

    val dsDF = spark.table("t_join_index").where("key % 3 == 1")

    val joinedDF = indexManager.joinWithIndex(dsDF.select("key", "dt")
      .union(spark.sql("select 2 as key, '2018-10-05' as dt")).repartition(1))

    Assertions.assertEquals(dfToStr(dsDF, Some(joinedDF.columns)), dfToStr(joinedDF))
  }
}
