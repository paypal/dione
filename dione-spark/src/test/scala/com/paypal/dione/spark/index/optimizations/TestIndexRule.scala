package com.paypal.dione.spark.index.optimizations

import com.paypal.dione.SparkCleanTestDB
import com.paypal.dione.spark.Dione
import com.paypal.dione.spark.index.{IndexManager, IndexSpec}
import com.paypal.dione.spark.index.avro.TestAvroIndexManagerJoin.spark
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api._


object TestIndexRule extends SparkCleanTestDB {

  override val baseTestPath: String = "TestData/TestIndexRule/"
  override val dbName: String = "TestIndexRule"

  @BeforeAll
  def initData(): Unit = {
    import spark.implicits._


    spark.sql(s"create table t_rule (key int, sub_key string, var1 string, var2 int) " +
      s"partitioned by (dt string) stored as avro")

    (0 until 10).map(i => (i, "sub_key_"+i, "var_a_" + i, i))
      .toDF("key", "sub_key", "var1", "var2").repartition(2).createOrReplaceTempView("t")
    spark.sql(s"insert overwrite table t_rule partition (dt='2021-10-04') select * from t")

    spark.table("t_rule").show()
  }

}

@TestMethodOrder(classOf[OrderAnnotation])
class TestIndexRule {

  val indexSpec = IndexSpec("t_rule", "t_rule_index", Seq("key"), Seq("sub_key"))

  @Test
  @Order(1)
  def testCreateIndexManager(): Unit = {
    IndexManager.createNew(indexSpec)(spark)
  }

  @Test
  @Order(2)
  def testAppendNewPartitions(): Unit = {
    val indexManager = IndexManager.load("t_rule_index")(spark)
    spark.conf.set("index.manager.btree.num.parts", "2")
    spark.conf.set("index.manager.btree.interval", "3")
    spark.conf.set("index.manager.btree.height", "1")
    indexManager.appendMissingPartitions()

    Assertions.assertEquals(10, spark.table("t_rule_index").count())
  }

  @Test
  @Order(3)
  def testCoveringProject(): Unit = {
    Dione.enable(spark)
    Dione.getContext.addIndex(indexSpec)
    val dsDF = spark.table("t_rule").select("key", "sub_key")

    Assertions.assertEquals(dsDF.queryExecution.optimizedPlan.collect {
      case h: HiveTableRelation =>
        h.tableMeta.identifier.identifier
    }, Seq("t_rule_index"))
  }

  @Test
  @Order(3)
  def testFilter(): Unit = {
    Dione.enable(spark)
    Dione.getContext.addIndex(indexSpec)
    val dsDF = spark.table("t_rule").select("key", "sub_key").where("key == 7")

    dsDF.explain(true)
    dsDF.show()
  }
}
