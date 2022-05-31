package com.paypal.dione.spark.index.optimizations

import com.paypal.dione.SparkCleanTestDB
import com.paypal.dione.spark.Dione
import com.paypal.dione.spark.index.avro.TestAvroIndexManagerJoin.spark
import com.paypal.dione.spark.index.{IndexManager, IndexSpec}
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._


object TestIndexRule extends SparkCleanTestDB {

  override val baseTestPath: String = "TestData/TestIndexRule/"
  override val dbName: String = "TestIndexRule".toLowerCase

  @BeforeAll
  def initData(): Unit = {
    import spark.implicits._

    spark.sql(s"create table t_rule (key string, sub_key string, var1 string, var2 int) " +
      s"partitioned by (dt string) stored as avro")

    (0 until 10).map(i => (i, "sub_key_"+i, "var_a_" + i, i))
      .toDF("key", "sub_key", "var1", "var2").repartition(2).createOrReplaceTempView("t")
    spark.sql(s"insert overwrite table t_rule partition (dt='2021-10-04') select * from t")

    //spark.table("t_rule").show()
  }

}

@TestInstance(Lifecycle.PER_CLASS)
class TestIndexRule {

  val tableName = TestIndexRule.dbName + ".t_rule"
  val idxTableName = "t_rule_index"

  @BeforeAll
  def testCreateIndexManager(): Unit = {
    val indexSpec = IndexSpec.create(tableName, idxTableName, Seq("key"), Seq("sub_key"))(spark)
    IndexManager.createNew(indexSpec)(spark)

    val indexManager = IndexManager.load(idxTableName)(spark)
    spark.conf.set("index.manager.btree.num.parts", "2")
    spark.conf.set("index.manager.btree.interval", "3")
    spark.conf.set("index.manager.btree.height", "1")
    indexManager.appendMissingPartitions()

    Dione.enable(spark)
    Dione.getContext.addIndex(indexManager)
  }

  @Test
  def testIndexSize(): Unit = {
    Assertions.assertEquals(10, spark.table(idxTableName).count())
  }

  @Test
  def testCoveringProject(): Unit = {
    val df = spark.table(tableName).select("key", "sub_key")

    AssertPlanUsesTable(df, TestIndexRule.dbName + "." + idxTableName)

    Assertions.assertEquals(10, df.collect().length)
  }

  @Test
  def testCoveringProjectFilter(): Unit = {
    val df = spark.table(tableName).select("key", "sub_key").where("sub_key=='sub_key_4'")

    AssertPlanUsesTable(df, TestIndexRule.dbName + "." + idxTableName)

    Assertions.assertEquals("[4,sub_key_4]", df.collect().mkString(","))
  }

  @Test
  def testFilterEqualTo(): Unit = {
    val df = spark.table(tableName).select("key", "sub_key", "var2", "var1").where("key == '7'")

    AssertPlanUsesTable(df, TestIndexRule.dbName + "." + idxTableName)

    Assertions.assertEquals("[7,sub_key_7,7,var_a_7]", df.collect().mkString(","))
  }


  @Test
  def testFilterEqualToWithPartition(): Unit = {
    val df = spark.table(tableName).select("sub_key", "key", "dt", "var1").where("key == '7'")

        df.explain(true)
    //    df.show()
    AssertPlanUsesTable(df, TestIndexRule.dbName + "." + idxTableName)

    Assertions.assertEquals("[sub_key_7,7,2021-10-04,var_a_7]", df.collect().mkString(","))
  }

  @Test
  def testNoIndex(): Unit = {
    val df = spark.table("t_rule").select("key", "sub_key", "var2", "var1").where("var2=6")

    AssertPlanUsesTable(df, tableName)
  }

  private def AssertPlanUsesTable(df: Dataset[Row], tableName: String) = {
    Assertions.assertEquals(Seq(tableName),
      df.queryExecution.optimizedPlan.collect {
        case h: HiveTableRelation =>
          h.tableMeta.identifier.database.get + "." + h.tableMeta.identifier.identifier
      })
  }
}
