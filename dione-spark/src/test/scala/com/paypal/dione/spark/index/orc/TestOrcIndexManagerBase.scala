package com.paypal.dione.spark.index.orc

import com.paypal.dione.spark.index.{IndexSpec, TestIndexManagerBase}
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api._


@TestMethodOrder(classOf[OrderAnnotation])
class TestOrcIndexManagerBase extends TestIndexManagerBase {

  lazy val indexSpec: IndexSpec = IndexSpec("orc_data_tbl", "orc_data_tbl_idx", Seq("id_col"))

  def initDataTable(fieldsSchema: String, partitionFieldName: String): Unit = {
    spark.sql(s"create table ${indexSpec.dataTableName} ($fieldsSchema) partitioned by ($partitionFieldName string) stored as orc")
  }

  val testSamples = Seq(SampleTest("msg_100", Nil, "var_a_100", 22, 0, -1))

  override val samplePartition: String = "'2021-02-03'"
}