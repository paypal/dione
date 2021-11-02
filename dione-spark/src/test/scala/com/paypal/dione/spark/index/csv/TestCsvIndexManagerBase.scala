package com.paypal.dione.spark.index.csv

import com.paypal.dione.spark.index.{IndexSpec, TestIndexManagerBase}
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api._


@TestMethodOrder(classOf[OrderAnnotation])
class TestCsvIndexManagerBase extends TestIndexManagerBase {

  lazy val indexSpec: IndexSpec = IndexSpec("csv_data_tbl", "csv_data_tbl_idx", Seq("id_col"))

  def initDataTable(fieldsSchema: String, partitionFieldSchema: String): Unit = {
    spark.sql(s"create table ${indexSpec.dataTableName} ($fieldsSchema) partitioned by ($partitionFieldSchema)" +
      s" row format delimited" +
      s" fields terminated by '|'")
  }

  val testSamples = Seq(SampleTest("msg_100", Nil, "var_a_100", 607, 0, -1))
}