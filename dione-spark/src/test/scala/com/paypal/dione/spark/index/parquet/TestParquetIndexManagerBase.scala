package com.paypal.dione.spark.index.parquet

import com.paypal.dione.spark.index.{IndexSpec, TestIndexManagerBase}
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api._


@TestMethodOrder(classOf[OrderAnnotation])
class TestParquetIndexManagerBase extends TestIndexManagerBase {

  lazy val indexSpec: IndexSpec = IndexSpec.create("parquet_data_tbl", "parquet_data_tbl_idx", Seq("id_col"), Seq("meta_field"))

  def initDataTable(fieldsSchema: String, partitionFieldSchema: String): Unit = {
    val sc = spark.sparkContext
    sc.hadoopConfiguration.setInt("parquet.block.size", 100)

    spark.sql(s"create table ${indexSpec.dataTableName} ($fieldsSchema) partitioned by ($partitionFieldSchema) stored as parquet")

  }

  val testSamples = Seq(SampleTest("msg_100", Seq("meta_100"), "var_a_100", 0, 22, -1))
}