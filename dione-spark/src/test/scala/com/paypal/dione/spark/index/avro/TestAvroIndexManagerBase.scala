package com.paypal.dione.spark.index.avro

import com.paypal.dione.spark.index.{IndexSpec, TestIndexManagerBase}
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api._


@TestMethodOrder(classOf[OrderAnnotation])
class TestAvroIndexManagerBase extends TestIndexManagerBase {

  lazy val indexSpec: IndexSpec = IndexSpec("avro_data_tbl", "avro_data_tbl_idx", Seq("id_col"))

  def initDataTable(fieldsSchema: String, partitionFieldName: String): Unit = {
    spark.sql(s"create table ${indexSpec.dataTableName} ($fieldsSchema) partitioned by ($partitionFieldName long) stored as avro")
  }

  val testSamples = Seq(SampleTest("msg_100", Nil, "var_a_100", 349, 22, 31))

  override val samplePartition: String = "20210203"
}