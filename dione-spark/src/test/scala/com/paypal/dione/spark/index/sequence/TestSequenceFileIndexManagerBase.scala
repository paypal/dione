package com.paypal.dione.spark.index.sequence

import com.paypal.dione.spark.index.{IndexSpec, TestIndexManagerBase}
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api._


@TestMethodOrder(classOf[OrderAnnotation])
class TestSequenceFileIndexManagerBase extends TestIndexManagerBase {

  lazy val indexSpec: IndexSpec = IndexSpec("seq_file_data_tbl", "seq_file_data_tbl_idx", Seq("id_col"), Seq("meta_field"))

  def initDataTable(fieldsSchema: String, partitionFieldName: String): Unit = {
    spark.sql(s"create table ${indexSpec.dataTableName} ($fieldsSchema)" +
      s" partitioned by ($partitionFieldName string)" +
      s""" ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\u0010'
        STORED AS INPUTFORMAT
        'org.apache.hadoop.mapred.SequenceFileInputFormat'
        OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'
        """)
  }

  val testSamples = Seq(SampleTest("msg_100", Seq("meta_100"), "var_a_100", 985, 0, 35))

  override val samplePartition: String = "'2021-02-03'"
}