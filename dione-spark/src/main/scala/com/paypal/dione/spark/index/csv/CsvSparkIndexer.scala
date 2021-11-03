package com.paypal.dione.spark.index.csv

import com.paypal.dione.hdfs.index.HdfsIndexer
import com.paypal.dione.hdfs.index.csv.CsvIndexer
import com.paypal.dione.spark.index._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.serde2.`lazy`.{LazySerDeParameters, LazyUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object CsvSparkIndexer extends IndexManagerFactory {

  override def canResolve(inputFormat: String, serde: String): Boolean =
    inputFormat.contains("org.apache.hadoop.mapred.TextInputFormat")

  override def createSparkIndexer(spark: SparkSession, indexSpec: IndexSpec): SparkIndexer =
    CsvSparkIndexer(spark, indexSpec.dataTableName)
}

case class CsvSparkIndexer(@transient spark: SparkSession, dataTableName: String)
  extends SparkIndexer {

  override type T = Seq[String]

  private val sparkCatalogTable = IndexManagerUtils.getSparkCatalogTable(spark, dataTableName)
  private val schemaWithoutPartitionCols = spark.table(dataTableName).drop(sparkCatalogTable.partitionColumnNames: _*).schema
  private val outputFields: Seq[String] = schemaWithoutPartitionCols.map(_.name)
  private var requestedFieldsSchema: Seq[(Int, StructField)] = _

  def initHdfsIndexer(file: Path, conf: Configuration, start: Long, end: Long, fieldsSchema: StructType): HdfsIndexer[Seq[String]] = {
    if (requestedFieldsSchema == null) {
      val fieldsMap = schemaWithoutPartitionCols.map(_.name).zipWithIndex.toMap
      requestedFieldsSchema = fieldsSchema.map(f => fieldsMap(f.name) -> f)
    }

    // logic from org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters.collectSeparators()
    val tableProperties = sparkCatalogTable.storage.properties
    val delimiter = LazyUtils.getByte(tableProperties.getOrElse("field.delim",
      tableProperties.getOrElse("serialization.format", null)), LazySerDeParameters.DefaultSeparators(0))
    CsvIndexer(file, start, end, conf, delimiter.toChar)
  }

  def convert(t: Seq[String]): Seq[Any] = {
    val l = t.toList
    requestedFieldsSchema.map(rf => {
      rf._2.dataType match {
        case _: StringType => l(rf._1)
        case _: LongType => l(rf._1).toLong
        case _: IntegerType => l(rf._1).toInt
        case other => throw new RuntimeException("type " + other + " is currently not support for CsvSparkIndexer")
      }
    })
  }

  def convertMap(t: Seq[String]): Map[String, Any] = outputFields.zip(t).toMap

}
