package com.paypal.dione.spark.index.sequence

import com.paypal.dione.hdfs.index.HdfsIndexer
import com.paypal.dione.hdfs.index.sequence.SequenceFileIndexer
import com.paypal.dione.spark.index._
import com.paypal.dione.spark.index.sequence.SeqFileUtils.{SerDeInfo, createDeserializer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.{SerializableConfiguration, SparkSqlHiveUtils}
import org.apache.spark.sql.types.StructType

object SeqFileSparkIndexer extends IndexManagerFactory {
  override def canResolve(inputFormat: String, serde: String): Boolean =
    inputFormat.contains("SequenceFileInputFormat") && serde.contains("lazy.LazySimpleSerDe")

  override def createSparkIndexer(spark: SparkSession, indexSpec: IndexSpec): SparkIndexer = {
    SeqFileSparkIndexer(spark, indexSpec.dataTableName)
  }
}

case class SeqFileSparkIndexer(@transient spark: SparkSession, dataTableName: String)
  extends SparkIndexer {

  override type T = Seq[(String, Any)]

  def createSerdeInfo(hiveTable: Table): SerDeInfo = {
    // TODO - might have to use the partition serde (underlying code is ready):
    SerDeInfo(hiveTable.getDeserializerClass, hiveTable.getDeserializerClass, hiveTable.getMetadata, hiveTable.getMetadata)
  }

  private val sparkCatalogTable = IndexManagerUtils.getSparkCatalogTable(spark, dataTableName)
  private val hiveTable: Table = SparkSqlHiveUtils.toHiveTable(sparkCatalogTable)
  private val schemaWithoutPartitionCols = spark.table(dataTableName).drop(sparkCatalogTable.partitionColumnNames: _*).schema

  val serdeInfo = createSerdeInfo(hiveTable)
  val serConf = SerializableConfiguration.broadcast(spark)

  def initHdfsIndexer(file: Path, conf: Configuration, start: Long, end: Long, fieldsSchema: StructType): HdfsIndexer[Seq[(String, Any)]] = {

    val outputFields: Seq[String] = fieldsSchema.map(_.name)
    val deserializer = createDeserializer(serdeInfo, schemaWithoutPartitionCols, outputFields, serConf.value.value)
    SequenceFileIndexer(file, start, end, deserializer, serConf.value.value)
  }

  def convert(t: Seq[(String, Any)]): Seq[Any] = t.map(_._2)

  def convertMap(t: Seq[(String, Any)]): Map[String, Any] = t.toMap

}
