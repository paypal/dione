package com.paypal.dione.spark.index.avro

import com.paypal.dione.avro.utils.GenericRecordMap
import com.paypal.dione.hdfs.index.HdfsIndexer
import com.paypal.dione.hdfs.index.avro.AvroIndexer
import com.paypal.dione.spark.index.{IndexManagerFactory, IndexSpec, SparkIndexer}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.AvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

object AvroSparkIndexer extends IndexManagerFactory {

  override def canResolve(inputFormat: String, serde: String): Boolean =
    inputFormat.contains("Avro") && serde.contains("AvroSerDe")

  override def createSparkIndexer(spark: SparkSession, indexSpec: IndexSpec): SparkIndexer =
    AvroSparkIndexer(spark)
}

case class AvroSparkIndexer(@transient spark: SparkSession) extends SparkIndexer {
  override type T = GenericRecord

  private var avroHdfsIndexer: AvroIndexer = _
  private var converter: AvroDeserializer = _
  private var fieldsSchema: StructType = _

  def initHdfsIndexer(file: Path, conf: Configuration, start: Long, end: Long, fieldsSchema: StructType): HdfsIndexer[GenericRecord] = {
    avroHdfsIndexer = AvroIndexer(file, start, end, conf)
    this.fieldsSchema = fieldsSchema
    converter = new AvroDeserializer(avroHdfsIndexer.getSchema(), fieldsSchema)
    avroHdfsIndexer
  }

  def convert(gr: GenericRecord): Seq[Any] = {
    converter.deserialize(gr).asInstanceOf[InternalRow].toSeq(fieldsSchema).map {
      case f: UTF8String => f.toString
      case f => f
    }
  }

  def convertMap(gr: GenericRecord): Map[String, Any] =
    GenericRecordMap(gr)

}
