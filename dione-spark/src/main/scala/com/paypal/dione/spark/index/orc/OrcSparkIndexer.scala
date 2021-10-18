package com.paypal.dione.spark.index.orc

import com.paypal.dione.hdfs.index.HdfsIndexer
import com.paypal.dione.hdfs.index.orc.OrcIndexer
import com.paypal.dione.spark.index.{IndexManagerFactory, IndexSpec, SparkIndexer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object OrcSparkIndexer extends IndexManagerFactory {

  override def canResolve(inputFormat: String, serde: String): Boolean =
    inputFormat.contains("OrcInputFormat") && serde.contains("OrcSerde")

  override def createSparkIndexer(spark: SparkSession, indexSpec: IndexSpec): SparkIndexer =
    OrcSparkIndexer(spark)
}

case class OrcSparkIndexer(@transient spark: SparkSession) extends SparkIndexer {

  override type T = Seq[(String, Any)]
  private var fieldsSchema: StructType = _
  private var fieldsSet: Set[String] = _

  def initHdfsIndexer(file: Path, conf: Configuration, start: Long, end: Long,
                      fieldsSchema: StructType): HdfsIndexer[Seq[(String, Any)]] = {
    if (start!=0)
      throw new RuntimeException("currently splits are not supported for ORC files," +
        "please set `indexer.files.chunk.split=false`")

    this.fieldsSchema = fieldsSchema
    this.fieldsSet = fieldsSchema.fieldNames.toSet
    OrcIndexer(file, start, end, conf, Some(fieldsSchema.fieldNames))
  }

  def convert(t: Seq[(String, Any)]): Seq[Any] = t.map(_._2)

  def convertMap(t: Seq[(String, Any)]): Map[String, Any] = t.toMap

}
