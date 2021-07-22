package com.paypal.dione.spark.avro.btree

import java.io.{ByteArrayInputStream, DataInputStream}
import java.util.Base64

import org.apache.avro.file.CodecFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType

import scala.util.Try

/**
 * Job level configurations
 */
class AvroBtreeJobOptions(val schema: StructType,
                          val map: Map[String, String]
                         ) extends Serializable {
  validate()

  def hadoopConf = {
    val conf = new Configuration()
    conf.readFields(new DataInputStream(new ByteArrayInputStream(Base64.getDecoder.decode(map("__hadoop.conf")))))
    conf
  }

  def basePath: Path = new Path(map("path"))
//  def stagingPath: Path = basePath.child(".staging")
  def keyRecordName: String = map.getOrElse("avro.keyRecordName", "key")
  def valueRecordName: String = map.getOrElse("avro.valueRecordName", "value")
  def recordNamespace: String = map.getOrElse("avro.recordNamespace", "indexer")
  def compression: String = map.getOrElse("avro.compression", "deflate")

  def keyFields: Seq[String] = map("key.fields").split(",").map(_.trim)
  def valueFields: Seq[String] = map("value.fields").split(",").map(_.trim)

  def interval: Int = map.getOrElse("btree.interval", "1000").toInt
  def height: Int = map.getOrElse("btree.height", "4").toInt

  def cleanup = map.get("cleanup").forall(_.toBoolean)


  private def validate() = {
    // required options:
//    Seq("key.fields", "value.fields", "__hadoop.conf").foreach { key =>
    Seq("key.fields", "value.fields").foreach { key =>
      map.getOrElse(key, throw new IllegalArgumentException("missing option: " + key))
    }

    // validate codec:
    try {
      CodecFactory.fromString(compression)
    } catch {
      case e: Throwable =>
        throw new IllegalArgumentException("invalid avro compression: " + compression, e)
    }

    // ensure specified key/value fields really exist:
    val schemaFields = schema.map(_.name).toSet
    val invalids = (keyFields ++ valueFields).filterNot(schemaFields.contains)
    if (invalids.nonEmpty)
      throw new IllegalArgumentException(
        "The following key or value field(s) could not be found in the schema: " + invalids.mkString("[", ",", "]"))

    Try(cleanup).getOrElse(throw new IllegalArgumentException("cleanup must be boolean"))
  }
}

