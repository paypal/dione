package com.paypal.dione.hdfs.index.parquet

import com.paypal.dione.hdfs.index.{HdfsIndexer, HdfsIndexerMetadata}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.hadoop.ParquetFileReader
import org.slf4j.LoggerFactory

case class ParquetIndexer(file: Path, start: Long, end: Long, conf: Configuration, projectedSchema: Option[Schema])
  extends HdfsIndexer[GenericRecord]() {

  def this(file: Path, conf: Configuration) =
    this(file, 0, 0, conf, None)

  if (start!=0)
    throw new RuntimeException("currently splits are not supported for Parquet files")

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val fileName = file.toString
  private val reader = ParquetFileReader.open(conf, file)
  private val fileReader = new MyInternalParquetRecordReader[GenericRecord](new AvroReadSupport[GenericRecord])
  if (projectedSchema.nonEmpty) {
    AvroReadSupport.setRequestedProjection(conf, projectedSchema.get)
  } else {
    conf.unset(AvroReadSupport.AVRO_REQUESTED_PROJECTION)
  }
  fileReader.initialize(reader, conf)

  override def closeCurrentFile(): Unit = {
    fileReader.close()
  }

  /**
   * Regular seek. Called once per offset (block).
   */
  override def seek(offset: Long): Unit = {
    if (fileReader.getCurrentBlock + 1 == offset.toInt) {
      logger.debug("skipping to end of block {}", offset)
      fileReader.skipToEndOfBlock()
    } else if (fileReader.getCurrentBlock + 1 < offset.toInt) {
      logger.debug("skipping from block {} to block {}", fileReader.getCurrentBlock, offset)
      (fileReader.getCurrentBlock + 1 until offset.toInt).foreach(_ => fileReader.skipRowGroup())
    }
  }

  /**
   * Skip the next row - can avoid deserialization, etc.
   */
  override def skip(): Unit = {
    fileReader.nextKeyValue()
  }

  /**
   * Read the next row
   */
  override def readLine(): GenericRecord = {

    val hasNext = fileReader.nextKeyValue()
    if (hasNext)
      fileReader.getCurrentValue
    else null
  }

  override def getCurMetadata(): HdfsIndexerMetadata = {
    HdfsIndexerMetadata(fileName, fileReader.getCurrentBlock, fileReader.getCurrentInBlock)
  }
}
