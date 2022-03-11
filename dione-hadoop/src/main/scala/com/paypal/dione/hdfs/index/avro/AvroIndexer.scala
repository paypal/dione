package com.paypal.dione.hdfs.index.avro

import com.paypal.dione.hdfs.index.{HdfsIndexer, HdfsIndexerMetadata}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.mapred.FsInput
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

case class AvroIndexer(file: Path, start: Long, end: Long, conf: Configuration) extends HdfsIndexer[GenericRecord]() {

  private val fileReader = new DataFileReader(new FsInput(file, conf), new GenericDatumReader[GenericRecord])
  private var reusedRecord: GenericRecord = _

  override def closeCurrentFile(): Unit = {
    if (fileReader != null)
      fileReader.close()
  }

  /**
   * Regular seek. Called once per offset (block).
   */
  override def seek(offset: Long): Unit = fileReader.seek(offset)

  /**
   * Skip the next row - can avoid deserialization, etc.
   */
  override def skip(): Unit = readNext()

  private def readNext(): Unit = {
    reusedRecord =
      if (reusedRecord == null)
        fileReader.next(reusedRecord)
      else
        fileReader.next()
  }

  fileReader.sync(start)
  private var prevSync = -1L
  private var numInBlock = -1
  private var lastPosition = 0L
  private var totalInBlock = -1L

  /**
   * Read the next row
   */
  override def readLine(): GenericRecord = {

    if (fileReader.pastSync(end) || !fileReader.hasNext)
      return null

    if (prevSync != fileReader.previousSync()) {
      prevSync = fileReader.previousSync()
      totalInBlock = fileReader.getBlockCount
      numInBlock = -1
    }

    readNext()
    numInBlock += 1

    reusedRecord
  }

  override def getCurMetadata(): HdfsIndexerMetadata = {
    val size = tryInferSize(fileReader, totalInBlock.intValue())
    lastPosition = fileReader.previousSync()
    HdfsIndexerMetadata(file.toString, prevSync, numInBlock, size)
  }

  def getSchema(): Schema = fileReader.getSchema

  private def tryInferSize(reader: DataFileReader[GenericRecord], totalInBlock: Int): Int = {
    val longSize = reader.getBlockSize /  totalInBlock
    longSize.intValue()
  }
}