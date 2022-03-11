package com.paypal.dione.hdfs.index.orc

import com.paypal.dione.hdfs.index.{HdfsIndexer, HdfsIndexerMetadata}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.storage.ql.exec.vector.{BytesColumnVector, DoubleColumnVector, LongColumnVector}
import org.apache.orc.OrcFile
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._

case class OrcIndexer(file: Path, start: Long, end: Long, conf: Configuration, projectedFields: Option[Seq[String]])
  extends HdfsIndexer[Seq[(String, Any)]]() {

  def this(file: Path, conf: Configuration) =
    this(file, 0, 0, conf, None)

  if (start!=0)
    throw new RuntimeException("currently splits are not supported for ORC files")

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val reader = OrcFile.createReader(file, OrcFile.readerOptions(conf))
  private val recordReader = reader.rows()
  private val fileSchema = reader.getSchema
  private val batch = fileSchema.createRowBatch(1000)
  private val projectedFieldsList = if (projectedFields.nonEmpty) {
    val origFieldsMap = fileSchema.getFieldNames.zipWithIndex.toMap
    val projectedFieldsList = projectedFields.get.map(pf => pf -> origFieldsMap(pf)).toList
    batch.projectedColumns = projectedFieldsList.map(_._2).toArray
    batch.projectionSize = batch.projectedColumns.length
    projectedFieldsList
  } else {
    fileSchema.getFieldNames.zipWithIndex.toList
  }

  private var numInBatch = 0

  override def closeCurrentFile(): Unit = {
    recordReader.close()
    batch.reset()
  }

  /**
   * Regular seek. Called once per offset (block).
   */
  override def seek(offset: Long): Unit = {
    logger.debug("seeking to offset: {}", offset)
    if (batch.size > 0 &&
        offset >= recordReader.getRowNumber - batch.size &&
        offset < recordReader.getRowNumber) {
     numInBatch =  (offset - recordReader.getRowNumber + batch.size).toInt
     logger.debug("batch already loaded, changing numInBatch to {}", numInBatch)
    } else {
      logger.debug("using seekToRow")
      recordReader.seekToRow(offset)
    }
  }

  /**
   * Skip the next row - can avoid deserialization, etc.
   */
  override def skip(): Unit = {
    // ORC has row random access internally. no need for skip.
  }

  /**
   * Read the next row
   */
  override def readLine(): Seq[(String, Any)] = {
    if (numInBatch >= batch.size) {
      recordReader.nextBatch(batch)
      numInBatch = 0
    }

    val res = if (numInBatch<batch.size) {
      val a = (0 until batch.projectionSize).map{ icol =>
        val columnVector = batch.cols(projectedFieldsList(icol)._2)
        if (columnVector.isNull(numInBatch)) {
          projectedFieldsList.get(icol)._1 -> null
        } else {
          val fieldDesc = fileSchema.getChildren.get(icol)
          // based on https://www.javahelps.com/2020/08/read-and-write-orc-files-in-core-java.html
          val v = fieldDesc.getCategory.getName match {
            case "tinyint" => columnVector.asInstanceOf[LongColumnVector].vector(numInBatch).toByte
            case "smallint" => columnVector.asInstanceOf[LongColumnVector].vector(numInBatch).toShort
            case "int" | "date" => columnVector.asInstanceOf[LongColumnVector].vector(numInBatch).toInt
            case "bigint" => columnVector.asInstanceOf[LongColumnVector].vector(numInBatch)
            case "boolean" => columnVector.asInstanceOf[LongColumnVector].vector(numInBatch) == 1L
            case "float" => columnVector.asInstanceOf[DoubleColumnVector].vector(numInBatch).toFloat
            case "double" => columnVector.asInstanceOf[DoubleColumnVector].vector(numInBatch)
            case "string" | "varchar" => columnVector.asInstanceOf[BytesColumnVector].toString(numInBatch)
            case "char" => columnVector.asInstanceOf[BytesColumnVector].toString(numInBatch).charAt(0)
            case s: String => throw new RuntimeException("Unsupported type " + s)
          }
          projectedFieldsList.get(icol)._1 -> v
        }
      }
      a
    } else null

    numInBatch+=1
    res
  }

  override def getCurMetadata(): HdfsIndexerMetadata = {
    HdfsIndexerMetadata(file.toString, recordReader.getRowNumber-batch.size+numInBatch-1, 0)
  }
}
