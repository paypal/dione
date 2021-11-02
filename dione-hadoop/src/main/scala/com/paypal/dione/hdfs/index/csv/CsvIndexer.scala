package com.paypal.dione.hdfs.index.csv

import com.paypal.dione.hdfs.index.{HdfsIndexer, HdfsIndexerMetadata}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, LineRecordReader}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.slf4j.LoggerFactory

/**
 * based on org.apache.spark.sql.execution.datasources.HadoopFileLinesReader
 */

case class CsvIndexer(file: Path, start: Long, end: Long, conf: Configuration,
                      delimiter: Char)
  extends HdfsIndexer[Seq[String]]() {

  if (start!=0)
    throw new RuntimeException("currently splits are not supported for ORC files")

  private val logger = LoggerFactory.getLogger(this.getClass)

  var reader: LineRecordReader = _
  private var curPosition = -1L
  private var lastPosition = -1L

  override def closeCurrentFile(): Unit = {
    reader.close()
  }

  /**
   * Regular seek. Called once per offset (block).
   */
  override def seek(offset: Long): Unit = {
    // @TODO - need to better optimize. not to re-init on every seek
    initReader(offset, end)
  }

  /**
   * Skip the next row - can avoid deserialization, etc.
   */
  override def skip(): Unit = {
    if (reader==null) {
      initReader(start, end)
    }
    reader.nextKeyValue()
  }

  /**
   * Read the next row
   */
  override def readLine(): Seq[String] = {
    if (reader==null) {
      initReader(start, end)
    }

    lastPosition = curPosition
    curPosition = Option(reader.getCurrentKey).map(_.get()).getOrElse(0)

    val hasNext = reader.nextKeyValue()
    if (hasNext) {
      reader.getCurrentValue.toString.split(delimiter).toSeq
    } else null
  }

  override def getCurMetadata(): HdfsIndexerMetadata = {
    // some quirk of the first line read
    val plusOne = if (lastPosition == curPosition) 1 else 0
    HdfsIndexerMetadata(file.toString, curPosition, plusOne)
  }

  val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
  val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)

  private def initReader(start: Long, end: Long) = {
    reader = new LineRecordReader()
    reader.initialize(new FileSplit(file, start, end - start, Array.empty), hadoopAttemptContext)
  }
}
