package com.paypal.dione.spark.metrics

import org.apache.spark.dione.{Metrics, ReaderMetrics}
import org.slf4j.LoggerFactory

/**
 * Reports reader-data stats from inside executors. By default prints only file/partition level stats.
 * Set kvstorage.reader.reporter=none to disable entirely.
 */
class StatsReporter extends Serializable {
  def initPartitionMetrics(): Unit = {}

  def closeFile(file: String): Unit = {}

  def openFile(file: String): Unit = {}

  def partitionDone(): Unit = {}

  def startRecordRead(): Unit = {}

  def endRecordRead(size: Long): Unit = {}
}

object StatsReporter {

  private def now = System.currentTimeMillis()

  class SoftReporter extends StatsReporter {

    private val logger = LoggerFactory.getLogger(getClass)
    private var threadId: String = null
    private val files: scala.collection.mutable.Set[String] = scala.collection.mutable.Set.empty[String]
    private var metrics: ReaderMetrics = null
    var
    // partition scope:
    partitionStartTs,
    partitionRecords,
    partitionSize,
    dataFileTime,
    numFiles,

    // file scope:
    fileOpenTs,
    fileReadMs,
    fileRecords,
    fileSize,

    // record scope:
    recordReadStart: Long = 0

    override def initPartitionMetrics(): Unit = {
      partitionStartTs = now
      partitionRecords = 0
      partitionSize = 0
      dataFileTime = 0
      numFiles = 0
      threadId = Seq("[", Thread.currentThread().getId.toString, "]").mkString
      initFileMetrics()
      metrics = Metrics.getReaderMetricsForTask
      files.clear()
    }

    private def initFileMetrics() = {
      fileOpenTs = 0
      fileReadMs = 0
      fileRecords = 0
      fileSize = 0
      recordReadStart = 0
    }

    override def partitionDone(): Unit = {
      val totalPartitionTime = now - partitionStartTs
      report(
        s"Partition summary - " +
          s" user time: $totalPartitionTime," +
          s" data file time: $dataFileTime," +
          s" records: $partitionRecords," +
          s" bytes read: $partitionSize," +
          s" files: $numFiles"
      )
    }

    override def openFile(file: String): Unit = {
      if (files.contains(file))
        throw new RuntimeException("opened twice " + file)
      files += file
      initFileMetrics()
      fileOpenTs = now
      numFiles += 1
      report("Reading file: " + file)
    }

    override def closeFile(file: String): Unit = {
      val n = now
      val totalFileTime = n - fileOpenTs
      val totalPartitionTime = n - partitionStartTs
      partitionRecords += fileRecords
      partitionSize += fileSize
      dataFileTime += fileReadMs

      metrics.incRecords(fileRecords)
      metrics.incBytes(fileSize)

      report(
        s"Closing file $file, " +
          s"records read from file: $fileRecords, " +
          s"data file net latency ms: $fileReadMs, " +
          s"spark gross latency ms: $totalFileTime, " +
          s"bytes read: $fileSize, " +
          s"records read from partition so far: $partitionRecords, " +
          s"files read from partition so far: $numFiles, " +
          s"partition time ms: $totalPartitionTime")
    }

    override def startRecordRead() = recordReadStart = now

    override def endRecordRead(size: Long) = {
      fileReadMs += now - recordReadStart
      fileRecords += 1
      fileSize += size
    }

    private def report(msg: String) = {
      logger.info(threadId + " " + msg)
    }

  }

}