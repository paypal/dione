package org.apache.spark.dione

import org.apache.spark.TaskContext

trait ReaderMetrics {
  def incRecords(r: Long)

  def incBytes(b: Long)
}

trait WriterMetrics {
  def incRecords(r: Long)

  def incBytes(b: Long)
}

/**
 * Exposes Spark private metrics
 */
object Metrics {
  def getReaderMetricsForTask: ReaderMetrics = {
    val metrics = TaskContext.get().taskMetrics().inputMetrics
    new ReaderMetrics {
      override def incRecords(r: Long): Unit = metrics.incRecordsRead(r)

      override def incBytes(b: Long): Unit = metrics.incBytesRead(b)
    }
  }

  def getWriterMetricsForTask: WriterMetrics = {
    val metrics = TaskContext.get().taskMetrics().outputMetrics
    new WriterMetrics {
      var records, bytes = 0L

      override def incRecords(r: Long): Unit = {
        records += r
        metrics.setRecordsWritten(records)
      }

      override def incBytes(b: Long): Unit = {
        bytes += b
        metrics.setBytesWritten(bytes)
      }
    }
  }
}


