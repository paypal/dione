package com.paypal.dione.hdfs.index

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import scala.collection.Iterator

/**
 * Top level class in the Indexer Hierarchy.
 * This defines the basic functionality users need to implement to index files of specific type.
 *
 * closeCurrentFile - close the opened file
 *
 * createIndex - given a specific Hadoop file of a certain type return an iterator with the data T and metadata M, which
 *               typically describes the location of T (like filename, row number, etc.)
 *
 * seek - seek to block
 * skip - skip next "row"
 * next - retrieve the data of next "row"
 *
 * fetch - given metadata, load T (quickly).
 *
 */
abstract class HdfsIndexer[T>:Null]() extends Iterator[T]{

  private val logger = LoggerFactory.getLogger(this.getClass)

  val file: Path
  val conf: Configuration
  val start: Long
  val end: Long

  def closeCurrentFile(): Unit

  /**
   * Regular seek. Called once per offset (block).
   */
  def seek(offset: Long): Unit

  /**
   * Skip the next row - can avoid deserialization, etc.
   */
  def skip(): Unit

  def readLine(): T

  var nextLine: T = _
  var numRead: Int = -1

  def hasNext(): Boolean = {
    if (nextLine == null) {
      nextLine = readLine()
      if (numRead % 10000 == 1) {
        logger.debug("Current metadata: " + getCurMetadata())
      }
    }

    nextLine != null
  }

  /**
   * Read the next row
   */
  final def next(): T = {
    if (!hasNext()) {
      throw new RuntimeException("Empty iterator!")
    }
    val _next = nextLine
    nextLine = null
    _next
  }

  def iteratorWithMetadata: Iterator[(T, HdfsIndexerMetadata)] = this.map(t => (t, getCurMetadata()))

  def getCurMetadata(): HdfsIndexerMetadata

  def fetch(hdfsIndexerMetadata: HdfsIndexerMetadata): T = {
    fetch(hdfsIndexerMetadata.position, hdfsIndexerMetadata.numInBlock)
  }

  def fetch(block: Long, numInBlock: Int): T = {
    logger.debug("fetching block " + block + ", numInBlock " + numInBlock)
    seek(block)
    (0 until numInBlock).foreach(_ => skip())

    val t: T = next()
    //closeCurrentFile()

    t
  }

}
