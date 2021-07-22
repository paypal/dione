package com.paypal.dione.hdfs.index.sequence

import com.paypal.dione.hdfs.index.sequence.SequenceFileIndexer.Deserializer
import com.paypal.dione.hdfs.index.{HdfsIndexer, HdfsIndexerMetadata}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{DataOutputBuffer, SequenceFile, Writable}
import org.slf4j.LoggerFactory

case class SequenceFileIndexer(file: Path, start: Long, end: Long, deserializer: Deserializer, conf: Configuration)
  extends HdfsIndexer[Seq[(String, Any)]]() {
  private val logger = LoggerFactory.getLogger(this.getClass)

  logger.info(s"reading $file in range [$start, $end]")

  lazy private val buffer = new DataOutputBuffer(10 << 10)
  private val reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file))
  reader.sync(start)
  // TODO - really need to initialize k/v on every file?
  private val key = reader.getKeyClass.newInstance().asInstanceOf[Writable]
  private val value = reader.getValueClass.newInstance().asInstanceOf[Writable]
  private var outputPosition = reader.getPosition
  private var inputPosition = -1L
  private var curMetadata: HdfsIndexerMetadata = _
  private var atBlockStart = true

  override def closeCurrentFile(): Unit = reader.close()

  override def seek(offset: Long): Unit = {
    reader.seek(offset)
    outputPosition = offset
  }

  override def skip(): Unit = reader.next(key)


  /**
   * Read the next row
   */
  override def readLine(): Seq[(String, Any)] = {

    val (hasData, position): (Boolean, Long) =
      if (reader.isBlockCompressed) {
        // when the block is compressed, the internal reader unrolls the entire block and moves the position
        // to the next block. the uncompressed rows are buffered and served from memory, but the position does
        // NOT reflect the block of the next() row. so we need to keep the previous block position
        // and check if we still in the same (prev) block, or moved to new block.
        val hasData = reader.next(key, value) // must call next() before checking position
        val readerPosition = reader.getPosition
        val outOfSync = reader.syncSeen()
        if (readerPosition == outputPosition || atBlockStart || !outOfSync) {
          // still in the same block
          atBlockStart = false
          inputPosition = readerPosition
        }
        else {
          // moved to new block
          atBlockStart = true
          outputPosition = inputPosition
        }
        (hasData, outputPosition)
      } else {
        // no compression - take position and only then call next()
        val pos = reader.getPosition
        (reader.next(key, value), pos)
      }

    val outOfRange = position >= end && reader.syncSeen()
    if (!hasData || outOfRange)
      return null

    val nextNumInBlock = if (curMetadata == null || position != curMetadata.position) 0 else curMetadata.numInBlock + 1
    curMetadata = HdfsIndexerMetadata(file.toString, position, nextNumInBlock)


    deserializer.deserialize(key, value)
  }

  override def getCurMetadata(): HdfsIndexerMetadata = {
    val size = getRowSize(key, value)
    curMetadata.copy(size = size)
  }

  private def getRowSize(k: Writable, v: Writable) = {
    buffer.reset()
    k.write(buffer)
    v.write(buffer)
    val size = buffer.getLength
    size
  }
}

object SequenceFileIndexer {

  trait Deserializer {
    def deserialize(k: Writable, v: Writable): Seq[(String, Any)]
  }

}