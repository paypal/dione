package com.paypal.dione.hdfs.index

import com.paypal.dione.hdfs.index.HdfsIndexContants.{FILE_NAME_COLUMN, OFFSET_COLUMN, SIZE_COLUMN, SUB_OFFSET_COLUMN}
import org.apache.avro.generic.GenericRecord

object HdfsIndexerMetadata {
  // for seek/fetch operations, when size does not matter
  def apply(file: String, position: Long, numInBlock: Int): HdfsIndexerMetadata =
    HdfsIndexerMetadata(file, position, numInBlock, -1)

  def apply(record: GenericRecord): HdfsIndexerMetadata = {
    HdfsIndexerMetadata(
      record.get(FILE_NAME_COLUMN).toString,
      record.get(OFFSET_COLUMN).asInstanceOf[Long],
      record.get(SUB_OFFSET_COLUMN).asInstanceOf[Int],
      record.get(SIZE_COLUMN).asInstanceOf[Int])
  }

}

case class HdfsIndexerMetadata(file: String, position: Long, numInBlock: Int, size: Int)
