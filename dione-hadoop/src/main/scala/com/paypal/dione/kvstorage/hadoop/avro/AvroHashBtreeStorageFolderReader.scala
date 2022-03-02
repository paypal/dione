package com.paypal.dione.kvstorage.hadoop.avro

import com.paypal.dione.kvstorage.hadoop.KVStorageFileReader
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.hashing.MurmurHash3

object AvroHashBtreeStorageFolderReader {

  private val logger = LoggerFactory.getLogger(this.getClass)

  val AVRO_BTREE_FILE_EXTENSION = ".btree.avro"
  val AVRO_BTREE_SCHEMA_FILENAME = ".btree.avsc"

  def apply(folderName: String): AvroHashBtreeStorageFolderReader = {
    val conf = new Configuration()
    val fs = new Path(folderName).getFileSystem(conf)

    logger.debug("listing files in folder: " + folderName)
    val fileList = fs.listStatus(new Path(folderName)).map(_.getPath.getName).filter(_.endsWith(AVRO_BTREE_FILE_EXTENSION)).sorted
    logger.debug("got file list with " + fileList.size + " files")

    new AvroHashBtreeStorageFolderReader(folderName, fileList.toList)
  }

  val hashModuloUdf = (keys: mutable.WrappedArray[String], modulo: Int) => {
    nonNegativeMod(MurmurHash3.orderedHash(keys), modulo)
  }

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

}

case class AvroHashBtreeStorageFolderReader(folderName: String, fileList: List[String])
  extends KVStorageFileReader[Seq[Any], GenericRecord](folderName) {

  def getFile(key: Seq[Any]): AvroBtreeStorageFileReader = {
    val numFile = AvroHashBtreeStorageFolderReader.hashModuloUdf(
      mutable.WrappedArray.make(key.toArray.map(_.toString)), fileList.size)
    getFile(numFile)
  }

  private def getFile(numFile: Int): AvroBtreeStorageFileReader = {
    val kvStorageFilename = new Path(folderName, fileList(numFile)).toString
    AvroBtreeStorageFileReader(kvStorageFilename)
  }

  override def getIterator(key: Seq[Any]): Iterator[GenericRecord] = {
    val kvStorageFileReader = getFile(key)
    try {
      kvStorageFileReader.getIterator(key)
    } //finally kvStorageFileReader.close()
  }

  override def getIterator(): Iterator[(Seq[Any], GenericRecord)] = ???

}