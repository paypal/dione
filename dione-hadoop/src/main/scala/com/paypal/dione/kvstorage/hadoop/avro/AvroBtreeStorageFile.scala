package com.paypal.dione.kvstorage.hadoop.avro

import com.paypal.dione.avro.hadoop.file.AvroBtreeFile.Writer
import com.paypal.dione.avro.hadoop.file.{AvroBtreeFile, RecordProjection}
import com.paypal.dione.kvstorage.hadoop.{KVStorageFileReader, KVStorageFileWriter}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.specific.SpecificData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

case class AvroBtreeStorageFileFactory(kschema: Schema, vschema: Schema) {
  def writer(interval: Int, height: Int): AvroBtreeStorageFileWriter =
    AvroBtreeStorageFileWriter(kschema, vschema, interval, height)

  def reader(fileName: String): AvroBtreeStorageFileReader =
    AvroBtreeStorageFileReader(fileName)
}

case class AvroBtreeStorageFileWriter(kschema: Schema,
                                      vschema: Schema,
                                      interval: Int,
                                      height: Int,
                                      preSort: Boolean = true
                                     ) extends KVStorageFileWriter[GenericRecord, GenericRecord] {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def write(entries: Iterator[(GenericRecord, GenericRecord)], path: String): Unit = {
    val writer = initWriter(path)

    val sortedEntriesList =
      if (preSort) {
        val entriesList = entries.toList
        logger.info("read all entries")
        val model = SpecificData.get()
        val sorted = entriesList.sortWith((o1, o2) => model.compare(o1._1, o2._1, kschema) < 0)
        logger.info("sorted all entries list")
        sorted
      } else {
        entries
      }

    var counter = 1
    var lastEntry: (GenericRecord, GenericRecord) = null
    sortedEntriesList.foreach { case e@(keyRecord, valueRecord) =>
      lastEntry = e
      if (counter % 100000 == 0)
        logger.info("written record #" + counter + ": " + e)
      counter += 1

      writer.append(keyRecord, valueRecord)
    }
    writer.close()
    logger.info("last written record #" + counter + ": " + lastEntry)
  }

  private def initWriter(location: String) = {
    val path = new Path(location)
    logger.info("creating new key-value avro file: " + path)

    val options = new Writer.Options()
      .withKeySchema(kschema)
      .withValueSchema(vschema)
      .withConfiguration(new Configuration())
      .withInterval(interval)
      .withHeight(height)
      .withCodec("deflate")
      .withPath(path)

    new AvroBtreeFile.Writer(options)
  }
}


case class AvroBtreeStorageFileReader(override val path: String)
  extends KVStorageFileReader[GenericRecord, GenericRecord](path) with AutoCloseable {
  private val logger = LoggerFactory.getLogger(this.getClass)

  val fileReader = createReaderFrom(path)
  private val keyGR = new GenericData.Record(fileReader.getKeySchema)

  override def getIterator(key: GenericRecord): Iterator[GenericRecord] = {
    fileReader.get(key)
  }

  def toGR(key: Seq[Any], grOpt: Option[GenericData.Record] = None): GenericRecord = {
    val gr = grOpt.getOrElse(new GenericData.Record(fileReader.getKeySchema))
    key.zipWithIndex.foreach { case (obj, i) => gr.put(i, obj) }
    gr
  }

  def getIterator(key: Seq[Any]): Iterator[GenericRecord] = {
    getIterator(toGR(key, Some(keyGR)))
  }

  override def getIterator(): Iterator[(GenericRecord, GenericRecord)] = {
    import scala.collection.JavaConversions._
    val projection = new RecordProjection(fileReader.getKeySchema, fileReader.getValueSchema)

    fileReader.getIterator.map(t => (projection.getKey(t), projection.getValue(t)))
  }

  private def createReaderFrom(path: String) = {
    logger.info(s"Opening avro file: " + path)

    val options = new AvroBtreeFile.Reader.Options()
      .withConfiguration(new Configuration())
      .withPath(new Path(path))

    new AvroBtreeFile.Reader(options)
  }

  override def close(): Unit = fileReader.close()
}

object GenericRecordOrdering extends Ordering[GenericRecord] {
  override def compare(x: GenericRecord, y: GenericRecord): Int =
    GenericData.get().compare(x, y, x.getSchema)
}