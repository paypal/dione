package com.paypal.dione.hdfs.index.avro

import com.paypal.dione.avro.utils.AvroExtensions
import com.paypal.dione.hdfs.index.HdfsIndexerMetadata
import org.apache.avro.SchemaBuilder
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.DatumWriter
import org.apache.avro.specific.SpecificData
import org.apache.avro.util.Utf8
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api._

object TestAvroIndexer extends AvroExtensions {

  val avroFile = new Path("TestData/hdfs_indexer/avro_file")
  val fileSystem = avroFile.getFileSystem(new Configuration())

  @BeforeAll
  def dataPrep(): Unit = {

    val avroSchema = SchemaBuilder.record("single_string").fields()
      .requiredString("val1")
      .requiredInt("val2")
      .requiredString("val3")
      .requiredInt("val4")
      .endRecord()
    val model = SpecificData.get
    val datumWriter = model.createDatumWriter(avroSchema).asInstanceOf[DatumWriter[GenericRecord]]

    val dataOutputStream = fileSystem.create(avroFile)
    val fileWriter = new DataFileWriter[GenericRecord](datumWriter).create(avroSchema, dataOutputStream)

    (1 to 100).foreach(i => {
      val r = new GenericData.Record(avroSchema).putItems(
        (i + "", i, (i * 2) + "", i * 2).productIterator)
      fileWriter.append(r)
      if (i % 17 == 3)
        fileWriter.sync()
    })

    fileWriter.close()
  }
}

@TestMethodOrder(classOf[OrderAnnotation])
class TestAvroIndexer extends AvroExtensions {

  import TestAvroIndexer._

  @Test
  @Order(1)
  def testSimpleCreateIndex(): Unit = {
    val entries = AvroIndexer(avroFile, 0, 1<<30, fileSystem.getConf).iteratorWithMetadata.toList

    val schema = SchemaBuilder.record("single_string").fields().requiredString("val1")
      .requiredInt("val2").requiredString("val3").requiredInt("val4").endRecord()

    def assert(num: Int, md: HdfsIndexerMetadata): Unit = {
      val m = new GenericData.Record(schema).putItems(
        (num.toString, num, (num * 2).toString, num * 2).productIterator)
      val res = entries(num - 1)
      Assertions.assertEquals((m, md).toString(), res.toString())
    }

    Assertions.assertEquals(100, entries.size)

    entries.take(10).foreach(println)

    // first
    assert(1, HdfsIndexerMetadata(avroFile.toString, 209, 0, 6))

    // same block:
    assert(9, HdfsIndexerMetadata(avroFile.toString, 245, 5, 7))
    assert(13, HdfsIndexerMetadata(avroFile.toString, 245, 9, 7))
    // new block:
    assert(50, HdfsIndexerMetadata(avroFile.toString, 554, 12, 9))
    assert(51, HdfsIndexerMetadata(avroFile.toString, 554, 13, 9))
  }

  @Order(2)
  @Test
  def testSimpleFetch(): Unit = {

    val avroIndexer = AvroIndexer(avroFile, 0, 1 << 30, fileSystem.getConf)

    { // try the first line in first block
      val gr = avroIndexer.fetch(HdfsIndexerMetadata(avroFile.toString, 209, 0))
      Assertions.assertEquals("2", gr.get("val3").asInstanceOf[Utf8].toString)
      Assertions.assertEquals("1", gr.get("val1").asInstanceOf[Utf8].toString)
    }

    { // try the last line in first block
      val gr = avroIndexer.fetch(HdfsIndexerMetadata(avroFile.toString, 209, 2))
      Assertions.assertEquals("6", gr.get("val3").asInstanceOf[Utf8].toString)
    }

    { // try the first line in another block
      val gr = avroIndexer.fetch(HdfsIndexerMetadata(avroFile.toString, 245, 0))
      Assertions.assertEquals("8", gr.get("val3").asInstanceOf[Utf8].toString)
    }

    (6 to 14).foreach(i => {
      val gr = avroIndexer.fetch(HdfsIndexerMetadata(avroFile.toString, 393, i))
      Assertions.assertEquals(((21 + i) * 2).toString, gr.get("val3").asInstanceOf[Utf8].toString)
    })
  }
}