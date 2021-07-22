package com.paypal.dione.kvstorage.hadoop.avro

import java.io.File

import com.paypal.dione.avro.hadoop.file.AvroBtreeFile
import com.paypal.dione.avro.hadoop.file.AvroBtreeFile.{BufferedWriter, Writer}
import com.paypal.dione.avro.utils.AvroExtensions
import org.apache.avro.SchemaBuilder
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class BufferedWriterTest extends AvroExtensions {
  val schema = SchemaBuilder.record("node").fields()
    .requiredString("val_string")
    .requiredInt("val_int")
    .optionalLong("offset")
    .endRecord()

  @Test
  def sanity: Unit = {

    val writer = new BufferedWriter(new Writer.Options(), schema, "")
    val records = (1 to 20).map(i => (i.toString, i, null.asInstanceOf[Any])).map(_.productIterator).map(schema.createRecord().putItems(_))

    // generate dummy "linked list" where every block points to the previous
    // when the files is reversed, this  is enough to check validity
    var sync: Any = null.asInstanceOf[Object]
    val syncs = new ArrayBuffer[Any]()
    records
      .grouped(4) //4 records per block
      .foreach { block =>
        block.foreach { record =>
          record.put("offset", sync)
          println("original record: " + record)
          writer.append(record)
        }
        sync = writer.sync()
        syncs.add(sync)
      }

    // write reversed file:
    val stream = FileSystem.get(new Configuration()).create(new Path("TestData/avro/buffersTest.avro"))
    writer.reverseAndClose(stream)

    // read:
    val reader = DataFileReader.openReader(
      new File("TestData/avro/buffersTest.avro"), schema.createGenericDatumReader())
      .asInstanceOf[DataFileReader[GenericRecord]]
    //    reader.iterator().foreach(println)
    reader.sync(0)


    // sanity checks:
    assertEquals(20, reader.iterator().size, "should have 20 records")
    reader.sync(0)

    while (reader.hasNext) {
      assertEquals(4, reader.getBlockCount, "every block should have 4 records")
      reader.nextBlock()
    }


    reader.sync(0)
    val dataSize = reader.getMetaLong(AvroBtreeFile.DATA_SIZE_KEY)
    val headerStart = reader.previousSync()

    val fileRecords = reader.iterator().toList

    println("data size: " + dataSize)
    println("start: " + headerStart)
    fileRecords.dropRight(4).foreach { record =>
      val offset = record.get("offset").asInstanceOf[Long]
      val realSync = dataSize - offset + headerStart
      reader.seek(realSync) // seek to pointed block
      println(reader.next())
    }
  }

}
