package com.paypal.dione.kvstorage.hadoop.avro

import com.paypal.dione.avro.utils.AvroExtensions
import org.apache.avro.SchemaBuilder
import org.junit.jupiter.api.{Assertions, Test}

import scala.collection.mutable.ArrayBuffer

class TestAvroBtreeStorageFile extends AvroExtensions {
  val simpleSchema = SchemaBuilder.record("single_string").fields().requiredString("val1").endRecord()
  val simpleSchema2 = SchemaBuilder.record("single_string2").fields().requiredString("val2").endRecord()
  val tupleSchema = SchemaBuilder.record("simple_tuple").fields().requiredString("val_string").requiredInt("val_int").endRecord()

  val simpleStorage = AvroBtreeStorageFileFactory(simpleSchema, simpleSchema2)
  val tuplesStorage = AvroBtreeStorageFileFactory(simpleSchema, tupleSchema)

  val filename = "TestData/TestAvroBtreeStorageFile"
  val printDebug = false
  val strList = new ArrayBuffer[String]

  @Test
  def testOneLevel(): Unit = {

    def tuple2records(t: (String, (String, Int))) = t match {
      case (k, v) =>
        (simpleSchema.createRecord(k), tupleSchema.createRecord().putItems(v.productIterator))
    }

    val kvStorageFileWriter = tuplesStorage.writer(3, 1)
    val entries = (1 to 10).iterator.map(i => (i.toString.reverse.padTo(3, '0').reverse, (i.toString, i))).map(tuple2records)
    kvStorageFileWriter.write(entries, filename)

    val kvStorageFileReader = tuplesStorage.reader(filename)

    Assertions.assertEquals(10, kvStorageFileReader.getIterator().toList.size)
    val expected = List(("001", ("1", 1)), ("002", ("2", 2)), ("003", ("3", 3))).map(tuple2records).map(_.toString())
    val actual = kvStorageFileReader.getIterator().toList.take(3).map(_.toString())
    Assertions.assertEquals(expected, actual)
  }

  @Test
  def testIntervalOfOne(): Unit = {
    val kvStorageFileWriter = simpleStorage.writer(1, 3)
    val entries = (1 to 10).iterator.map(_.toString.reverse.padTo(3, '0').reverse)
      .map(i => (simpleSchema.createRecord(i), simpleSchema2.createRecord(i)))
    kvStorageFileWriter.write(entries, filename)

    val kvStorageFileReader = simpleStorage.reader(filename)
    printBtreeAvroFile(kvStorageFileReader)

    Assertions.assertEquals("002", kvStorageFileReader.get(simpleSchema.createRecord("002")).get.get("val2").toString)
    Assertions.assertEquals(None, kvStorageFileReader.get(simpleSchema.createRecord("100")))
    Assertions.assertEquals(None, kvStorageFileReader.get(simpleSchema.createRecord("000")))
    Assertions.assertEquals(10, kvStorageFileReader.getIterator().size)
  }

  @Test
  def testBigger(): Unit = {
    val kvStorageFileWriter = simpleStorage.writer(50, 3)
    val entries = (1 to 100000).iterator.map(i => (simpleSchema.createRecord(i.toString), simpleSchema2.createRecord(i.toString)))
    kvStorageFileWriter.write(entries, filename)

    val kvStorageFileReader = simpleStorage.reader(filename)
    Assertions.assertEquals("1234", kvStorageFileReader.get(simpleSchema.createRecord("1234")).get.get("val2").toString)
    Assertions.assertEquals(None, kvStorageFileReader.get(simpleSchema.createRecord("100a")))
    Assertions.assertEquals(None, kvStorageFileReader.get(simpleSchema.createRecord("000")))
    Assertions.assertEquals(100000, kvStorageFileReader.getIterator().size)
  }

  @Test
  def testMany(): Unit = {
    val kvStorageFileWriter = simpleStorage.writer(2, 3)
    val N = 100
    val entries = (1 to N).iterator.map(i => (simpleSchema.createRecord((i * 2).toString), simpleSchema2.createRecord((i * 2).toString)))
    kvStorageFileWriter.write(entries, filename)

    val kvStorageFileReader = simpleStorage.reader(filename)
    (-1 to 2 * N + 2).foreach(i => {
      //      println("Trying to get: " + i)
      if (i <= 0 || i > 2 * N || i % 2 == 1)
        Assertions.assertEquals(None, kvStorageFileReader.get(simpleSchema2.createRecord(i.toString)))
      else
        Assertions.assertEquals(i.toString, kvStorageFileReader.get(simpleSchema2.createRecord(i.toString)).get.get("val2").toString)
    })
  }

  @Test
  def simpleTest(): Unit = {
    val kvStorageFileWriter = simpleStorage.writer(2, 3)

    val entries = Seq("a" -> "1", "c" -> "3", "b" -> "2").iterator
      .map(i => (simpleSchema.createRecord(i._1), simpleSchema2.createRecord(i._2)))
    kvStorageFileWriter.write(entries, filename)

    val kvStorageFileReader = simpleStorage.reader(filename)
    Assertions.assertEquals("1", kvStorageFileReader.get(simpleSchema.createRecord("a")).get.get("val2").toString)
    Assertions.assertEquals(None, kvStorageFileReader.get(simpleSchema.createRecord("aa")))
    Assertions.assertEquals(3, kvStorageFileReader.getIterator().size)
  }

  @Test
  def testBugLast(): Unit = {
    val kvStorageFileWriter = simpleStorage.writer(1000, 2)
    val entries = (1 to 19).iterator.map(_.toString).map{ i=>
      (simpleSchema.createRecord(i), simpleSchema2.createRecord(i))
    }
    kvStorageFileWriter.write(entries, filename)

    val kvStorageFileReader = simpleStorage.reader(filename)
    (1 to 19).foreach(i => {
      Assertions.assertEquals("" + i, kvStorageFileReader.get(simpleSchema2.createRecord("" + i)).get.get("val2").toString)
    })
    Assertions.assertEquals(None, kvStorageFileReader.get(simpleSchema.createRecord("0")))
    Assertions.assertEquals(None, kvStorageFileReader.get(simpleSchema.createRecord("20")))
    Assertions.assertEquals(None, kvStorageFileReader.get(simpleSchema.createRecord("100")))
  }

  def btreeProps(n: Int, interval: Int, height: Int): Unit = {
    val kvStorageFileWriter = simpleStorage.writer(interval, height)
    val entries = (1 to n).map(_.toString.reverse.padTo(3, '0').reverse)
    kvStorageFileWriter.write(entries.iterator.map(i => (simpleSchema.createRecord(i), simpleSchema2.createRecord(i))), filename)

    val kvStorageFileReader = simpleStorage.reader(filename)
    Assertions.assertEquals(entries.toList.mkString(","),
      kvStorageFileReader.getIterator().map(_._2.get("val2")).toList.mkString(","))

    printBtreeAvroFile(kvStorageFileReader)
  }

  def printBtreeAvroFile(kvStorageFileReader: AvroBtreeStorageFileReader): Unit = {
    import scala.collection.JavaConverters._
    val fileS = kvStorageFileReader.fileReader.getmFileReader
    fileS.sync(0)
    val it = fileS.iterator().asScala
    var block = 0L
    val headerPos = kvStorageFileReader.fileReader.getFileHeaderEnd
    printDebug("datasize: " + kvStorageFileReader.fileReader.getmFileReader.getMetaLong("data_bytes"))
    printDebug("header end at: " + headerPos)

    strList.clear()
    it.foreach(r => {
      val lastSync = kvStorageFileReader.fileReader.getmFileReader.previousSync()
      if (lastSync!=block) {
        printDebug(r.toString)
        if (block>0)
          printDebug("block ^^ at:" + (block-headerPos))
        block = lastSync
      } else {
        printDebug(r.toString)
      }
    })
  }

  def printDebug(log: String): Unit = {
    if (printDebug)
      println(log)
    strList.append(log)
  }

  @Test
  def testIterator33(): Unit = btreeProps(100, 3, 3)

  @Test
  def testIterator11(): Unit = btreeProps(100, 1, 1)

  @Test
  def testIterator1000_1(): Unit = btreeProps(100,1000, 1)

  @Test
  def testIterator10_3(): Unit = btreeProps(100,10, 3)

  @Test
  def testIterator30_3_3(): Unit = {
    btreeProps(20,2, 3)
    Assertions.assertEquals(
      """{val1: 001, val2: 001, metadata: 295}
        |{val1: 008, val2: 008, metadata: 195}
        |{val1: 015, val2: 015, metadata: 93}
        |block ^^ at:0
        |{val1: 002, val2: 002, metadata: 259}
        |{val1: 005, val2: 005, metadata: 227}
        |block ^^ at:45
        |{val1: 003, val2: 003, metadata: null}
        |{val1: 004, val2: 004, metadata: null}
        |block ^^ at:81
        |{val1: 006, val2: 006, metadata: null}
        |{val1: 007, val2: 007, metadata: null}
        |block ^^ at:113
        |{val1: 009, val2: 009, metadata: 157}
        |{val1: 012, val2: 012, metadata: 125}
        |block ^^ at:145
        |{val1: 010, val2: 010, metadata: null}
        |{val1: 011, val2: 011, metadata: null}
        |block ^^ at:183
        |{val1: 013, val2: 013, metadata: null}
        |{val1: 014, val2: 014, metadata: null}
        |block ^^ at:215
        |{val1: 016, val2: 016, metadata: 59}
        |{val1: 019, val2: 019, metadata: 27}
        |block ^^ at:247
        |{val1: 017, val2: 017, metadata: null}
        |{val1: 018, val2: 018, metadata: null}
        |block ^^ at:281
        |{val1: 020, val2: 020, metadata: null}
        |block ^^ at:313""".stripMargin,
      strList.mkString("\n").replaceAll("\"", ""))
  }

  @Test
  def testIteratorWithKey(): Unit = {
    val kvStorageFileWriter = simpleStorage.writer(2, 3)

    val entries = Seq("a1" -> "1", "b1" -> "2", "a1" -> "3").iterator
      .map(i => (simpleSchema.createRecord(i._1), simpleSchema2.createRecord(i._2)))
    kvStorageFileWriter.write(entries, filename)

    val kvStorageFileReader = simpleStorage.reader(filename)

    Assertions.assertEquals(List("1", "3"),
      kvStorageFileReader.getIterator(simpleSchema.createRecord("a1")).map(_.get("val2").toString).toList)

    Assertions.assertEquals(List("2"),
      kvStorageFileReader.getIterator(simpleSchema.createRecord("b1")).map(_.get("val2").toString).toList)

    Assertions.assertFalse(kvStorageFileReader.getIterator(simpleSchema.createRecord("a0")).hasNext)
    Assertions.assertFalse(kvStorageFileReader.getIterator(simpleSchema.createRecord("a2")).hasNext)
    Assertions.assertFalse(kvStorageFileReader.getIterator(simpleSchema.createRecord("c1")).hasNext)
  }

  @Test
  def testIteratorWithKey2(): Unit = {
    val kvStorageFileWriter = simpleStorage.writer(100, 1)

    val entries = Seq("a1" -> "1", "b1" -> "2", "b1" -> "3").iterator
      .map(i => (simpleSchema.createRecord(i._1), simpleSchema2.createRecord(i._2)))
    kvStorageFileWriter.write(entries, filename)

    val kvStorageFileReader = simpleStorage.reader(filename)

    Assertions.assertEquals(List("2", "3"),
      kvStorageFileReader.getIterator(simpleSchema.createRecord("b1")).map(_.get("val2").toString).toList)
  }
}

