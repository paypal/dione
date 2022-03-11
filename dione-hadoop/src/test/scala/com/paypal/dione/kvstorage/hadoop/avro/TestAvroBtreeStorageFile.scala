package com.paypal.dione.kvstorage.hadoop.avro

import com.paypal.dione.avro.utils.AvroExtensions
import org.apache.avro.SchemaBuilder
import org.junit.jupiter.api.{Assertions, Test}

class TestAvroBtreeStorageFile extends AvroExtensions {
  val simpleSchema = SchemaBuilder.record("single_string").fields().requiredString("val1").endRecord()
  val simpleSchema2 = SchemaBuilder.record("single_string2").fields().requiredString("val2").endRecord()
  val tupleSchema = SchemaBuilder.record("simple_tuple").fields().requiredString("val_string").requiredInt("val_int").endRecord()

  val simpleStorage = AvroBtreeStorageFileFactory(simpleSchema, simpleSchema2)
  val tuplesStorage = AvroBtreeStorageFileFactory(simpleSchema, tupleSchema)

  val filename = "TestData/TestAvroBtreeStorageFile"

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
    kvStorageFileReader.fileReader.sync(0)
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
    Assertions.assertEquals("002", kvStorageFileReader.get(simpleSchema.createRecord("002")).get.get("val2").toString)
    Assertions.assertEquals(None, kvStorageFileReader.get(simpleSchema.createRecord("100")))
    Assertions.assertEquals(None, kvStorageFileReader.get(simpleSchema.createRecord("000")))
    kvStorageFileReader.fileReader.sync(0)
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
    kvStorageFileReader.fileReader.sync(0)
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
    kvStorageFileReader.fileReader.sync(0)
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

