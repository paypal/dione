package com.paypal.dione.hdfs.index.sequence

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.io.{SequenceFile, Text, Writable}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Assertions, Test}

import scala.util.Random

class TestSequenceFileIndexer {

  @Test
  def sanityTest: Unit = {
    test()
  }

  @Test
  def testBlockCompression: Unit = {
    test(SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK))
  }

  @Test
  def testRecordCompression: Unit = {
    test(SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD))
  }

  private def test(options: Writer.Option*) = {
    val file = new Path("/tmp/.indexTests")
    val configuration = new Configuration()
    val separator = ","

    val dummySplitter = new SequenceFileIndexer.Deserializer {
      override def deserialize(k: Writable, v: Writable) =
        v.asInstanceOf[Text].toString.split(",").toList match {
          case key :: f1 :: f2 :: f3 :: Nil =>
            Seq("id" -> key, "col1" -> f1, "col2" -> f2, "col3" -> f3)
          case other =>
            throw new RuntimeException("unexpected row: " + other)
        }
    }


    // data:
    val data = (0 to 50).map { i =>
      val key = i.toString
      val f1 = Random.nextInt().toString
      val f2 = Random.nextInt().toString
      val f3 = (0 to (5 << 10)).map(_ => Random.nextInt()).mkString("") // large payload to enforce multiple blocks
      (key, f1, f2, f3)
    }.toArray

    val opts = options ++ Seq(
      SequenceFile.Writer.file(file),
      SequenceFile.Writer.keyClass(classOf[Text]),
      SequenceFile.Writer.valueClass(classOf[Text]))

    val writer = SequenceFile.createWriter(configuration, opts: _*)
    data.foreach { case (key, f1, f2, f3) =>
      val text = new Text(Seq(key, f1, f2, f3).mkString(separator))
      val k = new Text("ignoreMe") // hive ignores the key
      writer.append(k, text)
      println(s"key $key is at position ${writer.getLength}")
    }
    writer.close()

    // index the file:
    val indexer1 = SequenceFileIndexer(file, 0, 1 << 30, dummySplitter, configuration)
    val index = indexer1.iteratorWithMetadata.toArray
    Assertions.assertEquals(51, index.length)

    // fetch each row by index:
    index.zip(data).foreach {
      case t@(index, data) =>
        //println(t)
        val indexer2 = SequenceFileIndexer(file, 0, 1 << 30, dummySplitter, configuration)
        println("fetching: " + index._2)
        val m = indexer2.fetch(index._2)
        val actual: Array[AnyRef] = m.map(_._2.toString).toArray
        val expected: Array[AnyRef] = data.productIterator.toArray.map(_.toString).toArray
        assertArrayEquals(expected, actual)
    }

  }
}
