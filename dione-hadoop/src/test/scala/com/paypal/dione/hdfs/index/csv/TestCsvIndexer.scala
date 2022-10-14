package com.paypal.dione.hdfs.index.csv

import com.paypal.dione.hdfs.index.HdfsIndexerMetadata
import com.paypal.dione.hdfs.index.csv.TestCsvIndexer.{entries, splitEntries}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api._

object TestCsvIndexer {
  var entries: Seq[(Seq[String], HdfsIndexerMetadata)] = _
  var splitEntries: Seq[(Seq[String], HdfsIndexerMetadata)] = _
}

@TestMethodOrder(classOf[OrderAnnotation])
class TestCsvIndexer {

  val bizlogFile = new Path("src/test/resources/csv/file.csv")
  val fileSystem = bizlogFile.getFileSystem(new Configuration())

  @Test
  @Order(1)
  def testSimpleCreateIndex(): Unit = {
    entries = CsvIndexer(bizlogFile, 0, 1<<30, fileSystem.getConf, ',').iteratorWithMetadata.toList

    Assertions.assertEquals(4, entries.size)

    entries.take(10).foreach(println)

    Assertions.assertEquals(entries.head._2, HdfsIndexerMetadata(bizlogFile.toString, 0, 0, -1))
    Assertions.assertEquals(entries.head._1.toList, List("a1", "b1", "c1", "d1", "11", "12", "13"))
    Assertions.assertEquals(entries(2)._2, HdfsIndexerMetadata(bizlogFile.toString, 21, 0, -1))
  }

  @Test
  @Order(1)
  def testCreateIndexFromSplit(): Unit = {
    splitEntries = CsvIndexer(bizlogFile, 30, 1000, fileSystem.getConf, ',').iteratorWithMetadata.toList

    Assertions.assertEquals(2, splitEntries.size)

    splitEntries.take(10).foreach(println)

    Assertions.assertEquals(splitEntries.head._2, HdfsIndexerMetadata(bizlogFile.toString, 30, 0, -1))
    Assertions.assertEquals(splitEntries.head._1.toList, List("a3", "b3", "c3", "d3", "31", "32", "33"))
    Assertions.assertEquals(splitEntries.tail.head._2, HdfsIndexerMetadata(bizlogFile.toString, 42, 0, -1))
  }

  @Order(2)
  @Test
  def testSimpleFetch(): Unit = {

    val csvIndexer = CsvIndexer(bizlogFile, 0, 1 << 30, fileSystem.getConf, ',')

    {
      val sq = csvIndexer.fetch(HdfsIndexerMetadata(bizlogFile.toString, 0, 0))
      Assertions.assertEquals("c1", sq(2))
      Assertions.assertEquals("d1", sq(3))
    }

    {
      val sq = csvIndexer.fetch(HdfsIndexerMetadata(bizlogFile.toString, 21, 0))
      Assertions.assertEquals("c3", sq(2))
      Assertions.assertEquals("d3", sq(3))
    }

    {
      val sq = csvIndexer.fetch(HdfsIndexerMetadata(bizlogFile.toString, 42, 0))
      Assertions.assertEquals("b4", sq(1))
      Assertions.assertEquals("41", sq(4))
    }
  }

  @Order(2)
  @Test
  def testAllEntiresFetch(): Unit = {
    testEntriesFetch(entries)
  }

  @Order(2)
  @Test
  def testSplitEntiresFetch(): Unit = {
    testEntriesFetch(splitEntries)
  }

  def testEntriesFetch(entries: Seq[(Seq[String], HdfsIndexerMetadata)]): Unit = {
    val csvIndexer = CsvIndexer(bizlogFile, 0, 1 << 30, fileSystem.getConf, ',')
    entries.foreach(e => {
      println("fetching: " + e._2)
      val sq = csvIndexer.fetch(e._2)
      Assertions.assertEquals(e._1, sq)
    })
  }

}