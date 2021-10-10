package com.paypal.dione.hdfs.index.orc

import com.paypal.dione.hdfs.index.HdfsIndexerMetadata
import com.paypal.dione.hdfs.index.orc.TestOrcIndexer.{fileSystem, orcFile}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.storage.ql.exec.vector.{BytesColumnVector, DoubleColumnVector, LongColumnVector}
import org.apache.orc.{OrcFile, TypeDescription}
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api.{Assertions, BeforeAll, Order, Test, TestMethodOrder}

import java.nio.charset.StandardCharsets


@TestMethodOrder(classOf[OrderAnnotation])
object TestOrcIndexer {

  val orcFile = new Path("TestData/hdfs_indexer/orc_file")
  private val fileSystem = orcFile.getFileSystem(new Configuration())

  @BeforeAll
  def dataPrep(): Unit = {
    fileSystem.delete(orcFile, false)

    val schema = TypeDescription.fromString("struct<id:string,val1:string,val2:float,val3:int>")
    val batch = schema.createRowBatch

    val idColumnVector = batch.cols(0).asInstanceOf[BytesColumnVector]
    val val1ColumnVector = batch.cols(1).asInstanceOf[BytesColumnVector]
    val val2ColumnVector = batch.cols(2).asInstanceOf[DoubleColumnVector]
    val val3ColumnVector = batch.cols(3).asInstanceOf[LongColumnVector]

    val writer = OrcFile.createWriter(orcFile, OrcFile.writerOptions(new Configuration()).setSchema(schema))

    (0 until 13).foreach(i => {
      val ii = batch.size

      val bufId = i.toString.getBytes(StandardCharsets.UTF_8)
      idColumnVector.setRef(ii, bufId, 0, bufId.length)
      val bufVal1 = ("v"+i).getBytes(StandardCharsets.UTF_8)
      val1ColumnVector.setRef(ii, bufVal1, 0, bufVal1.length)
      val2ColumnVector.vector.update(ii, i*2.0)
      val3ColumnVector.vector.update(ii, i)

      batch.size+=1
      if (batch.size == 5) {
        writer.addRowBatch(batch)
        batch.reset()
      }
    })
    writer.addRowBatch(batch)
    writer.close()

  }

}

@TestMethodOrder(classOf[OrderAnnotation])
class TestOrcIndexer {

  @Test
  @Order(1)
  def testCreateIndex: Unit = {
    val orcIndexList = new OrcIndexer(orcFile, fileSystem.getConf).iteratorWithMetadata.toList

    println(orcIndexList)
    Assertions.assertEquals(13, orcIndexList.size)
    Assertions.assertEquals((0,0), (orcIndexList.head._2.position, orcIndexList.head._2.numInBlock))
    Assertions.assertEquals((8,0), (orcIndexList(8)._2.position, orcIndexList(8)._2.numInBlock))
  }

  @Order(2)
  @Test
  def testSimpleFetch(): Unit = {
    val orcIndexer = new OrcIndexer(orcFile, fileSystem.getConf)
    Assertions.assertEquals("Vector((id,7), (val1,v7), (val2,14.0), (val3,7))",
      orcIndexer.fetch(HdfsIndexerMetadata(orcFile.toString, 7, 0)).toString)
  }

}