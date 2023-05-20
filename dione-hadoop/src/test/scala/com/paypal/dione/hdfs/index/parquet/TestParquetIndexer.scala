package com.paypal.dione.hdfs.index.parquet

import com.paypal.dione.avro.utils.AvroExtensions
import com.paypal.dione.hdfs.index.HdfsIndexerMetadata
import com.paypal.dione.hdfs.index.parquet.TestParquetIndexer.{fileSystem, parquetFile}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroSchemaConverter
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api._

object TestParquetIndexer extends AvroExtensions {

  val parquetFile = new Path("TestData/hdfs_indexer/parquet_file")
  private val fileSystem = parquetFile.getFileSystem(new Configuration())

  @BeforeAll
  def dataPrep(): Unit = {
    import org.apache.parquet.avro.AvroParquetWriter

    val avroSchema = SchemaBuilder.record("single_string").fields()
      .requiredString("val1")
      .requiredInt("val2")
      .optionalString("val3")
      .requiredInt("val4")
      .endRecord()

    val conf = fileSystem.getConf

    fileSystem.delete(parquetFile, false)
    val fileWriter = AvroParquetWriter.builder[GenericRecord](parquetFile)
      .withRowGroupSize(100)
      .withSchema(avroSchema)
      .withConf(conf).build
    (1 to 1000).foreach(i => {
      val r = new GenericData.Record(avroSchema).putItems(
        (i + "", i, (i * 2) + "", i * 2).productIterator)
      fileWriter.write(r)
    })

    fileWriter.close()
  }
}

@TestMethodOrder(classOf[OrderAnnotation])
class TestParquetIndexer {

  val projectedAvroSchema = SchemaBuilder.record("asd").fields()
    .requiredInt("val2")
    .optionalString("val3")
    .endRecord()

  @Test
  @Order(1)
  def testCreateIndex: Unit = {
    val conf = fileSystem.getConf
    val parquetIndexList = ParquetIndexer(TestParquetIndexer.parquetFile, 0, 0, conf, Some(projectedAvroSchema))
      .iteratorWithMetadata.toList

    //println(parquetIndexList)
    Assertions.assertEquals(1000, parquetIndexList.size)
    Assertions.assertEquals((0,0), (parquetIndexList.head._2.position, parquetIndexList.head._2.numInBlock))
  }

  @Order(2)
  @Test
  def testSimpleFetch(): Unit = {
    val parquetIndexer = new ParquetIndexer(parquetFile, fileSystem.getConf)
    Assertions.assertEquals("{\"val1\": \"5\", \"val2\": 5, \"val3\": \"10\", \"val4\": 10}",
      parquetIndexer.fetch(HdfsIndexerMetadata(parquetFile.toString, 0,4,0)).toString)

    Assertions.assertEquals("{\"val1\": \"999\", \"val2\": 999, \"val3\": \"1998\", \"val4\": 1998}",
      parquetIndexer.fetch(9,98).toString)
  }

  @Order(3)
  @Test
  def testProjectedRead(): Unit = {

    val parquetIndexer = ParquetIndexer(parquetFile, 0, 0, fileSystem.getConf, Some(projectedAvroSchema))
    (1 to 4).foreach(_ => parquetIndexer.next())

    val gr = parquetIndexer.next()
    Assertions.assertEquals("{\"val2\": 5, \"val3\": \"10\"}", gr.toString)
  }

  @Order(4)
  @Test
  def testMapConversion(): Unit = {
    val str = "{\"type\":\"record\",\"name\":\"asd\",\"namespace\":\"dsa_ns\",\"fields\":[{\"name\":\"id_col\",\"type\":[\"string\",\"null\"]},{\"name\":\"var1\",\"type\":[\"string\",\"null\"]},{\"name\":\"var_map\",\"type\":[{\"type\":\"map\",\"values\":[\"string\",\"null\"]},\"null\"]}]}"
    val conf = fileSystem.getConf
    val prj = new AvroSchemaConverter(conf).convert(new Schema.Parser().parse(str))
    // this is the current "wrong" conversion which caused a bug in ParquetIndexer and an ugly workaround
    // looks like this is fixed now with the new parquet-1.12.2 version
    Assertions.assertEquals(
      """message dsa_ns.asd {
        |  optional binary id_col (STRING);
        |  optional binary var1 (STRING);
        |  optional group var_map (MAP) {
        |    repeated group key_value (MAP_KEY_VALUE) {
        |      required binary key (STRING);
        |      optional binary value (STRING);
        |    }
        |  }
        |}
        |""".stripMargin, prj.toString)

    // this is the "correct" conversion:
    //
    //    message dsa_ns.asd {
    //      optional binary id_col (UTF8);
    //      optional binary var1 (UTF8);
    //      optional group var_map (MAP) {
    //        repeated group key_value {
    //          required binary key (UTF8);
    //          optional binary value (UTF8);
    //        }
    //      }
    //    }
  }

}
