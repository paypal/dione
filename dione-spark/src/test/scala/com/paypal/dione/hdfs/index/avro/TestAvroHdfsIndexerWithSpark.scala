package com.paypal.dione.hdfs.index.avro

import java.io.File

import com.paypal.dione.SparkCleanTestDB
import com.paypal.dione.avro.utils.AvroExtensions
import com.paypal.dione.hdfs.index.HdfsIndexContants._
import com.paypal.dione.hdfs.index.HdfsIndexerMetadata
import com.paypal.dione.hdfs.index.avro.TestAvroHdfsIndexerWithSpark.baseTestPath
import com.paypal.dione.kvstorage.hadoop.avro.{AvroBtreeStorageFileReader, AvroBtreeStorageFileWriter}
import com.paypal.dione.spark.index.avro.SeqSplitTest.fs
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api._

import scala.collection.JavaConversions._

object TestAvroHdfsIndexerWithSpark extends SparkCleanTestDB {

  override val baseTestPath: String = "TestData/TestAvroHdfsIndexerWithSpark/"
  override val dbName: String = "TestAvroHdfsIndexerWithSpark"

  @BeforeAll
  def initData(): Unit = {
    import spark.implicits._

    val varsDF = (1 to 30).map(i => ("msg_" + i, "var_a_" + i, "var_b_" + i, "2018-10-04 12:34:" + (i % 60).toString.padTo(2, '0')))
      .toDF("message_id", "var1", "var2", "time_result_created").repartition(3)

    varsDF.createOrReplaceTempView("t")
    spark.sql(s"create table t1 stored as avro as select * from t")

    spark.table("t1").show()
  }

}

@TestMethodOrder(classOf[OrderAnnotation])
class TestAvroHdfsIndexerWithSpark extends AvroExtensions {

  private val avroKeySchema = SchemaBuilder.record("key").fields().requiredString("message_id").endRecord()
  private val avroMoreFieldsSchema = SchemaBuilder.record("index").fields().requiredString("time_result_created").endRecord()

  private val avroMergedSchema = getMergedAvroSchema(avroMoreFieldsSchema)

  lazy val spark = TestAvroHdfsIndexerWithSpark.spark

  @Order(1)
  @Test
  def testHdfsIndexer(): Unit = {
    val showPartition = spark.sql("desc formatted t1").collect()
    val prtLocationStr = showPartition.find(row => row.getString(0).contains("Location")).get.getString(1)
    val locationPath = new Path(prtLocationStr)

    val conf = new Configuration()
    val fs = locationPath.getFileSystem(conf)
    val files = fs.listStatus(locationPath)
    println("number of files: " + files.size)
    files.foreach(file => {
      // create the index data
      val entriesIt = AvroIndexer(file.getPath, 0, 1 << 30, fs.getConf).iteratorWithMetadata
        .map { case (gr, idx) => {
          val key = projectRecord(gr, avroKeySchema)
          val v = mergeWithGenericRecord(idx, projectRecord(gr, avroMoreFieldsSchema))
          (key, v)
        } }
      val filename = file.getPath.getName.substring(0, 10)
      // save the index data in a key-value storage format
      AvroBtreeStorageFileWriter(avroKeySchema, avroMergedSchema, 3, 2)
        .write(entriesIt, baseTestPath + "avro_hdfs_btree/index_" + filename)
    })
  }

  @Order(2)
  @Test
  def testIndexKeyValue(): Unit = {
    val avroBtreeStorageFileReader = AvroBtreeStorageFileReader(baseTestPath + "avro_hdfs_btree/index_part-00001")

    Assertions.assertEquals(10, avroBtreeStorageFileReader.getIterator().size)

    val filename = avroBtreeStorageFileReader.get(createRecord(avroKeySchema, "msg_13")).get.get(FILE_NAME_COLUMN).toString
    Assertions.assertEquals("part-00001", filename.substring(filename.lastIndexOf("/") + 1).substring(0, 10))
    Assertions.assertEquals(None, avroBtreeStorageFileReader.get(createRecord(avroKeySchema, "foo")))
  }

  @Order(3)
  @Test
  def testIndexerFetch(): Unit = {
    val avroBtreeStorageFileReader = AvroBtreeStorageFileReader(baseTestPath + "avro_hdfs_btree/index_part-00001")

    val r = avroBtreeStorageFileReader.get(createRecord(avroKeySchema, "msg_13")).get
    val fetcher = AvroIndexer(new Path(r.get(FILE_NAME_COLUMN).toString), 0, 1<<30, fs.getConf)
    val gr = fetcher.fetch(HdfsIndexerMetadata(r))
    println(gr)
    Assertions.assertEquals("var_a_13", gr.get("var1").toString)
  }

  @Order(4)
  @Test
  def testFolderIndexing(): Unit = {
    val showPartition = TestAvroHdfsIndexerWithSpark.spark.sql("desc formatted t1").collect()
    val prtLocationStr = showPartition.find(row => row.getString(0).contains("Location")).get.getString(1)
    val locationPath = new Path(prtLocationStr)

    val conf = new Configuration()
    val fs = locationPath.getFileSystem(conf)
    val files = fs.listStatus(locationPath)
    println("number of files: " + files.size)

    val folderEntries = files.flatMap(file => {
      AvroIndexer(file.getPath, 1, 1 << 30, fs.getConf).iteratorWithMetadata
        .map { case (gr, idx) => {
          val key = projectRecord(gr, avroKeySchema)
          val v = mergeWithGenericRecord(idx, projectRecord(gr, avroMoreFieldsSchema))
          (key, v)
        }}
    }).iterator

    AvroBtreeStorageFileWriter(avroKeySchema, avroMergedSchema, 3, 2)
      .write(folderEntries, baseTestPath + "avro_folder_btree/idx_file")
  }

  @Order(5)
  @Test
  def testFolderKeyValue(): Unit = {
    val avroBtreeStorageFileReader = AvroBtreeStorageFileReader(baseTestPath + "avro_folder_btree/idx_file")

    val gr = avroBtreeStorageFileReader.get(createRecord(avroKeySchema, "msg_20")).get
    val filename = gr.get(FILE_NAME_COLUMN).toString

    Assertions.assertEquals("part-00002", filename.substring(filename.lastIndexOf("/")+1).substring(0,10))
    Assertions.assertEquals("2018-10-04 12:34:20", gr.get("time_result_created").toString)
    avroBtreeStorageFileReader.fileReader.sync(0)
    Assertions.assertEquals(30, avroBtreeStorageFileReader.getIterator().size)
    Assertions.assertEquals(None, avroBtreeStorageFileReader.get(createRecord(avroKeySchema, "m3")))
  }

  @Order(6)
  @Test
  def testReadIndexInHive(): Unit = {
    spark.sql("drop table if exists avro_btree_idx")
    val fullPath = new File(baseTestPath + "avro_folder_btree").getAbsoluteFile.toString

    val schemaFullPath = new File("src/test/resources/btree_schema_example.avsc").getAbsoluteFile.toString
    spark.sql(
      s"""
         |create external table avro_btree_idx (
         |    a string
         |)
         |TBLPROPERTIES ("avro.schema.url"="$schemaFullPath")
         |stored as AVRO
         |location '$fullPath'
         |""".stripMargin)

    spark.table("avro_btree_idx").show(100, truncate = false)
  }

  @Order(7)
  @Test
  def testJoinAndFetchWithIndexInSpark(): Unit = {
    import spark.implicits._
    val ds = spark.createDataset((9 to 11).map(i => "msg_"+i)).toDF("message_id")

    val vars = ds.join(spark.table("avro_btree_idx"), "message_id").map(row => {
      val file = new Path(row.getAs[String](FILE_NAME_COLUMN))
      val fs = file.getFileSystem(new Configuration())
      val gr = AvroIndexer(file, 0, 1<<30, fs.getConf).fetch(HdfsIndexerMetadata(row.getAs[String](FILE_NAME_COLUMN),
        row.getAs[Long](OFFSET_COLUMN), row.getAs[Int](SUB_OFFSET_COLUMN)))
      (gr.get("message_id").toString, gr.get("var1").toString, gr.get("var2").toString)
    })

    Assertions.assertEquals(List("(msg_10,var_a_10,var_b_10)", "(msg_11,var_a_11,var_b_11)", "(msg_9,var_a_9,var_b_9)"),
      vars.collect().sorted.toList.map(_.toString()))
  }

  def mergeWithGenericRecord(idx: HdfsIndexerMetadata, gr: GenericRecord): GenericRecord = {
    val mergedSchema = getMergedAvroSchema(gr.getSchema)
    val clonedGR = projectRecord(gr, mergedSchema)
    clonedGR.put(FILE_NAME_COLUMN, idx.file)
    clonedGR.put(OFFSET_COLUMN, idx.position)
    clonedGR.put(SUB_OFFSET_COLUMN, idx.numInBlock)
    clonedGR.put(SIZE_COLUMN, idx.size)
    clonedGR
  }

  def getMergedAvroSchema(avroSchema: Schema): Schema = {

    val avroIndexerSchema = SchemaBuilder.record("index_metadata").fields().requiredString(FILE_NAME_COLUMN)
      .requiredLong(OFFSET_COLUMN).requiredInt(SUB_OFFSET_COLUMN).requiredInt(SIZE_COLUMN).endRecord()

    import scala.collection.JavaConversions._

    val sb = Schema.createRecord(avroSchema.getName, avroSchema.getDoc, avroSchema.getNamespace, avroSchema.isError)
    val mergedFields = (avroSchema.getFields ++ avroIndexerSchema.getFields).map(cloneField)
    sb.setFields(mergedFields)
    sb
  }

  def projectRecord(record: GenericRecord, subSchema: Schema): GenericRecord = {
    val otherRecord = new GenericData.Record(subSchema)
    subSchema.getFields.map(_.name()).foreach { name =>
      otherRecord.put(name, record.get(name))
    }
    otherRecord
  }

  def cloneField(f: Schema.Field) = new Schema.Field(f.name, f.schema, f.doc, f.defaultVal(), f.order)

}

