package com.paypal.dione.spark.index

import com.paypal.dione.hdfs.index.HdfsIndexContants.FILE_NAME_COLUMN
import com.paypal.dione.hdfs.index.{HdfsIndexer, HdfsIndexerMetadata}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
 * wrapper trait between to use HdfsIndexer inside Spark.
 * indexer implementors should implement:
 *
 * createIndex - given Hadoop file init and call HdfsIndexer.iteratorWithMetadata
 *
 * initHdfsIndexer - create new concrete HdfsIndexer
 *
 * convert - from T to Row (Seq[Any]) to use in IndexReader
 * convert - from T to Map[String, Any] to use by fetch calls
 *
 */
trait SparkIndexer {

  private val logger = LoggerFactory.getLogger(this.getClass)

  type T >: Null

  @transient val spark: SparkSession

  def initHdfsIndexer(file: Path, conf: Configuration, start: Long, end: Long, fieldsSchema: StructType): HdfsIndexer[T]

  def initHdfsIndexer(file: Path, conf: Configuration, fieldsSchema: StructType): HdfsIndexer[T] = {
    initHdfsIndexer(file, conf, 0, 1L << 40, fieldsSchema)
  }

  def convert(t: T): Seq[Any]

  def convertMap(t: T): Map[String, Any]

  /**
   * API for manual indexing not using the Manager's higher level APIs
   *
   * @param filesDF
   * @param fieldsSchema
   * @return
   */
  def createIndexDF(filesDF: DataFrame, fieldsSchema: StructType): DataFrame = {
    IndexManagerUtils.createIndexDF(filesDF, fieldsSchema, this)
  }

  def readPayload(indexGR: GenericRecord, payloadSchema: StructType): Map[String, Any] = {
    logger.debug("initializing file: " + indexGR.get(FILE_NAME_COLUMN).toString)
    val hdfsIndexer = initHdfsIndexer(new Path(indexGR.get(FILE_NAME_COLUMN).toString),
      new Configuration(), payloadSchema)
    val hdfsIndexMetadata = HdfsIndexerMetadata(indexGR)
    val fetchedT = hdfsIndexer.fetch(hdfsIndexMetadata)
    convertMap(fetchedT)
  }

  def loadByIndex(index: DataFrame, payloadSchema: StructType): DataFrame = {
    val ignoreFailures = spark.conf.get("indexer.reader.ignore.failures", "false").toBoolean
    IndexReader.read(index, IndexReader(spark, this, payloadSchema, ignoreFailures))
  }

}
