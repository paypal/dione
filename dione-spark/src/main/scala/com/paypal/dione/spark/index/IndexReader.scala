package com.paypal.dione.spark.index

import com.paypal.dione.hdfs.index.HdfsIndexContants.{FILE_NAME_COLUMN, OFFSET_COLUMN, SIZE_COLUMN, SUB_OFFSET_COLUMN}
import com.paypal.dione.hdfs.index.{HdfsIndexer, HdfsIndexerMetadata}
import com.paypal.dione.spark.metrics.StatsReporter
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, hash, spark_partition_id}
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
 * Base class for readers that support seek to offset and sub offset.
 *
 */
case class IndexReader(@transient spark: SparkSession, sparkIndexer: SparkIndexer, fieldsSchema: StructType,
                       ignoreFailures: Boolean)
  extends Serializable with Logging {
  type T = sparkIndexer.T

  private val logger = LoggerFactory.getLogger(this.getClass)

  @transient private var currentFile: String = _
  @transient private var lastOffset = 0L
  @transient private var lastSubOffset = 0
  @transient private var lastRow: Seq[Any] = _

  val reporter: StatsReporter = IndexReader.getReporter(spark)

  protected var hdfsIndexer: HdfsIndexer[T] = _

  def read(index: DataFrame): DataFrame = IndexReader.read(index, this)

  def readPayload[T](indexGR: GenericRecord): Map[String, Any] = {
    logger.debug("initing file: " + indexGR.get(FILE_NAME_COLUMN).toString)
    val hdfsIndexer = sparkIndexer.initHdfsIndexer(new Path(indexGR.get(FILE_NAME_COLUMN).toString),
      new Configuration(), fieldsSchema)
    val hdfsIndexMetadata = HdfsIndexerMetadata(indexGR)
    val fetchedT = hdfsIndexer.fetch(hdfsIndexMetadata)
    sparkIndexer.convertMap(fetchedT)
  }

  private def init() = {
    reporter.initPartitionMetrics()
  }

  def mapPartitions(iter: Iterator[Row]): Iterator[Row] = {
    if (iter.isEmpty) return iter

    val dummy = new {
      var start = false
    }

    iter.flatMap { row =>
      if (!dummy.start) {
        this.init()
        dummy.start = true
      }
      val tryExtract = Try(extractPayload(row))
      val res = tryExtract match {
        case Success(t) => Iterator(Row.fromSeq(row.toSeq ++ t))
        case Failure(e) =>
          logError("failed to read from " + currentFile, e)
          if (ignoreFailures)
            Iterator.empty
          else throw e
      }

      if (!iter.hasNext)
        partitionDone()
      res
    }
  }

  private def extractPayload(indexRow: Row): Seq[Any] = {
    val file = indexRow.getAs(FILE_NAME_COLUMN).toString
    if (file != currentFile) {
      lastOffset = 0L
      lastSubOffset = -1
      if (currentFile != null)
        reporter.closeFile(currentFile)
      if (hdfsIndexer != null)
        hdfsIndexer.closeCurrentFile()
      reporter.openFile(file)
      hdfsIndexer = sparkIndexer.initHdfsIndexer(new Path(file), new Configuration(), fieldsSchema)
      currentFile = file
    }

    val offset = indexRow.getAs[Long](OFFSET_COLUMN)
    val subOffset = indexRow.getAs[Int](SUB_OFFSET_COLUMN)
    val size = indexRow.getAs[Int](SIZE_COLUMN)
    validateOrder(offset, subOffset, file)

    // short-circuit the case when there is more than one read of the same entry
    if (lastOffset == offset && lastSubOffset == subOffset)
      return lastRow

    reporter.startRecordRead()
    if (offset != lastOffset) {
      logger.debug("seeking to offset {}", offset)
      hdfsIndexer.seek(offset) // new block, seek to start
      lastOffset = offset
      lastSubOffset = -1
    }

    // skip irrelevant rows
    if (lastSubOffset+1 < subOffset) {
      logger.debug("skipping rows from {} to {}", lastSubOffset+1, subOffset)
      (lastSubOffset + 1 until subOffset).foreach(_ => hdfsIndexer.skip())
    }

    val payload = hdfsIndexer.next()
    lastSubOffset = subOffset
    reporter.endRecordRead(size)
    lastRow = sparkIndexer.convert(payload)
    lastRow
  }

  private def partitionDone(): Unit = {
    hdfsIndexer.closeCurrentFile()
    reporter.closeFile(currentFile)
    reporter.partitionDone()
  }

  // TODO: push to hdfsIndexer
  private def validateOrder(currentOffset: Long, currentSubOffset: Int, currentFile: String): Unit = {
    def fail() = {
      val curr = Seq(currentFile, currentOffset, currentSubOffset).mkString(", ")
      val prev = Seq(this.currentFile, lastOffset, lastSubOffset).mkString(", ")
      throw new RuntimeException(s"internal error - rows not sorted. current row: $curr, prev: $prev")
    }

    // unordered read is a fatal performance hit
    if (currentOffset < lastOffset) fail()
    if (currentOffset == lastOffset && currentSubOffset < lastSubOffset) fail()
    if (currentFile < this.currentFile) fail()
  }
}

/**
 * Helper class to read payload data from seekable files, given index DataFrame
 */
object IndexReader {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Read payload data from seekable files
   *
   * @param rawIndex index DataFrame, contains the index metadata columns.
   * @return new DataFrame with new data columns
   */
  def read(rawIndex: DataFrame, reader: IndexReader): DataFrame = {

    // drop payload columns from the index, if exists:
    val cleanIndex = rawIndex.drop(reader.fieldsSchema.map(_.name): _*)
    val outputSchema = StructType(cleanIndex.schema ++ reader.fieldsSchema)

    val spark = rawIndex.sparkSession
    val readInChunks = spark.conf.get("indexer.reader.chunks", "false").toBoolean
    val readerDF = if (readInChunks) {
      preparePartitionsInChunks(cleanIndex)
    } else {
      cleanIndex.repartition(col(FILE_NAME_COLUMN)).sortWithinPartitions(FILE_NAME_COLUMN, OFFSET_COLUMN, SUB_OFFSET_COLUMN)
    }

    readerDF
      // remove intermediate columns:
      .select(cleanIndex.columns.map(col): _*)
      .mapPartitions(reader.mapPartitions)(RowEncoder(outputSchema))
  }

  def getReporter(spark: SparkSession) = {
    val reporter = spark.conf.get("indexer.reader.reporter", null) match {
      case null =>
        new StatsReporter.SoftReporter()
      case "none" =>
        new StatsReporter()
      case clz =>
        Class.forName(clz).getConstructor().newInstance().asInstanceOf[StatsReporter]
    }
    reporter
  }

  private[index] def preparePartitionsInChunks(keysDF: DataFrame) = {
    val spark = keysDF.sparkSession
    import spark.implicits._

    val isDebug = spark.conf.get("indexer.reader.debug", "false").toBoolean

    // the algorithm of this step is explained inline...

    // `spark.sql.shuffle.partitions` is expected to be very large compared to the keys DataFrame, since
    // it should be configured to handle the workload INCLUDING the payloads (after reading from Avro).
    // but before we read from Avro we want to split the data to chunks (see below), and we need to
    // reduce the number of partitions to get more keys inside each partitions.
    val repartitionRatio = spark.conf.get("indexer.reader.repartitionRatio", "100").toInt
    val shufflePartitions = spark.conf.get("spark.sql.shuffle.partitions").toInt
    val partitions = math.max(shufflePartitions / repartitionRatio, 1)

    val reducedPartitionsDF =
      keysDF.repartition(partitions, hash(col(FILE_NAME_COLUMN))).sortWithinPartitions(FILE_NAME_COLUMN, OFFSET_COLUMN, SUB_OFFSET_COLUMN)

    // now inside every partition we split the rows to chunks of X bytes.
    // the size of each row is predicted by `data_size` column which was
    // generated when the row was written to index.
    // depending (also) on Avro compression, the actual size in memory might get many times larger.
    // for example, 20MB in Avro can take more than 500MB in Spark heap.
    // the default chunk size is 20MB, but its expected to be tuned by the user.
    // its important to have low number of partitions at this stage, otherwise the chunks will be too small.
    val maxChunkSize = spark.conf.get("indexer.reader.maxBytes", (20 * (1 << 20)).toString).toInt

    // manually calculating accumulative sum, to maintain the current partitions.
    // using Spark sql functions will cause repartition of the data.
    val accumulatedBytesDF = sumPartitionBytes(reducedPartitionsDF)

    // split and distribute chunks, using original number of partitions:
    val chunkedDF =
      accumulatedBytesDF
        .withColumn("__chunk", ($"__sum" / maxChunkSize).cast("int"))
        .repartition(hash(spark_partition_id()), hash($"__chunk"))
        .sortWithinPartitions(FILE_NAME_COLUMN, OFFSET_COLUMN, SUB_OFFSET_COLUMN)

    if (isDebug) {
      printChunksStats(chunkedDF)
      chunkedDF
    } else {
      chunkedDF.select(keysDF.columns.map(col): _*) //drop intermediate columns
    }
  }

  private[index] def sumPartitionBytes(reducedPartitionsDF: DataFrame) = {
    val newFileCostInBytes = reducedPartitionsDF.sparkSession.conf.get(
      "indexer.reader.newFileCostInBytes", (1 << 20).toString).toInt
    case class Accumulator(row: Row, file: String, accSum: Long, count: Long)
    reducedPartitionsDF.mapPartitions { iter =>
      iter.scanLeft[Accumulator](Accumulator(Row(), null, 0L, 0L)) { case (Accumulator(_, file, acc, count), row) =>
        val currentFile = row.getAs(FILE_NAME_COLUMN).toString
        val cost = if (currentFile == file) 0 else newFileCostInBytes
        val sum = acc + row.getAs[Int](SIZE_COLUMN) + cost
        val rowWithAccSum = Row.fromSeq(row.toSeq ++ Seq(sum)) // add __sum column
        Accumulator(rowWithAccSum, currentFile, sum, count + 1)
      }.drop(1 /* <-- drop the initial empty row */).map(_.row)
    }(RowEncoder(reducedPartitionsDF.schema.add("__sum", LongType)))
  }

  private def printChunksStats(chunkedDF: DataFrame): Unit = {
    val spark = chunkedDF.sparkSession
    import spark.implicits._

    chunkedDF.explain()

    val chunks = chunkedDF.select($"__chunk").distinct().count()
    logger.info("number of chunks: " + chunks)
    logger.info("chunks distribution:")
    val partitions: Array[(Long, Int)] = chunkedDF.rdd.mapPartitions { iter =>
      val list = iter.toList
      val (sum, len) = list.map { row => (row.getAs[Int](SIZE_COLUMN)) }.foldLeft((0L, 0)) { case ((sum, length), size) =>
        (sum + size, length + 1)
      }

      Iterator((sum, len))
    }.collect
    partitions.foreach { case (sum, len) => logger.info(s"partition size, length: [$sum, $len]") }

  }
}
