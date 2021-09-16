package com.paypal.dione.spark.index

import com.paypal.dione.hdfs.index.HdfsIndexContants._
import com.paypal.dione.spark.index.IndexManager.PARTITION_DEF_COLUMN
import com.paypal.dione.spark.index.avro.AvroSparkIndexer
import com.paypal.dione.spark.index.parquet.ParquetSparkIndexer
import com.paypal.dione.spark.index.sequence.SeqFileSparkIndexer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.dione.Metrics
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hive.SerializableConfiguration
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

import java.util.UUID

object IndexManagerUtils {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def getMissingPartitions(tableA: String, tableB: String)(spark: SparkSession): Seq[Seq[(String, String)]] = {
    val dataTablePrts = IndexManagerUtils.getTablePartitions(tableA, spark)
    val indexTablePrts = IndexManagerUtils.getTablePartitions(tableB, spark).toSet
    dataTablePrts.filterNot(prtDef => indexTablePrts.contains(prtDef))
  }

  def getPartitionLocations(tablename: String, partitionsSpec: Seq[Seq[(String, String)]], spark: SparkSession): Seq[(Map[String, String], String)] = {
    partitionsSpec.map(partitionSpec => {
      val staticPartitionFilter = partitionSpec.map(p => p._1 + "='" + p._2 + "'").mkString(",")
      val showPartition = spark.sql(s"desc formatted $tablename partition ($staticPartitionFilter)").collect()
      val prtLocationStr = showPartition.find(row => row.getString(0).contains("Location")).get.getString(1)
      (partitionSpec.toMap, new Path(prtLocationStr).toString)
    })
  }

  def listFilesDF(partitionLocations: Seq[(Map[String, String], String)], spark: SparkSession): DataFrame = {
    import spark.implicits._

    import scala.collection.JavaConversions._

    val partitionLocationsRows = partitionLocations.map(a => Row(a._1, a._2)).toList
    val partitionLocationsDF = spark.createDataFrame(partitionLocationsRows,
      StructType(Seq(StructField(PARTITION_DEF_COLUMN, MapType(StringType, StringType, true)),
        StructField("path", StringType, true))))

    // TODO: change to udf+explode
    val serConf = SerializableConfiguration.broadcast(spark)
    val chunkSize = spark.conf.get("indexer.files.chunkMB", "50").toLong << 20
    val filesDF = partitionLocationsDF
      .repartition(col("path"))
      .flatMap((row: Row) => {
        val path = new Path(row.getAs[String]("path"))

        val conf = serConf.value.value
        val fs = path.getFileSystem(conf)

        val fileStatuses = if (!fs.exists(path)) Seq.empty else fs.listStatus(path).toSeq

        fileStatuses.flatMap { file =>
          val chunks = (file.getLen.doubleValue() / chunkSize.doubleValue()).ceil.intValue()
          (0 until chunks).map { chunkId =>
            val startPos = chunkSize * chunkId
            val endPos = math.min(startPos + chunkSize, file.getLen)
            (row.getAs[Map[String, String]](PARTITION_DEF_COLUMN), path.toString, file.getPath.getName, startPos, endPos)
          }
        }
      }).toDF(PARTITION_DEF_COLUMN, "path", "file", "start_position", "end_position")

    filesDF
  }

  def createIndexDF(filesDF: DataFrame, fieldsSchema: StructType, sparkIndexer: SparkIndexer): DataFrame = {

    val outputSchema = StructType(filesDF.schema ++ fieldsSchema ++ IndexManager.indexSchema)
    implicit val encoder = RowEncoder(outputSchema)

    filesDF.flatMap((row: Row) => {
      val metrics = Metrics.getReaderMetricsForTask
      val filename = row.getAs[String]("path") + "/" + row.getAs[String]("file")
      logger.info("indexing file: " + filename)

      val (start, end) = (row.getAs[Long]("start_position"), row.getAs[Long]("end_position"))
      sparkIndexer.initHdfsIndexer(new Path(filename), new Configuration(), start, end, fieldsSchema)
        .iteratorWithMetadata
        .map { t =>
          metrics.incBytes(t._2.size)
          Row.fromSeq(row.toSeq ++ sparkIndexer.convert(t._1) ++ Seq(t._2.file, t._2.position, t._2.numInBlock, t._2.size))
        }
    })
  }

  def calcBtreeProperties(filesDF: DataFrame, partitionsSpecLength: Int, indexer: DataFrame => DataFrame): (Int, Int, Int) = {
    val spark = filesDF.sparkSession

    val indexInterval = spark.conf.get("index.manager.btree.interval", "1000").toInt
    val userHeight = spark.conf.get("index.manager.btree.height", "-1").toInt
    val userNumParts = spark.conf.get("index.manager.btree.num.parts", "-1").toInt
    if (userHeight>0 && userNumParts>0)
      return (userNumParts, indexInterval, userHeight)

    val sampleRate = spark.conf.get("indexer.sampler.files.rate", "0.05").toDouble
    val (estimatedPartsTotal, estimatedRowsTotal) =
      IndexManagerUtils.sampleFilesAndInferSize(filesDF.drop(PARTITION_DEF_COLUMN), sampleRate, indexer)
    val estimatedParts = (estimatedPartsTotal.doubleValue() / partitionsSpecLength).ceil.intValue()
    val estimatedRows = (estimatedRowsTotal.doubleValue() / partitionsSpecLength).ceil.intValue()
    require(estimatedParts > 0)
    require(estimatedRows > 0)

    val height = if (userHeight <= 0) (math.log(estimatedRows) / math.log(indexInterval)).ceil.intValue()
                 else userHeight

    val numParts = if (userNumParts>0) userNumParts else estimatedParts

    (numParts, indexInterval, height)
  }

   /**
   * Samples some files, index them, examine the output and infer the source row count, and the desired file count
   *
   * @param filesDF the files dataframe
   * @param indexer indexer to use
   * @return tuple - (num output files, estimated num rows)
   */
  private def sampleFilesAndInferSize(filesDF: DataFrame, sampleRate: Double, indexer: DataFrame => DataFrame) = {
    val spark = filesDF.sparkSession
    val sampleFiles = filesDF.sample(sampleRate)
    val reduceShufflePartitions: Int = (spark.conf.get("spark.sql.shuffle.partitions").toDouble * sampleRate).ceil.intValue()
    val sampleIndexDF = indexer(sampleFiles).repartition(reduceShufflePartitions)
    val (numPartsSample, rowCountSample) = IndexManagerUtils.inferSize(sampleIndexDF)
    val estimatedParts = (numPartsSample.doubleValue() / sampleRate).ceil.intValue()
    val estimatedRows = (rowCountSample.doubleValue() / sampleRate).ceil.longValue()
    logger.info(s"estimatedParts: $estimatedParts, estimatedRows: $estimatedRows")
    (estimatedParts, estimatedRows)
  }

  private def inferSize(indexDF: DataFrame) = {
    val spark = indexDF.sparkSession
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    import org.apache.spark.sql.functions._
    import spark.implicits._

    def getPathSize(path: Path) = fs.getContentSummary(path).getSpaceConsumed / fs.getDefaultReplication(path)

    // sample:
    val tmpPath = "/tmp/" + UUID.randomUUID().toString // TODO really in /tmp ?
    val codec = spark.conf.getOption("spark.sql.avro.compression.codec")
    spark.conf.set("spark.sql.avro.compression.codec", "deflate")
    indexDF.write.format("avro").save(tmpPath)
    codec.foreach(spark.conf.set("spark.sql.avro.compression.codec", _))
    try {
      val sample = new {
        val (inputSourceBytes, outputRowCount) = spark.read.format("avro").load(tmpPath).agg(
          sum(SIZE_COLUMN), // infer source size by this column
          count(lit(1))) // simple row count ...
          .as[(Long, Long)].head()
        val outputIndexSize = getPathSize(new Path(tmpPath)).doubleValue()
      }

      // extract stats:
      val targetFileSize = spark.conf.get("index.manager.targetFileSizeMB", "50").toInt << 20
      val numOutputFiles = (sample.outputIndexSize / targetFileSize).ceil.intValue()
      (numOutputFiles, sample.outputRowCount)
    } finally {
      fs.delete(new Path(tmpPath), true)
    }

  }

  def initNewIndexTable(spark: SparkSession, indexSpec: IndexSpec) = {
    val IndexSpec(dataTableName, indexTableName, keys, moreFields) = indexSpec

    // resolve schema
    val cols = keys ++ moreFields
    val df = spark.table(dataTableName).select(cols.head, cols.tail: _*)
    val colsSchema = toSafeDataFrame(df).schema

    val schemaStr = Seq(colsSchema, IndexManager.indexSchema)
      .flatMap(schema => schema.fields.map(field => field.name + " " + field.dataType.typeName)).mkString(", ")

    // resolve partitions' schema
    val partitionsKeys = spark.catalog.listColumns(dataTableName).filter(_.isPartition).collect().map(_.name)
    val partitionsSchema = spark.table(dataTableName).select(partitionsKeys.map(col): _*).schema
    val partitionsSchemaStr = partitionsSchema.fields.map(field => field.name + " " + field.dataType.typeName).mkString(", ")

    val tblproperties = Seq("index.meta.dataTableName" -> dataTableName, "index.meta.keys" -> keys.mkString("|"),
      "index.meta.moreFields" -> moreFields.mkString("|")).map(t => "'" + t._1 + "'='" + t._2 + "'")

    spark.sql(s"create table $indexTableName (" + schemaStr + ") partitioned by (" +
      partitionsSchemaStr + ") stored as avro TBLPROPERTIES (" + tblproperties.mkString(",") + ")")
  }

  def getTablePartitions(tableName: String, spark: SparkSession): Seq[Seq[(String, String)]] = {
    getTablePartitions(tableName, spark.table(tableName), spark)
  }

  def getTablePartitions(keysTableName: String, valuesDF: DataFrame, spark: SparkSession): Seq[Seq[(String, String)]] = {
    val partitionKeys = spark.catalog.listColumns(keysTableName).filter(_.isPartition).collect().map(_.name)
    val partitionValues = valuesDF.selectExpr(partitionKeys:_*).distinct()
    val valuesMap = partitionValues.collect().map(_.getValuesMap[String](partitionKeys)).distinct
    valuesMap.map(_.toSeq).toSeq
  }

  def toSafeDataFrame(df: DataFrame): DataFrame = {

    // databricks avro converts decimal to string. this messes up the sorting.
    // TODO - newer versions of avro converters DO support decimals
    val decimalCols = df.schema.filter { col =>
      col.dataType match {
        case DecimalType() => true
        case _ => false
      }
    }

    val castDecimal = df.sparkSession.conf.get("indexer.castDecimalToDouble", "false").toBoolean
    if (decimalCols.nonEmpty && !castDecimal)
      throw new IllegalArgumentException(s"decimal columns are not supported. " +
        s"set indexer.castDecimalToDouble=true to index the columns as double type")

    logger.warn(s"Converting column(s) to double: ${decimalCols.map(_.name).mkString(", ")}")
    val safeDF = decimalCols.foldLeft(df) { case (df, field) =>
      df.withColumn(field.name, col(field.name).cast("double"))
    }
    safeDF
  }

  def createIndexManager(spark: SparkSession, indexSpec: IndexSpec): IndexManager = {
    val (dataDb, dataTable) = getTableName(spark, indexSpec.dataTableName)

    val storage@(inputFormat, serde) = {
      val storage = spark.sessionState.catalog.externalCatalog.getTable(dataDb, dataTable).storage
      val format = storage.inputFormat.getOrElse(throw new RuntimeException("cannot determine data input format"))
      val serde = storage.serde.getOrElse(throw new RuntimeException("cannot determine data serde"))
      (format, serde)
    }

    val sparkIndexer = Seq(SeqFileSparkIndexer, AvroSparkIndexer, ParquetSparkIndexer)
      .find(_.canResolve(inputFormat, serde))
      .getOrElse(throw new RuntimeException("could not find indexer for data type: " + storage))
      .createSparkIndexer(spark, indexSpec)

    logger.info(s"initialized new IndexManager for table $dataTable with storage $storage: " + sparkIndexer.getClass.getName)
    new IndexManager(spark, sparkIndexer, indexSpec)
  }

  def getTableName(spark: SparkSession, tableName: String): (String, String) = {
    val (db, table) = tableName.split("\\.").toList match {
      case table :: Nil => (spark.catalog.currentDatabase, table)
      case db :: table :: Nil => (db, table)
      case invalid => throw new IllegalArgumentException("invalid table name: " + invalid)
    }
    (db, table)
  }

  def getSparkCatalogTable(spark: SparkSession, fullTableName: String): CatalogTable = {
    val (db, table) = IndexManagerUtils.getTableName(spark, fullTableName)
    val identifier: TableIdentifier = TableIdentifier(table, Some(db))
    //val sparkPartitions = spark.sessionState.catalog.listPartitions(identifier)
    val sparkCatalogTable = spark.sessionState.catalog.getTableMetadata(identifier)
    sparkCatalogTable
  }

}
