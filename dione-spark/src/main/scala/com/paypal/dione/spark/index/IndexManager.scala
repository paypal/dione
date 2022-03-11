package com.paypal.dione.spark.index

import com.paypal.dione.hdfs.index.HdfsIndexContants.{FILE_NAME_COLUMN, OFFSET_COLUMN, SIZE_COLUMN, SUB_OFFSET_COLUMN}
import com.paypal.dione.kvstorage.hadoop.avro.AvroHashBtreeStorageFolderReader
import com.paypal.dione.spark.avro.btree.SparkAvroBtreeUtils
import com.paypal.dione.spark.index.IndexManager.PARTITION_DEF_COLUMN
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class IndexSpec(dataTableName: String, indexTableName: String,
                     keys: Seq[String], moreFields: Seq[String] = Nil)

object IndexManager {

  val PARTITION_DEF_COLUMN = "partition_def"

  val indexSchema: StructType = StructType(Seq(
    StructField(FILE_NAME_COLUMN, StringType),
    StructField(OFFSET_COLUMN, LongType),
    StructField(SUB_OFFSET_COLUMN, IntegerType),
    StructField(SIZE_COLUMN, IntegerType)
  ))

  /**
   * Creates a new index table on a data table for the given keys.
   *
   * @param indexSpec - all relevant defining properties
   * @param spark
   * @return
   */
  def createNew(indexSpec: IndexSpec)(implicit spark: SparkSession): IndexManager = {

    // TODO: assert index table doesn't exist
    // TODO: match on supported data type (avro/seq)
    // TODO: allow passing explicit data type

    // resolve data type if not passed explicitly
    val indexManager = IndexManagerUtils.createIndexManager(spark, indexSpec)

    IndexManagerUtils.initNewIndexTable(spark, indexSpec)

    indexManager
  }

  /**
   * Load an already defined index from an index table.
   *
   * @param indexTableName - table which holds index data and metadata.
   * @param spark
   * @return
   */
  def load(indexTableName: String)(implicit spark:SparkSession): IndexManager = {
    val (db, table) =  IndexManagerUtils.getTableName(spark, indexTableName)
    val tblProperties = spark.sessionState.catalog.externalCatalog.getTable(db, table).properties

    val dataTableName = tblProperties("index.meta.dataTableName")
    val keys = tblProperties("index.meta.keys").split("\\|")
    val moreFields = tblProperties("index.meta.moreFields").split("\\|").filterNot(_.isEmpty)
    val indexSpec = IndexSpec(dataTableName, indexTableName, keys, moreFields)

    // TODO - add the manager class to the table metadata, and pass explicitly here:
    val indexManager: IndexManager = IndexManagerUtils.createIndexManager(spark, indexSpec)
    indexManager
  }
}

/**
 * Top level api for manage and maintain index for 3rd party table, of any format.
 * Instance of this class is bound to specific source table and index table.
 * The index table will contain the index data, and also some other metadata (for any need).
 *
 * This API is agnostic to the source table file format.
 *
 * This API uses Spark to handle:
 * 1. metadata - for the index and for the tables we would like to index.
 * 2. distribution - most of the hard work is done per file by the HdfsIndexer and the KVStorageFile, we leverage
 *                   Spark only to distribute their work.
 *
 */
case class IndexManager(@transient val spark: SparkSession, sparkIndexer: SparkIndexer,
                        indexSpec: IndexSpec) extends Serializable {

  val IndexSpec(dataTableName, indexTableName, keys, moreFields) = indexSpec

  lazy val indexFolder: String = {
    val descFormattedIndex = spark.sql(s"desc formatted $indexTableName").collect()
    descFormattedIndex.find(row => row.getString(0).contains("Location")).get.getString(1)
  }

  /**
   *
   * @return partition definitions that exist in dataTableName and not in indexTableName
   */
  def getMissingPartitions(): Seq[Seq[(String, String)]] = {
    IndexManagerUtils.getMissingPartitions(dataTableName, indexTableName)(spark)
  }

  /**
   * index all partitions that are in dataTableName and not yet in indexTableName
   */
  def appendMissingPartitions(): Unit = {
    val missingPrts = getMissingPartitions()
    if (missingPrts.nonEmpty)
      appendNewPartitions(missingPrts)
  }

   /**
   * Index new partitions from the source data table
   *
   * @param partitionsSpec the new partitions to index
   */
  def appendNewPartitions(partitionsSpec: Seq[Seq[(String, String)]]): Unit = {
    val partitionLocations = IndexManagerUtils.getPartitionLocations(dataTableName, partitionsSpec, spark)

    val fileLikeExp = spark.conf.get("index.manager.file.filter", "%")

    val filesDF = IndexManagerUtils.listFilesDF(partitionLocations, spark).where(s"file like '$fileLikeExp'")
      .repartition(col("path"), col("file"), col("start_position"))

    val (numParts, indexInterval, height) = IndexManagerUtils.calcBtreeProperties(filesDF, partitionsSpec.length,
      indexer = createIndexDFInternal(_).drop("path", "file", "start_position", "end_position"))

    // we manually drop partitions and afterwards create them with "append" mode because in "overwrite" mode Spark
    // deleted other partitions. e.g "overwriting" dt=2021-01-01/seq=01 will cause deletion of dt=2021-01-01/seq=00
    val partitionsToDrop = partitionsSpec.map(partitionSpec => {
      partitionSpec.map(p => p._1 + "='" + p._2 + "'").mkString("partition (", ",", ")")
    }).mkString(", ")
    spark.sql(s"alter table $indexTableName drop if exists "+ partitionsToDrop)

    val indexedDF = IndexManagerUtils.toSafeDataFrame(createIndexDFInternal(filesDF))
      .drop("path", "file", "start_position", "end_position")
    val partitionKeys = partitionsSpec.flatMap(_.map(_._1)).distinct
    val indexWithPrtCols = partitionKeys.foldLeft(indexedDF){ case (df,prtKey) =>
      df.withColumn(prtKey, expr(PARTITION_DEF_COLUMN+s"['$prtKey']"))}
      .drop(PARTITION_DEF_COLUMN)

    val partitionsSpecWithNumParts = partitionsSpec.map(t => (t, numParts))
    SparkAvroBtreeUtils.writePartitionedDFasAvroBtree(indexWithPrtCols, keys, indexFolder,
      indexInterval, height, partitionsSpecWithNumParts, "append")(spark)

    spark.sql("msck repair table " + indexTableName)
    spark.sql(s"alter table $indexTableName set TBLPROPERTIES ('avro.schema.url'='$indexFolder/.btree.avsc')")
    filesDF.unpersist()
  }

  private def getPartitionFolder(partitionSpec: Seq[(String, String)]) = {
    partitionSpec.map(p => p._1 + "=" + p._2).mkString("/")
  }

  private def createIndexDFInternal(filesDF: DataFrame): DataFrame = {
    val fieldsSchema: StructType = spark.table(dataTableName).select((keys ++ moreFields).map(col): _*).schema
    sparkIndexer.createIndexDF(filesDF, fieldsSchema)
  }

  /**
   * Get the entire index table as a DataFrame
   */
  def getIndex(): DataFrame = spark.table(indexTableName)

  /**
   * Fetch a single data record given a key and specific partition to search in
   *
   * @param key  record key
   * @return The Record as Map
   */
  def fetch(key: Seq[Any], partitionSpec: Seq[(String, String)], fields: Option[Seq[String]] = None): Option[Map[String, Any]] = {
    val valueIter = fetchAll(key, partitionSpec, fields)
    if (valueIter.hasNext)
      Some(valueIter.next())
    else None
  }

  /**
   * Fetch a single data record given a key and specific partition to search in
   *
   * @param key  record key
   * @return The Record as Map
   */
  def fetchAll(key: Seq[Any], partitionSpec: Seq[(String, String)], fields: Option[Seq[String]] = None): Iterator[Map[String, Any]] = {
    val partitionFolder = indexFolder + "/" + getPartitionFolder(partitionSpec)
    val avroHashBtreeFolderReader = AvroHashBtreeStorageFolderReader(partitionFolder)
    val valueIter = avroHashBtreeFolderReader.getIterator(key)
    valueIter.map(readPayload(_, fields))
  }

  /**
   * Read data given needed specific metadata properties.
   *
   * @param indexGR
   * @return
   */
  def readPayload(indexGR: GenericRecord, fields: Option[Seq[String]] = None): Map[String, Any] = {
    sparkIndexer.readPayload(indexGR, getStructTypeFromFields(fields))
  }

  def joinWithIndex(dsDF: DataFrame): DataFrame = {
    SparkAvroBtreeUtils.join(indexTableName, indexSpec.keys, dsDF)
  }

  /**
   * Load data from the source data table using the index.
   * the index is usually manipulated first - like filtered or joined with a driver-set.
   *
   * @param index DataFrame returned from getIndex(), probably filtered
   */
  def loadByIndex(index: DataFrame, fields: Option[Seq[String]] = None): DataFrame = {
    sparkIndexer.loadByIndex(index, getStructTypeFromFields(fields))
  }

  private def getStructTypeFromFields(fields: Option[Seq[String]] = None): StructType = {
    val dataTable = spark.table(dataTableName)

    fields
      .map(keys ++ _)
      .map(cols => dataTable.select(cols.map(col): _*).schema)
      .getOrElse(getAllDataFields())
  }

  private def getAllDataFields(): StructType = {
    val sparCatalogDataTable = IndexManagerUtils.getSparkCatalogTable(spark, dataTableName)
    val schemaWithoutPartitionCols = spark.table(dataTableName).drop(sparCatalogDataTable.partitionColumnNames: _*).schema
    schemaWithoutPartitionCols
  }

}

trait IndexManagerFactory {
  def canResolve(inputFormat: String, serde: String): Boolean

  def createSparkIndexer(spark: SparkSession, indexSpec: IndexSpec): SparkIndexer
}