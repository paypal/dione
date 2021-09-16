package com.paypal.dione.spark.avro.btree

import com.paypal.dione.avro.hadoop.file.AvroBtreeFile
import com.paypal.dione.kvstorage.hadoop.avro.AvroHashBtreeStorageFolderReader
import com.paypal.dione.spark.index.IndexManagerUtils
import org.apache.avro.generic.GenericData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.Partitioner
import org.apache.spark.sql.avro.AvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{array, col, expr}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * This object contains Spark APIs that leverage our special key-value file - AvroBtreeFile
 */
object SparkAvroBtreeUtils {

  private val logger = LoggerFactory.getLogger(this.getClass)
  val KEY_HASH_COLUMN = "keyhash"
  private val PARTITION_HASH_COLUMN = "prthash"

  /**
   * Save a Spark DataFrame as AvroBtreeFiles, so later we'll be able to leverage that for a few use-cases.
   * For example:
   * 1. fetching a specific key from a specific partition directly (without Spark) in a second.
   * 2. join another DF with the AvroBtree table without shuffling it as the data is already in order.
   *
   * @param df               DataFrame we want to save. typically an index data.
   * @param keys             the key column names
   * @param folderName       base folder to write the data to
   * @param numFilesInFolder number of files in each folder
   *
   * // btree related params per resulted file
   * @param interval size of each block of the b-tree
   * @param height   number of levels (how deep)
   * @param mode     whether to override or to append
   */
  def writeDFasAvroBtree(df: DataFrame, keys: Seq[String], folderName: String,
                         numFilesInFolder: Int, interval: Int, height: Int,
                         mode: String = "overwrite"
                        )(implicit spark: SparkSession): Unit = {
    writePartitionedDFasAvroBtree(df, keys, folderName, interval, height, Seq((Nil, numFilesInFolder)), mode)
  }

  /**
   * Generally, same as above but for partitioned data.
   * @param partitionsSpec static list of partition specs, each composed of a list of (key -> value) and number of
   *                       files to create for this partition.
   */
  def writePartitionedDFasAvroBtree(df: DataFrame, keys: Seq[String], folderName: String,
                                    interval: Int, height: Int,
                                    partitionsSpec: Seq[(Seq[(String, String)], Int)],
                                    mode: String = "overwrite"
                                   )(implicit spark: SparkSession): Unit = {

    if (partitionsSpec.isEmpty)
      return

    val keysSet = keys.toSet
    val partitionKeys = partitionsSpec.flatMap(_._1.map(_._1)).distinct
    val remainingColumns = df.columns.filterNot(c => keysSet.contains(c) || partitionKeys.contains(c))

    logger.info("writing index file to " + folderName + s" with interval: $interval, height: $height," +
      s" partitionsSpec: $partitionsSpec")

    val repartitionedDF = customRepartition(df, keys, partitionsSpec)

    repartitionedDF
      .write
      .partitionBy(partitionKeys:_*)
      .mode(mode)
      .format("com.paypal.dione.spark.avro.btree")
      .option("key.fields", keys.mkString(","))
      .option("value.fields", remainingColumns.mkString(","))
      .option("btree.interval", interval)
      .option("btree.height", height)
      .save(folderName)
  }

  /**
   * special join function to join DataFrame with avroBtreeTable. The main idea is to avoid the shuffle of the
   * `avroBtreeTable` as it is managed and thus already shuffled and sorted.
   */
  def join(avroBtreeTable: String, keys: Seq[String], dsDF: DataFrame): DataFrame = {
    // TODO: need to add some assertions - keys types, names etc.
    // TODO: also that `avroBtreeTable` is indeed avro btree

    // get table and partition information of `avroBtreeTable` - schema, locations,
    // number of files per partition, etc.
    val spark = dsDF.sparkSession

    // filter only partitions that exist both in the DS and in the index
    val dsPartitionsSpec = IndexManagerUtils.getTablePartitions(avroBtreeTable, dsDF, spark)
    val indexPartitionsSpec = IndexManagerUtils.getTablePartitions(avroBtreeTable, spark.table(avroBtreeTable), spark)
    val partitionsSpec = dsPartitionsSpec.intersect(indexPartitionsSpec)
    val partitionFilter = partitionsSpec.map(s => s.map(p => p._1 + "='" + p._2 + "'").mkString("(", " and ", ")")).mkString(" or ")
    val filteredDsDf = dsDF.where("1=1 and " + partitionFilter)

    val showTable = spark.sql(s"desc formatted $avroBtreeTable").collect()
    val tableLocationStr = showTable.find(row => row.getString(0).contains("Location")).get.getString(1)
    val partitionSpecWithFolders = partitionsSpec.map(l => (l, tableLocationStr + "/" + l.map(p => p._1 + "=" + p._2).mkString("/")))

    val partitionLocations = partitionSpecWithFolders.map(f => (f._1.toMap, f._2))
    val foldersArr = IndexManagerUtils.listFilesDF(partitionLocations, spark).groupBy("path").count()
      .collect().map(r => (r.getString(0), r.getLong(1)))
    if (foldersArr.isEmpty)
      return spark.emptyDataFrame

    val tmpMap = foldersArr.toMap
    val partitionsSpecWithNumFiles = partitionSpecWithFolders.map(s => (s._1, tmpMap(s._2).toInt))

    val partitionKeys = partitionsSpec.headOption.map(_.map(_._1)).getOrElse(Nil)

    val indexTableValueSchema = spark.table(avroBtreeTable)
      .drop(keys:_*)
      .drop(AvroBtreeFile.METADATA_COL_NAME)
      .drop(partitionKeys:_*)
      .schema
    val outputSchema = StructType(dsDF.schema ++ indexTableValueSchema)

    // repartition the dsDF using our customRepartition to get the matching partitions
    val repartitionedDF = SparkAvroBtreeUtils.customRepartition(filteredDsDf, keys, partitionsSpecWithNumFiles)

    // on-the-fly join between the matching partitions. both sides are sorted, so we just "merge-join" them
    val joinedDF = repartitionedDF.mapPartitions((it: Iterator[Row]) =>
      if (!it.hasNext)
        Iterator.empty
      else {
        val bufIt = it.buffered
        val row = bufIt.head
        val folder = tableLocationStr + "/" + partitionKeys.map(pk => (pk, row.getAs[String](pk))).map(p => p._1 + "=" + p._2).mkString("/")
        val fs = new Path(folder).getFileSystem(new Configuration)
        if (!fs.exists(new Path(folder)))
          Iterator.empty
        else {

          val avroBtreeStorageFileReader = AvroHashBtreeStorageFolderReader(folder).getFile(keys.map(row.getAs[Any]))
          val kvIt = avroBtreeStorageFileReader.getIterator().buffered
          val converter = new AvroDeserializer(avroBtreeStorageFileReader.fileReader.getValueSchema, indexTableValueSchema)

          bufIt.flatMap(row => {
            val key1GR = avroBtreeStorageFileReader.toGR(keys.map(row.getAs[Any]))
            var cmp = -100
            while (cmp<0 && kvIt.hasNext) {
              val head = kvIt.head
              cmp = GenericData.get.compare(head._1, key1GR, avroBtreeStorageFileReader.fileReader.getKeySchema)
              logger.debug("comparing file key {} with DS key {} and got {}", head._1, key1GR, cmp+"")
              if (cmp < 0)
                kvIt.next()
            }
            if (!kvIt.hasNext || cmp>0)
              Iterator.empty
            else {
              val nxt = kvIt.head
              // taking the dsDF's data without the last two fields (keyhash, prthash)
              // and the value record from the avro-btree file
              Iterator(Row.fromSeq(row.toSeq.slice(0, row.size - 2) ++
                converter.deserialize(nxt._2).asInstanceOf[InternalRow].toSeq(indexTableValueSchema).map {
                  case f: UTF8String => f.toString
                  case f => f
                }))
            }
          })
        }
      })(RowEncoder(outputSchema))
    joinedDF
  }

  /**
   * This is a custom partitioner. shuffles the data according to the keys and partition keys.
   * Each partitionSpec contains the number of files its rows will be shuffled to, and each file will contain
   * only keys with the same hash value. Also, files will be sorted by the keys.
   *
   * @param partitionsSpec static partitions definition. each entry contain the (key->value) list of a specific
   *                       partition and number of files the partition's folder.
   * @return repartitioned DataFrame where each partition contains only rows of the same hashkey and partition.
   */
  def customRepartition(df: DataFrame, keys: Seq[String],
                        partitionsSpec: Seq[(Seq[(String, String)], Int)]): DataFrame = {

    val dupPartitionValues = partitionsSpec.map(_._1).groupBy(identity).map(t => (t._1, t._2.size)).filter(_._2 > 1).keys
    assert(dupPartitionValues.isEmpty, "Found duplicated partitions: " + dupPartitionValues)

    val partitionKeys = partitionsSpec.flatMap(_._1.map(_._1)).distinct

    val spark = df.sparkSession

    val hashudf = spark.udf.register("hashudf",
      AvroHashBtreeStorageFolderReader.hashModuloUdf(_: mutable.WrappedArray[String], _: Int))

    // static map from partition values to (num_of_files, cumsum_num_of_files)
    var s = 0
    val prtsIndex: Map[Seq[String], (Int, Int)] = partitionsSpec.map(sq => {
      val sqMap = sq._1.toMap
      (partitionKeys.map(sqMap), sq._2)
    }).zipWithIndex.map(t => {s+=t._1._2; (t._1._1, (t._1._2, s-t._1._2))}).toMap

    val prtudf = spark.udf.register("prtudf", (prtSpec: mutable.WrappedArray[String]) =>
      prtsIndex.getOrElse(prtSpec, (0,0)))

    // for each "static" partition, we would like to have numFilesInPartition files.
    // and because each task creates one file, we want (numFilesInPartition * numOfPartitions) tasks.
    // moreover, we want to decide which rows are in each task, so in the reader we would know in which
    // file to find every key.
    val dfWithHashes = df
      .withColumn(PARTITION_HASH_COLUMN, prtudf(array(partitionKeys.map(col): _*)))
      .withColumn(KEY_HASH_COLUMN, hashudf(array(keys.map(col): _*), expr("prthash._1")))

    val customPartitionedRDD = dfWithHashes
      .rdd
      .map(row => ((row.getAs[Int](KEY_HASH_COLUMN), row.getAs[Row](PARTITION_HASH_COLUMN).get(1)), row))
      .partitionBy(new Partitioner {
        override def numPartitions: Int = prtsIndex.values.map(_._1).sum

        override def getPartition(key: Any): Int = {
          val (hashKeyMod, prtCumSum) = key.asInstanceOf[(Int, Int)]
          prtCumSum + hashKeyMod
        }

      })

    spark.createDataFrame(customPartitionedRDD.map(_._2), dfWithHashes.schema)
      .sortWithinPartitions((partitionKeys ++ keys).map(col):_*)
  }
}
