/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.paypal.dione.spark.execution

import com.paypal.dione.kvstorage.hadoop.avro.AvroHashBtreeStorageFolderReader
import com.paypal.dione.spark.Dione
import com.paypal.dione.spark.index.IndexManager
import com.paypal.dione.spark.sql.catalyst.catalog.HiveIndexTableRelation
import org.apache.avro.util.Utf8
import org.apache.hadoop.hive.ql.metadata.{Partition => HivePartition}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.hive._
import org.apache.spark.sql.types.{BooleanType, DataType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{Partition, TaskContext}


import scala.collection.JavaConverters._
import scala.collection.JavaConversions._


/**
 * copied and changed from HiveTableScanExec
 */
case class IndexBtreeFetchExec(
                               requestedAttributes: Seq[Attribute],
                               indexRelation: HiveIndexTableRelation,
                               idx: IndexManager,
                               partitionPruningPred: Seq[Expression],
                               dataFilters: Seq[Expression],
                               keys: Seq[Literal])
  extends LeafExecNode with CastSupport {

  def sparkSession = Dione.getContext.spark

  override def nodeName: String = s"Fetch From Index ${indexRelation.tableMeta.qualifiedName}"

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def producedAttributes: AttributeSet = outputSet ++
    AttributeSet(partitionPruningPred.flatMap(_.references))

  private val originalAttributes = AttributeMap(indexRelation.output.map(a => a -> a)
    ++ indexRelation.hiveDataTableRelation.output.map(a => a -> a))

  override val output: Seq[Attribute] = {
    // Retrieve the original attributes based on expression ID so that capitalization matches.
    requestedAttributes.map(originalAttributes)
  }

  // Bind all partition key attribute references in the partition pruning predicate for later
  // evaluation.
  private lazy val boundPruningPred = partitionPruningPred.reduceLeftOption(And).map { pred =>
    require(pred.dataType == BooleanType,
      s"Data type of predicate $pred must be ${BooleanType.catalogString} rather than " +
        s"${pred.dataType.catalogString}.")

    BindReferences.bindReference(pred, indexRelation.partitionCols)
  }

  @transient private lazy val hiveQlTable = SparkSqlHiveUtils.toHiveTable(indexRelation.tableMeta)

  private def castFromString(value: String, dataType: DataType) = {
    cast(Literal(value), dataType).eval(null)
  }

  /**
   * Prunes partitions not involve the query plan.
   *
   * @param partitions All partitions of the relation.
   * @return Partitions that are involved in the query plan.
   */
  private def prunePartitions(partitions: Seq[HivePartition]) = {
    boundPruningPred match {
      case None => partitions
      case Some(shouldKeep) => partitions.filter { part =>
        val dataTypes = indexRelation.partitionCols.map(_.dataType)
        val castedValues = part.getValues.asScala.zip(dataTypes)
          .map { case (value, dataType) => castFromString(value, dataType) }

        // Only partitioned values are needed here, since the predicate has already been bound to
        // partition key attribute references.
        val row = InternalRow.fromSeq(castedValues)
        shouldKeep.eval(row).asInstanceOf[Boolean]
      }
    }
  }

  // exposed for tests
  @transient lazy val rawPartitions = {
    val prunedPartitions =
      if (sparkSession.sessionState.conf.metastorePartitionPruning &&
          partitionPruningPred.size > 0) {
        // Retrieve the original attributes based on expression ID so that capitalization matches.
        val normalizedFilters = partitionPruningPred.map(_.transform {
          case a: AttributeReference => originalAttributes(a)
        })
        sparkSession.sessionState.catalog.listPartitionsByFilter(
          indexRelation.tableMeta.identifier,
          normalizedFilters)
      } else {
        sparkSession.sessionState.catalog.listPartitions(indexRelation.tableMeta.identifier)
      }
    prunedPartitions.map(SparkSqlHiveUtils.toHivePartition(_, hiveQlTable))
  }

  protected override def doExecute(): RDD[InternalRow] = {

    new RDD[InternalRow](Dione.getContext.spark.sparkContext, Nil) {
      override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
        val idxSplit = split.asInstanceOf[IndexPartition]
        val avroHashBtreeFolderReader = AvroHashBtreeStorageFolderReader(idxSplit.path)

        val blahUTF2 = (a: Any) => a match {
          case x: UTF8String => x.toString
          case x => x
        }

        val valueIter = avroHashBtreeFolderReader.getIterator(keys.map(k => blahUTF2(k.value)))

        val blahUTF8 = (a: Any) => a match {
          case x: Utf8 => UTF8String.fromString(x.toString)
          case x: String => UTF8String.fromString(x)
          case x => x
        }

        val cnvrt = UnsafeProjection.create(requestedAttributes, requestedAttributes)
        val idxRequestedFields = requestedAttributes.map(_.name)
          .filter(idx.moreFields.contains)

        val partitionMap = (indexRelation.partitionCols zip idxSplit.prtValues).map(p => p._1.name -> p._2).toMap

        val keysMap = (idx.keys zip keys).map(x => x._1 -> x._2.value).toMap

        val dataFieldsMap = indexRelation.hiveDataTableRelation.schema.fields.map(f => f.name -> f).toMap
        val allButDataFieldsSet = (idxRequestedFields ++ partitionMap.keys ++ keysMap.keys).toSet
        val dataRequestedFields = requestedAttributes.map(_.name)
          .filterNot(f => allButDataFieldsSet.contains(f))
        val dataSchema = dataRequestedFields.map(f => dataFieldsMap(f))

        valueIter.map(indexGR => {
          val dataMap = idx.sparkIndexer.readPayload(indexGR, StructType(dataSchema))

          val retSeq = requestedAttributes.map(_.name).map {
            case f if idx.moreFields.contains(f) => indexGR.get(f)
            case f if keysMap.contains(f) => keysMap(f)
            case f if partitionMap.contains(f) => partitionMap(f)
            case f if dataMap.contains(f) => dataMap(f)
            case f => throw new RuntimeException(s"Field $f not found")
          }.map(blahUTF8)

          val c = cnvrt(InternalRow.fromSeq(retSeq))
          c
        })
      }

      override protected def getPartitions: Array[Partition] = {
        val pp = prunePartitions(rawPartitions)
        pp.zipWithIndex.map(p => IndexPartition(p._2, p._1.getValues.toList, p._1.getLocation)).toArray
      }
    }
  }

}

case class IndexPartition(index: Int, prtValues: List[String], path: String) extends Partition
