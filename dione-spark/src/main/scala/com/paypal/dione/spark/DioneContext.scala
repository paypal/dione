package com.paypal.dione.spark

import com.paypal.dione.spark.index.{IndexManager, IndexSpec}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier

import scala.collection.mutable

case class DioneContext(spark: SparkSession) {

  val indices: mutable.Map[String, Seq[IndexManager]] = mutable.HashMap()

  def getIndices(tableIdentifier: TableIdentifier): Seq[IndexManager] = {
    indices.getOrElse(tableIdentifier.database.getOrElse(spark.catalog.currentDatabase) +
      "." + tableIdentifier.identifier, Nil)
  }

  def addIndex(indexManager: IndexManager): Unit = {
    indices.put(indexManager.dataTableName,
      indices.getOrElse(indexManager.dataTableName, Seq()) ++ Seq(indexManager))
  }


}
