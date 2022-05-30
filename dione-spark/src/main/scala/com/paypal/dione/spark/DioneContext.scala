package com.paypal.dione.spark

import com.paypal.dione.spark.index.{IndexManager, IndexSpec}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

case class DioneContext(spark: SparkSession) {

  val indices: mutable.Map[String, Seq[IndexManager]] = mutable.HashMap()

  def addIndex(indexManager: IndexManager): Unit = {
    indices.put(indexManager.dataTableName,
      indices.getOrElse(indexManager.dataTableName, Seq()) ++ Seq(indexManager))
  }


}
