package com.paypal.dione.spark

import com.paypal.dione.spark.index.IndexSpec
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

case class DioneContext(spark: SparkSession) {

  val indices: mutable.Map[String, Seq[IndexSpec]] = mutable.HashMap()

  def addIndex(indexSpec: IndexSpec): Unit = {
    indices.put(indexSpec.dataTableName,
      indices.getOrElse(indexSpec.dataTableName, Seq()) ++ Seq(indexSpec))
  }


}
