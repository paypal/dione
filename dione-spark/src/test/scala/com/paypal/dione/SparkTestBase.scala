package com.paypal.dione

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.jupiter.api.BeforeAll

import scala.collection.JavaConversions._

object SparkTestBase {
  def dfToStr(df: DataFrame, fields: Option[Seq[String]]=None) =
    df.select(fields.getOrElse(df.columns.toSeq).map(col):_*).collectAsList().sortBy(_.toString).mkString("\n")
}

trait SparkTestBase {

  lazy val spark: SparkSession = {
    val sparkConf = new SparkConf().setMaster(s"local[1]").set("spark.sql.shuffle.partitions", "3").setAppName("Spark Unit Test")
    val builder = SparkSession.builder().config(sparkConf).enableHiveSupport()
    val spark = builder.getOrCreate()
    // TODO really needed?
    spark.conf.set("indexer.sampler.files.rate", "1.0")
    spark
  }

  lazy val hadoopConf = spark.sparkContext.hadoopConfiguration
  lazy val fs: FileSystem = FileSystem.get(hadoopConf)
}

trait SparkWithTestFolder extends SparkTestBase {

  lazy val cleaner = {
    if (fs.exists(new Path(baseTestPath)))
      fs.delete(new Path(baseTestPath), true)
  }

  @BeforeAll
  def cleanTestFolder(): Unit = {
    cleaner
  }

  val baseTestPath: String
}

trait SparkCleanTestDB extends SparkWithTestFolder {

  @BeforeAll
  def cleanTestDB(): Unit = {
    cleanTestFolder()

    spark.sql(s"drop database if exists $dbName CASCADE")
    spark.sql(s"create database $dbName LOCATION '$baseTestPath/hive/'")
    spark.sql(s"use $dbName")

  }

  val dbName: String
}