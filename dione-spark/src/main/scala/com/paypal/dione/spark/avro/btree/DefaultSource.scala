package com.paypal.dione.spark.avro.btree

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

private class DefaultSource extends FileFormat {
  override def inferSchema(sparkSession: SparkSession, options: Map[String, String],
                           files: Seq[FileStatus]): Option[StructType] = None

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {
    val jobOptions = new AvroBtreeJobOptions(dataSchema, options)
    new AvroBtreeOutputWriterFactory(jobOptions)
  }
}
