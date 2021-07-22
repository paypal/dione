package com.databricks.spark.avro.dione

import com.databricks.spark.avro.SchemaConverters
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.DataType

/**
 * Spark-2.3 workaround to access databricks converter
 */
object AvroToSqlConverter {
  def getNewRecordNamespace = SchemaConverters.getNewRecordNamespace _
}

case class AvroToSqlConverter(sourceAvroSchema: Schema, targetSqlType: DataType) {
  val func = SchemaConverters.createConverterToSQL(sourceAvroSchema, targetSqlType)

  def convert(record: GenericRecord): GenericRow = func(record).asInstanceOf[GenericRow]
}