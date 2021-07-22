package com.paypal.dione.spark.avro.btree

import java.io.BufferedOutputStream

import com.databricks.spark.avro.SchemaConverters
import com.paypal.dione.avro.hadoop.file.AvroBtreeFile
import com.paypal.dione.avro.hadoop.file.AvroBtreeFile.Writer
import com.paypal.dione.kvstorage.hadoop.avro.AvroHashBtreeStorageFolderReader.AVRO_BTREE_SCHEMA_FILENAME
import com.paypal.dione.spark.avro.SparkAvroUtils.createConverterToAvro
import org.apache.avro.SchemaBuilder
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

class AvroBtreeOutputWriter( path: String,
                             context: TaskAttemptContext,
                             schema: StructType,
                             jobOptions: AvroBtreeJobOptions
                           ) extends OutputWriter {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val keysSchema = StructType(schema.fields.filter(f => jobOptions.keyFields.contains(f.name)))
  private val keysAvroSchema = createAvroSchema(keysSchema, jobOptions.keyRecordName)

  private val valuesSchema = StructType(schema.fields.filter(f => jobOptions.valueFields.contains(f.name)))
  private val valuesAvroSchema = createAvroSchema(valuesSchema, jobOptions.valueRecordName)

  def rowToGenericRecord(iRow: InternalRow, fields: Seq[String], converter: Any => Any) = {
    val row = internalRowConverter(iRow)
    converter(Row.fromSeq(fields.map(row.getAs[Any]))).asInstanceOf[GenericRecord]
  }

  private lazy val internalRowConverter =
    CatalystTypeConverters.createToScalaConverter(schema).asInstanceOf[InternalRow => Row]

  val keysConverter = createConverterToAvro(keysSchema, "key", jobOptions.recordNamespace)
  private lazy val rowKeyConverter = (row:InternalRow) => rowToGenericRecord(row, jobOptions.keyFields, keysConverter)

  val valuesConverter = createConverterToAvro(valuesSchema, "value", jobOptions.recordNamespace)
  private lazy val rowValueConverter = (row:InternalRow) => rowToGenericRecord(row, jobOptions.valueFields, valuesConverter)

  var writer: Writer = {
    val codecFactory = CodecFactory.fromString(jobOptions.compression)
//    val metrics: AvroBtreeWriter.TaskMetrics = createSparkOutputMetrics

    val avroBtreeFileOptions = new Writer.Options()
      .withKeySchema(keysAvroSchema)
      .withValueSchema(valuesAvroSchema)
      .withConfiguration(new Configuration())
      .withInterval(jobOptions.interval)
      .withHeight(jobOptions.height)
      .withCodec(codecFactory)
      .withPath(new Path(path))

    new AvroBtreeFile.Writer(avroBtreeFileOptions)

  }

  override def write(row: InternalRow): Unit = {
    val keyRecord = rowKeyConverter(row)
    val valueRecord = rowValueConverter(row)

    writer.append(keyRecord, valueRecord)
  }

  override def close(): Unit = {
    writer.close()
    //AvroBtreeDataSource.Commit(options)
    // ugly work-around to write the schema only once
    if (new Path(path).getName.startsWith("part-00000"))
      createSchemaFile()
  }

  private def createSchemaFile() = {

    val schemaFilePath = new Path(jobOptions.basePath, AVRO_BTREE_SCHEMA_FILENAME)
    logger.debug("writing avro schema file to: " + schemaFilePath + AVRO_BTREE_SCHEMA_FILENAME)

    val keysSchema = StructType(schema.fields.filter(f => jobOptions.keyFields.contains(f.name)))
    val valuesSchema = StructType(schema.fields.filter(f => jobOptions.valueFields.contains(f.name)))

    val avroKeysSchema = SchemaConverters.convertStructToAvro(keysSchema,
      SchemaBuilder.record(jobOptions.keyRecordName), jobOptions.recordNamespace)
    val avroValuesSchema = SchemaConverters.convertStructToAvro(valuesSchema,
      SchemaBuilder.record(jobOptions.valueRecordName), jobOptions.recordNamespace)

    val recordSchema = AvroBtreeFile.createSchema(avroKeysSchema, avroValuesSchema)
    val fs = schemaFilePath.getFileSystem(new Configuration())
    val schemaFileWriter = new BufferedOutputStream(fs.create(schemaFilePath))
    schemaFileWriter.write(recordSchema.toString(true).getBytes)
    schemaFileWriter.close()
  }

//  override def abort(): Unit = {
//    val fs = FileSystem.get(new Configuration())
//    Seq(options.staging).foreach(fs.delete(_, true))
//  }

  private def createAvroSchema(_schema: StructType, recordName: String) = {
    val builder = SchemaBuilder.record(recordName)
    SchemaConverters.convertStructToAvro(_schema, builder, jobOptions.recordNamespace)
  }

//  private def createSparkOutputMetrics = {
//    new AvroBtreeWriter.TaskMetrics {
//      val sparkMetrics = KVMetrics.getWriterMetricsForTask
//
//      override def incBytes(b: lang.Long): Unit = sparkMetrics.incBytes(b)
//
//      override def incRecords(b: lang.Long): Unit = sparkMetrics.incRecords(b)
//    }
//  }

}

