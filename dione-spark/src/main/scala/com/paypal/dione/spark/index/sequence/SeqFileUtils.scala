package com.paypal.dione.spark.index.sequence

import java.util.Properties

import com.paypal.dione.hdfs.index.sequence.SequenceFileIndexer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorConverters, StructField, StructObjectInspector}
import org.apache.hadoop.io.Writable
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.hive.SparkSqlHiveUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

object SeqFileUtils {

  /**
   * Used to send serialized info to the executors
   */
  case class SerDeInfo(tableSerde: Class[_ <: Deserializer],
                       partitionSerde: Class[_ <: Deserializer],
                       tableProps: Properties,
                       partitionProps: Properties) extends Serializable {

    def getTableSerde(conf: Configuration): Deserializer = createSerde(tableSerde, tableProps, conf)

    def getPartitionSerde(conf: Configuration): Deserializer = createSerde(partitionSerde, partitionProps, conf)

    private def createSerde(serdeClz: Class[_ <: Deserializer], props: Properties, conf: Configuration) = {
      val serde = serdeClz.newInstance()
      serde.initialize(conf, props)
      serde
    }
  }

  def createDeserializer(serde: SerDeInfo,
                         inputSchema: StructType,
                         outputFields: Seq[String],
                         conf: Configuration): SequenceFileIndexer.Deserializer = {
    val tableSerDe = serde.getTableSerde(conf)
    val partitionSerDe = serde.getPartitionSerde(conf)

    val objectInspector = createObjectInspector(tableSerDe, partitionSerDe)
    val inputHiveSchema = inputSchema.map(_.name).map(objectInspector.getStructFieldRef).toArray
    val inputHiveSchemaWithIndex = inputHiveSchema.map(_.getFieldName).zipWithIndex.toMap
    val outputFieldsWithPosition = outputFields.map(outputField => outputField -> inputHiveSchemaWithIndex(outputField))
    val converter = ObjectInspectorConverters.getConverter(partitionSerDe.getObjectInspector, objectInspector)

    val deserializer = new SequenceFileIndexer.Deserializer {
      override def deserialize(k: Writable, v: Writable): Seq[(String, Any)] = {
        val deserialized = partitionSerDe.deserialize(v)
        val raw = converter.convert(deserialized)
        outputFieldsWithPosition.map { case (name, positionInSchema) =>
          val field = inputHiveSchema(positionInSchema)
          val hiveObject = objectInspector.getStructFieldData(raw, field)
          val unwrapped = unwrap(field, hiveObject) match {
            case utf: UTF8String => utf.toString
            case v => v
          }
          name -> unwrapped
        }
      }
    }
    deserializer
  }


  def createObjectInspector(tableSerDe: Deserializer, partitionsSerDe: Deserializer) = {
    if (partitionsSerDe.getObjectInspector.equals(tableSerDe.getObjectInspector)) {
      partitionsSerDe.getObjectInspector.asInstanceOf[StructObjectInspector]
    } else {
      ObjectInspectorConverters.getConvertedOI(
        partitionsSerDe.getObjectInspector,
        tableSerDe.getObjectInspector).asInstanceOf[StructObjectInspector]
    }
  }

  def unwrap(field: StructField, hiveObject: AnyRef) = {
    field.getFieldObjectInspector match {
      case _ if hiveObject == null => null
      case oi: BooleanObjectInspector => oi.get(hiveObject)
      case oi: ByteObjectInspector => oi.get(hiveObject)
      case oi: ShortObjectInspector => oi.get(hiveObject)
      case oi: IntObjectInspector => oi.get(hiveObject)
      case oi: LongObjectInspector => oi.get(hiveObject)
      case oi: FloatObjectInspector => oi.get(hiveObject)
      case oi: DoubleObjectInspector => oi.get(hiveObject)
      case oi: HiveVarcharObjectInspector => UTF8String.fromString(oi.getPrimitiveJavaObject(hiveObject).getValue)
      case oi: HiveCharObjectInspector => UTF8String.fromString(oi.getPrimitiveJavaObject(hiveObject).getValue)
      case oi: HiveDecimalObjectInspector => oi.getPrimitiveJavaObject(hiveObject).bigDecimalValue()
      case oi: TimestampObjectInspector => DateTimeUtils.fromJavaTimestamp(oi.getPrimitiveJavaObject(hiveObject))
      case oi: DateObjectInspector => DateTimeUtils.fromJavaDate(oi.getPrimitiveJavaObject(hiveObject))
      case oi: BinaryObjectInspector => oi.getPrimitiveJavaObject(hiveObject)
      case oi =>
        val unwrapper = SparkSqlHiveUtils.hiveUnwrapperFor(oi)
        unwrapper(hiveObject)
    }
  }

}
