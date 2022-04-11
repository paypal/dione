package org.apache.spark.sql.avro

import org.apache.avro.Schema
import org.apache.spark.sql.types.DataType

// Helper object to access Spark private methods
object AvroSerializerHelper {

  def avroSerializer(dataType: DataType, schema: Schema, nullable: Boolean): Any => Any = {
    val serializer = new AvroSerializer(dataType, schema, nullable)
    serializer.serialize
  }

  def avroDeserializer(schema: Schema, dataType: DataType): Any => Option[Any] = {
    val deserializer = new AvroDeserializer(schema, dataType)
    deserializer.deserialize
  }

}
