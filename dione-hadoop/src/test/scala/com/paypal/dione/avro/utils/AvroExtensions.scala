package com.paypal.dione.avro.utils

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

trait AvroExtensions {
  ///////////////
  // Utilities //
  ///////////////

  def createRecord(schema: Schema) =
    new GenericData.Record(schema)

  def createRecord(schema: Schema, items: Any*) = {
    new GenericData.Record(schema).putItems(items.iterator)
  }

  ///////////////
  // Implicits //
  ///////////////

  implicit class RecordExtensions(r: GenericRecord) {
    def putItems[T <: Any](values: Iterator[T]) = {
      values.zipWithIndex.foreach { case (obj, i) =>
        r.put(i, obj)
      }
      r
    }

  }

  implicit class SchemaExtensions(s: Schema) {

    def createRecord() = AvroExtensions.this.createRecord(s)

    def createRecord(item: Any) =
      AvroExtensions.this.createRecord(s, item)

    def createGenericDatumReader() = GenericData.get().createDatumReader(s)

  }

}
