package com.paypal.dione.avro.utils

import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

import scala.collection.JavaConversions._

case class GenericRecordMap(gr: GenericRecord) extends Map[String, AnyRef] {
  override def +[B1 >: AnyRef](kv: (String, B1)): Map[String, B1] = ???

  override def get(key: String): Option[AnyRef] = gr.get(key) match {
    case s: Utf8 => Some(s.toString)
    case other => Option(other)
  }

  override def iterator: Iterator[(String, AnyRef)] =
    gr.getSchema.getFields.toIterator.map(f => f.name() -> gr.get(f.name()))

  override def -(key: String): Map[String, AnyRef] = ???
}