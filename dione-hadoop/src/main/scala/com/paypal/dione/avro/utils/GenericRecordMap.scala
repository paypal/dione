package com.paypal.dione.avro.utils

import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

import scala.collection.JavaConversions._

/**
 * A simple wrapper class for returning a Map object instead of GenericRecord
 */
case class GenericRecordMap(gr: GenericRecord, projectedFields: Option[Seq[String]] = None) extends Map[String, AnyRef] {
  override def +[B1 >: AnyRef](kv: (String, B1)): Map[String, B1] = ???

  lazy private val projectedFieldsSet = projectedFields.map(_.toSet)

  override def get(key: String): Option[AnyRef] = {
    if (projectedFields.isEmpty || projectedFieldsSet.get.contains(key)) {
      gr.get(key) match {
        case s: Utf8 => Some(s.toString)
        case other => Option(other)
      }
    } else None
  }

  override def iterator: Iterator[(String, AnyRef)] = {
    if (projectedFields.nonEmpty)
      projectedFields.get.toIterator.map(fname => fname -> gr.get(fname))
    else
      gr.getSchema.getFields.toIterator.map(f => f.name() -> gr.get(f.name()))
  }

  override def -(key: String): Map[String, AnyRef] = ???
}