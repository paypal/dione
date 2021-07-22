package org.apache.spark.sql.hive

import java.io.{DataInputStream, DataOutputStream, ObjectInputStream, ObjectOutputStream}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.io.Writable
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, SpecificInternalRow}
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.Utils

/**
 * Access to protected functions from spark.sql.hive
 */
object SparkSqlHiveUtils {
  val hiveClientImpl = HiveClientImpl

  def toHiveTable(table: CatalogTable, userName: Option[String] = None) =
    hiveClientImpl.toHiveTable(table, userName)

  def toHivePartition(p: CatalogTablePartition, ht: Table) = hiveClientImpl.toHivePartition(p, ht)

  def fillObject(schema: StructType,
                 iterator: Iterator[Writable],
                 rawDeser: Deserializer,
                 nonPartitionKeyAttrs: Seq[(Attribute, Int)],
                 tableDeser: Deserializer): Iterator[Row] = {
    val mutableRow = new SpecificInternalRow(schema.map(_.dataType))
    val encoder = RowEncoder(schema).resolveAndBind()
    val internalRows = HadoopTableReader.fillObject(iterator, rawDeser, nonPartitionKeyAttrs, mutableRow, tableDeser)
    internalRows.map(encoder.fromRow)
  }

  def hiveUnwrapperFor(objectInspector: ObjectInspector) = HadoopTableReader.unwrapperFor(objectInspector)

}

object SerializableConfiguration {
  def broadcast(spark: SparkSession) = {
    val sc = spark.sparkContext
    sc.broadcast(new SerializableConfiguration(sc.hadoopConfiguration))
  }
}

// copied from databricks
class SerializableConfiguration(@transient var value: Configuration) extends Serializable with KryoSerializable {
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    value = new Configuration(false)
    value.readFields(in)
  }

  def write(kryo: Kryo, out: Output): Unit = {
    val dos = new DataOutputStream(out)
    value.write(dos)
    dos.flush()
  }

  def read(kryo: Kryo, in: Input): Unit = {
    value = new Configuration(false)
    value.readFields(new DataInputStream(in))
  }
}