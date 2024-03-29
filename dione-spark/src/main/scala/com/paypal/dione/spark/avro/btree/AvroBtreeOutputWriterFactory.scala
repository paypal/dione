/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.paypal.dione.spark.avro.btree

import com.paypal.dione.kvstorage.hadoop.avro.AvroHashBtreeStorageFolderReader.AVRO_BTREE_FILE_EXTENSION
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.avro.AvroBtreeOutputWriter
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

private[avro] class AvroBtreeOutputWriterFactory(jobOptions: AvroBtreeJobOptions) extends OutputWriterFactory {

  override def getFileExtension(context: TaskAttemptContext): String = AVRO_BTREE_FILE_EXTENSION

  override def newInstance(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter = {
    new AvroBtreeOutputWriter(path, context, dataSchema, jobOptions)
  }
}
