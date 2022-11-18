package com.paypal.dione.spark.sql.catalyst.catalog

import com.paypal.dione.spark.index.IndexManager
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}

class HiveIndexTableRelation(tableMeta: CatalogTable,
                             dataCols: Seq[AttributeReference],
                             partitionCols: Seq[AttributeReference],
                             val hiveDataTableRelation: HiveTableRelation,
                             val indexManager: IndexManager,
                             val literalFilters: Seq[Literal])
  extends HiveTableRelation(tableMeta, dataCols, partitionCols) {

  override def output: Seq[AttributeReference] = hiveDataTableRelation.output
}
