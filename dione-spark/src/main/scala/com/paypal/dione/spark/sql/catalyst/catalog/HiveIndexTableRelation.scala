package com.paypal.dione.spark.sql.catalyst.catalog

import com.paypal.dione.spark.index.IndexManager
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.AttributeReference

class HiveIndexTableRelation(tableMeta: CatalogTable,
                             dataCols: Seq[AttributeReference],
                             partitionCols: Seq[AttributeReference],
                             val hiveDataTableRelation: HiveTableRelation,
                             val indexSpec: IndexManager)
  extends HiveTableRelation(tableMeta, dataCols, partitionCols) {

}
