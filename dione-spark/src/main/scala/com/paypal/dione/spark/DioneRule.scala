package com.paypal.dione.spark

import com.paypal.dione.spark.index.{IndexManager, IndexManagerUtils}
import com.paypal.dione.spark.sql.catalyst.catalog.HiveIndexTableRelation
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.StructType

object DioneRule extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transform {

      // For a query covering index we switch between the original table and the index table
      case p @ Project(_, h @ HiveTableRelation(_, _, _))
        if getCoveringIndex(h, p.projectList.map(_.name)).nonEmpty =>
        val idx = getCoveringIndex(h, p.projectList.map(_.name)).get
        val indexCatalogTable = IndexManagerUtils.getSparkCatalogTable(Dione.getContext.spark,
          idx.indexTableName)
        val updatedAttributes = toAttributes(indexCatalogTable.dataSchema,
          h.dataCols.filter(dc => p.projectList.map(_.name).contains(dc.name)))
        p.copy(child = h.copy(tableMeta = indexCatalogTable, dataCols = updatedAttributes))

      case p @ Project(_, f @ Filter(_, h @ HiveTableRelation(_, _, _)))
        if getCoveringIndex(h, p.projectList.map(_.name)).nonEmpty =>
        val idx = getCoveringIndex(h, p.projectList.map(_.name)).get
        val indexCatalogTable = IndexManagerUtils.getSparkCatalogTable(Dione.getContext.spark,
          idx.indexTableName)
        val updatedAttributes = toAttributes(indexCatalogTable.dataSchema,
          h.dataCols.filter(dc => p.projectList.map(_.name).contains(dc.name)))
        p.copy(child = f.copy(child = h.copy(tableMeta = indexCatalogTable, dataCols = updatedAttributes)))

      // For a data lookup index we add relevant information to later use in the strategy
      case p @ Project(_, f @ Filter(condition, h @ HiveTableRelation(_, _, _)))
        if getSpecificFilter(condition, h).nonEmpty =>
        val (idx, literalFilter) = getSpecificFilter(condition, h).get
        val indexCatalogTable = IndexManagerUtils.getSparkCatalogTable(Dione.getContext.spark,
          idx.indexTableName)
        val updatedAttributes = toAttributes(indexCatalogTable.dataSchema,
          h.dataCols.filter(dc => p.references.map(_.name).toSet.contains(dc.name)))
        p.copy(child = f.copy(child = new HiveIndexTableRelation(tableMeta = indexCatalogTable,
            dataCols = updatedAttributes, partitionCols = h.partitionCols, h, idx, literalFilter)))

      case f @ Filter(condition, h @ HiveTableRelation(_, _, _))
        if getSpecificFilter(condition, h).nonEmpty =>
        val (idx, literalFilter) = getSpecificFilter(condition, h).get
        val indexCatalogTable = IndexManagerUtils.getSparkCatalogTable(Dione.getContext.spark,
          idx.indexTableName)
        val updatedAttributes = toAttributes(indexCatalogTable.dataSchema,
          h.dataCols.filter(dc => f.references.map(_.name).toSet.contains(dc.name)))
        f.copy(child = new HiveIndexTableRelation(tableMeta = indexCatalogTable,
          dataCols = updatedAttributes, partitionCols = h.partitionCols, h, idx, literalFilter))
    }
  }

  // based on StructType.toAttributes()
  def toAttributes(structType: StructType, origAttrs: Seq[AttributeReference]): Seq[AttributeReference] = {
    val origMap = origAttrs.map(ar => ar.name -> ar).toMap
    structType
      .map(f => origMap.getOrElse(f.name, AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()))
  }

  def getCoveringIndex(h: HiveTableRelation, referencedAtts: Seq[String]): Option[IndexManager] = {
    Dione.getContext.getIndices(h.tableMeta.identifier)
      .find(ci => referencedAtts.forall(ci.indexSpec.getFields.contains))
  }

  def getIndexForTable(h: HiveTableRelation): Option[IndexManager] = {
    Dione.getContext.getIndices(h.tableMeta.identifier).headOption
  }

  private def getSpecificFilter(condition: Expression, hiveTableRelation: HiveTableRelation): Option[(IndexManager, Seq[Literal])] = {
    def findLiteralKeyExpression(key: String, p: Expression) = p match {
      case EqualTo(left, right: Literal) if left.references.size == 1 && left.references.toSeq.head.name == key => Some(right)
      case _ => None
    }

    val idx = getIndexForTable(hiveTableRelation)

    if (idx.nonEmpty) {
      val vals = idx.get.keys.map(k => condition.flatMap(p => findLiteralKeyExpression(k, p)).headOption)
      if (vals.exists(_.isEmpty))
        None
      else Some(idx.get, vals.map(v => Literal(v.get)))
    } else {
      None
    }
  }
}
