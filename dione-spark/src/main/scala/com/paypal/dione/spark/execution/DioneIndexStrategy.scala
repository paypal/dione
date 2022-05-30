package com.paypal.dione.spark.execution

import com.paypal.dione.spark.Dione
import com.paypal.dione.spark.index.IndexManager
import com.paypal.dione.spark.sql.catalyst.catalog.HiveIndexTableRelation
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

object DioneIndexStrategy extends Strategy {

  private def getIdx(relation: HiveTableRelation): Option[IndexManager] =
    Dione.getContext.indices.find(_._2.head.indexTableName == relation.tableMeta.identifier.identifier).map(_._2.head)

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case PhysicalOperation(projectList, predicates, relation: HiveIndexTableRelation) if getIdx(relation).nonEmpty =>
      // Filter out all predicates that only deal with partition keys, these are given to the
      // hive table scan operator to be used for partition pruning.
      val partitionKeyIds = AttributeSet(relation.partitionCols)
      val (pruningPredicates, otherPredicates) = predicates.partition { predicate =>
        !predicate.references.isEmpty &&
          predicate.references.subsetOf(partitionKeyIds)
      }

      val idx = getIdx(relation).get

      def findLiteralKeyExpression(key: String, p: Expression) = p match {
        case EqualTo(left, right: Literal) if left.references.size == 1 && left.references.toSeq.head.name == key => Some(right)
        case _ => None
      }

      val vals = idx.keys.map(k => otherPredicates.flatMap(p => findLiteralKeyExpression(k, p)).headOption)

      if (vals.exists(_.isEmpty))
        Nil
      else
        IndexBtreeScanExec(projectList.flatMap(_.references.toSeq).distinct,
          relation, idx, pruningPredicates, otherPredicates, vals.map(v => Literal(v.get))) :: Nil
    case _ =>
      Nil
  }

}
