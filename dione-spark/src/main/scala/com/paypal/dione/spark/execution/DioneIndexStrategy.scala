package com.paypal.dione.spark.execution

import com.paypal.dione.spark.Dione
import com.paypal.dione.spark.index.IndexManager
import com.paypal.dione.spark.index.IndexManager.indexMetaFields
import com.paypal.dione.spark.sql.catalyst.catalog.HiveIndexTableRelation
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

object DioneIndexStrategy extends Strategy {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case PhysicalOperation(projectList, predicates, indexRelation: HiveIndexTableRelation) =>
      // Filter out all predicates that only deal with partition keys, these are given to the
      // hive table scan operator to be used for partition pruning.
      val partitionKeyIds = AttributeSet(indexRelation.partitionCols)
      val (pruningPredicates, otherPredicates) = predicates.partition { predicate =>
        !predicate.references.isEmpty &&
          predicate.references.subsetOf(partitionKeyIds)
      }

      val idx = indexRelation.indexManager

      IndexBtreeFetchExec(projectList.flatMap(_.references.toSeq)
        .filterNot(p => indexMetaFields.contains(p.name)).distinct,
        indexRelation, idx, pruningPredicates, otherPredicates, indexRelation.literalFilters) :: Nil
    case _ =>
      Nil
  }

}
