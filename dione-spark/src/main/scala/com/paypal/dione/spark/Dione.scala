package com.paypal.dione.spark

import com.paypal.dione.spark.execution.DioneIndexStrategy
import org.apache.spark.sql.SparkSession

object Dione {

  private var dioneContext: DioneContext = _

  def getContext(spark: SparkSession): DioneContext = {
    if (Option(dioneContext).isEmpty)
      dioneContext = DioneContext()(spark)
    dioneContext
  }

  def getContext: DioneContext = {
    getContext(SparkSession.getActiveSession.getOrElse {
      throw new RuntimeException("No active spark session found")
    })
  }

  def enable(implicit spark: SparkSession): Unit = {
    spark.sessionState.experimentalMethods.extraOptimizations ++= DioneRule :: Nil
    spark.sessionState.experimentalMethods.extraStrategies ++= DioneIndexStrategy :: Nil
  }

  def disable(implicit spark: SparkSession): Unit = {
    spark.sessionState.experimentalMethods.extraOptimizations =
      spark.sessionState.experimentalMethods.extraOptimizations.filterNot(_ == DioneRule)
    spark.sessionState.experimentalMethods.extraStrategies =
      spark.sessionState.experimentalMethods.extraStrategies.filterNot(_ == DioneIndexStrategy)
  }
}
