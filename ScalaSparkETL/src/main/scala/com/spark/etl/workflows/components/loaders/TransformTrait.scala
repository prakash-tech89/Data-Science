package com.spark.etl.workflows.components.loaders

import org.apache.spark.sql.DataFrame

trait TransformTrait {

  val dataframes:Map[String, DataFrame] = Map()

  def transform(paramsMap: Map[String,Any],
                dataFrameMap : Map[String,DataFrame]): Map[String,DataFrame]

}