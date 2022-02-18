package com.spark.etl.workflows.components.transformers

import org.apache.spark.sql.DataFrame

trait LoaderTrait {



  def load(paramsMap: Map[String,Any],
           dataFrameMap : Map[String,DataFrame]): Unit

}