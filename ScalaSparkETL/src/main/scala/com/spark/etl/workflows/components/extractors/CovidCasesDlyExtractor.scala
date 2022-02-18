package com.spark.etl.workflows.components.extractors

import com.spark.etl.utils.SparkIOUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

class CovidCasesDlyExtractor extends ExtractorTrait {
  override def extract(paramsMap: Map[String, Any], extractedDF:
  Option[Map[String, DataFrame]]): Option[Map[String, DataFrame]] = {

    val log = Logger.getLogger(this.getClass.getName)

    val covidcasedlydf = SparkIOUtil.read("./data/01-01-2021.csv")

    covidcasedlydf.show(4)
    Some(Map("covidcasedlydf" -> covidcasedlydf))
  }
}
