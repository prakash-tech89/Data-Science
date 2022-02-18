package com.spark.etl.workflows.components.transformers

import com.spark.etl.utils.SparkIOUtil
import org.apache.spark.sql.{DataFrame, SaveMode}

class StoreSalesLoader extends LoaderTrait {

  override def load(paramsMap: Map[String, Any], dataFrameMap: Map[String, DataFrame]): Unit = {

    val df = dataFrameMap.get("strSalesDF").get

    SparkIOUtil.writeOrc(df, SaveMode.Append, "storeSalesTable" , None)

  }
}
