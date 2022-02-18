package com.spark.etl.workflows.components.transformers

import com.spark.etl.utils.SparkIOUtil
import org.apache.spark.sql.{DataFrame, SaveMode}

class ItemSalesLoader extends LoaderTrait {

  override def load(paramsMap: Map[String, Any], dataFrameMap: Map[String, DataFrame]): Unit = {

    val df = dataFrameMap.get("itemSalesDF").get

    SparkIOUtil.writeOrc(df, SaveMode.Append, "itemSalesTable" , None)

  }
}
