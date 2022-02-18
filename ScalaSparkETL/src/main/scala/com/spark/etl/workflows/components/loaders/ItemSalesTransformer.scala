package com.spark.etl.workflows.components.loaders

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

class ItemSalesTransformer extends TransformTrait {

  override def transform(paramsMap: Map[String, Any],
                         dataFrameMap: Map[String, DataFrame]):Map[String, DataFrame] = {

    val log = Logger.getLogger(this.getClass.getName)

    val itemDF = dataFrameMap.get("itemDF").get
    val salesDF = dataFrameMap.get("salesDF").get

    val salesItemExpr = itemDF.col("ItemNumber") === salesDF.col("ItemNumber")

    val result = salesDF.groupBy(col("ItemNumber"))
      .agg(sum("SaleAmount").alias("saleAmt"))
      .join(itemDF, salesItemExpr, "inner")
      .select(itemDF("ItemNumber"), col("ItemDescription"), col("saleAmt"))

    result.show(10)


    Map("itemSalesDF" -> result)
  }


}