package com.spark.etl.workflows.components.loaders

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

class StoreSalesTransformer extends TransformTrait {

  override def transform(paramsMap: Map[String, Any],
                         dataFrameMap: Map[String, DataFrame]):Map[String, DataFrame] = {

    val log = Logger.getLogger(this.getClass.getName)

    val salesDF = dataFrameMap.get("salesDF").get
    val storeDF = dataFrameMap.get("storeDF").get

    val salesStrExpr = storeDF.col("StoreNumber") === salesDF.col("StoreNumber")

    val result = salesDF.groupBy(col("StoreNumber"))
      .agg(sum("SaleAmount").alias("saleAmt"))
      .join(storeDF, salesStrExpr, "inner")
      .select(storeDF("StoreNumber"), col("StoreName"), col("saleAmt"))

    result.show(10)


    Map("strSalesDF" -> result)



  }


}