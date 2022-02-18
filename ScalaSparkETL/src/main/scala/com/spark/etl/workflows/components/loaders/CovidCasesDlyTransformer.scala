package com.spark.etl.workflows.components.loaders

import com.spark.etl.utils.SparkIOUtil.SparkImplicits.newStringEncoder
import com.sun.javaws.LocalInstallHandler.save
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, last, _}
import sun.invoke.util.ValueConversions.cast

class CovidCasesDlyTransformer extends TransformTrait {

  override def transform(paramsMap: Map[String, Any],
                         dataFrameMap: Map[String, DataFrame]): Map[String, DataFrame] = {

    val log = Logger.getLogger(this.getClass.getName)

    val covidDlyDF = dataFrameMap.get("covidcasedlydf").get


    //Filter out last 14 days and select needed columns to avoid unnecessary processing
      val Last14DayDF = covidDlyDF.filter(col("Last_Update") > date_add(current_timestamp(),-14)).select("Province_State", "Country_Region", "Last_Update", "Active")
    //  since i have used 1 day file which might not cover up for 14 days. So used below code to test my code
    //val Last14DayDF = covidDlyDF.filter(col("Last_Update") > "2021-01-02").select("Province_State", "Country_Region", "Last_Update", "Active")



    //Repartition based on country grain which is at higher grain and we need to do our operation at country level.
    Last14DayDF.repartition(col("Country_Region"))


    //Define window functions based on our need
    val countryWindow = Window.partitionBy(col("Country_Region"))
    val countryDayWindow = Window.partitionBy(col("Country_Region"), col("Last_Update"))
    val stateWindow = Window.partitionBy(col("Province_State"),col("Country_Region"))
    val countryStateWindow = Window.partitionBy(col("Country_Region"),col("Province_State"),col("ActiveCaseByState"))


    // find overall active case on country on day level for 14 days
    val sumPositiveDf = Last14DayDF
      .withColumn("sum_active", sum("Active") over (countryDayWindow))


    // Calculate difference from current day to 14 day active case
    val decreasePositiveDf = sumPositiveDf.withColumn("diff_active", (last("sum_active") over (countryWindow orderBy ("Last_Update")).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)) - (first("sum_active") over (countryWindow orderBy ("Last_Update")).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))


    //Find top 10 countries with decline in active case. Store it in list so that join can be avoided and we can use filter to improve performance
    val topnCntrListDF = decreasePositiveDf.select(col("Country_Region"),col("diff_active")).orderBy("diff_active").distinct().limit(10)
    val topnCntrList = topnCntrListDF.select(col("Country_Region")).as[String].collect().toList
    println(s"List of countries : $topnCntrList")


    // Filter out last 14 day dataframe for top 10 countries
    val topnCntryDF = Last14DayDF
      .filter(col("Country_Region").isin(topnCntrList: _*)).withColumn("sum_active", sum("Active") over (countryDayWindow))


    // find overall active case on state level
    val sumPositiveStateDf = topnCntryDF.withColumn("ActiveCaseByState", sum("Active") over (stateWindow)).select("Province_State","Country_Region","ActiveCaseByState").distinct()

    //Find top 3 state with increased active case among top 10 countries
    val topnStateList = sumPositiveStateDf.withColumn("row_number", row_number() over (countryWindow orderBy(desc("ActiveCaseByState")) ))
    val top3StateList = topnStateList.filter(col("row_number")<=3).select(col("Country_Region"),col("Province_State"),col("ActiveCaseByState"))



    //topnStateList.filter(col("Province_State")==="California").show(1000)
    top3StateList.show(500)





    Map("covidDlyDF" -> top3StateList)
  }


}