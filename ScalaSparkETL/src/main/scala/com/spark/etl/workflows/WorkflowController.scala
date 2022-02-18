package com.spark.etl.workflows

import com.spark.etl.utils.{ConfigUtil, SparkIOUtil, StringConstantsUtil}



import java.util.logging.Logger
import scala.collection.mutable

object WorkflowController {

  var paramsMaps:Map[String,Any]=scala.collection.immutable.Map()
  val log = Logger.getLogger(this.getClass.getName)


  def configureSpark(paramsMaps: Map[String, Any]): Unit = {
    val allParams = paramsMaps ++ ConfigUtil.getGlobalConfig(paramsMaps.get(StringConstantsUtil.RUNMODE)) ++
      ConfigUtil.getAppConfig(paramsMaps.get(StringConstantsUtil.WORKFLOW).get.toString)

    SparkIOUtil.configureSpark(allParams);

    log.info("Spark is configured")

  }

  def main(args: Array[String]): Unit = {

  if(args.length==0){
    log.info("Argument is empty")
  }
  else{
    for (arg <- args){
      val res=arg.split("=")
      paramsMaps=paramsMaps + (res(0)->res(1))
    }
  }
    configureSpark(paramsMaps)
    WorkflowManager.manageWorkFlow(paramsMaps)
  }

}
