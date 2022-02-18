package com.spark.etl.workflows

import com.spark.etl.workflows.workflow.{CovidCaseWorkflow, ItemSalesWorkflow, StoreSalesWorkflow, WorkflowTrait}
import com.spark.etl.utils.{StringConstantsUtil, Utils}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

object WorkflowManager {

  val log = Logger.getLogger(this.getClass.getName)

  def manageWorkFlow(paramsMap:Map[String, Any]) : Unit = {

    val workflowInstance:Option[WorkflowTrait] = paramsMap.get(StringConstantsUtil.WORKFLOW) match {
      case Some(StringConstantsUtil.ITEMSALESWORKFLOW) =>
        log.debug("Invoking " + StringConstantsUtil.ITEMSALESWORKFLOW)
        Some(ItemSalesWorkflow)

      case Some(StringConstantsUtil.STORESALESWORKFLOW) =>
        log.debug("Invoking " + StringConstantsUtil.STORESALESWORKFLOW)
        Some(StoreSalesWorkflow)
      case Some(StringConstantsUtil.COVIDCASEWORKFLOW) =>
        log.debug("Invoking " + StringConstantsUtil.COVIDCASEWORKFLOW)
        Some(CovidCaseWorkflow)

      case Some(x) => log.error("No Workflow implementation available for " + x)
        None
    }
    workflowInstance match {

      case Some(x) => executeFlow(paramsMap, workflowInstance.get)

      case None => log.error("No Workflow implementation available")
    }

  }

  def executeFlow(paramsMap: Map[String,Any], workflow:WorkflowTrait) : Unit ={
    var resultantDFMap : Map[String,DataFrame] = Map[String,DataFrame]()

    val extractorsSet = workflow.extractorsSet
    val transformersSet = workflow.transformersSet
    val loadersSet = workflow.loadersSet

    val printString = Utils.printString(extractorsSet) + Utils.printString(transformersSet) +
      Utils.printString(loadersSet)

    log.info("Running " + paramsMap.get(StringConstantsUtil.WORKFLOW) + " having " + printString)

    extractorsSet.map {
      ext => {
        val result = ext.extract(paramsMap, Some(resultantDFMap))
        result match {
          case None =>
          case map:Option[Map[String,DataFrame]] => resultantDFMap = resultantDFMap ++ map.get
        }
      }
    }

    transformersSet.map {

      trans => {
        val result = trans.transform(paramsMap, resultantDFMap)
        resultantDFMap = resultantDFMap ++ result
      }
    }


    loadersSet.map{

      loader => {
        loader.load(paramsMap, resultantDFMap)
      }
    }

  }





}
