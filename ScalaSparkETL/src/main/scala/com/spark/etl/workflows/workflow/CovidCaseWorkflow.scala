package com.spark.etl.workflows.workflow

import com.spark.etl.workflows.components.extractors.{CovidCasesDlyExtractor, ItemExtractor, SalesExtractor}
import com.spark.etl.workflows.components.loaders.{CovidCasesDlyTransformer, ItemSalesTransformer}
import com.spark.etl.workflows.components.transformers.ItemSalesLoader

object CovidCaseWorkflow extends WorkflowTrait {

  addExtractors(new CovidCasesDlyExtractor)

  addTransformers(new CovidCasesDlyTransformer)

  //addLoaders(new ItemSalesLoader)

}
