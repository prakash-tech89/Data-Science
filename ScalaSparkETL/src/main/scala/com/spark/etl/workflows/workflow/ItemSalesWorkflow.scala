package com.spark.etl.workflows.workflow

import com.spark.etl.workflows.components.extractors.{ItemExtractor, SalesExtractor}
import com.spark.etl.workflows.components.loaders.ItemSalesTransformer
import com.spark.etl.workflows.components.transformers.ItemSalesLoader
import com.spark.etl.workflows.workflow.WorkflowTrait

object ItemSalesWorkflow extends WorkflowTrait {

  addExtractors(new SalesExtractor, new ItemExtractor)

  addTransformers(new ItemSalesTransformer)

  addLoaders(new ItemSalesLoader)

}
