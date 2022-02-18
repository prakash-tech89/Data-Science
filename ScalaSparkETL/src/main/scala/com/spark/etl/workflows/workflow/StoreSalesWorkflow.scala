package com.spark.etl.workflows.workflow

import com.spark.etl.workflows.components.extractors.{SalesExtractor, StoreExtractor}
import com.spark.etl.workflows.components.loaders.StoreSalesTransformer
import com.spark.etl.workflows.components.transformers.StoreSalesLoader

object StoreSalesWorkflow extends WorkflowTrait {

  addExtractors(new SalesExtractor, new StoreExtractor)

  addTransformers(new StoreSalesTransformer)

  addLoaders(new StoreSalesLoader)

}
