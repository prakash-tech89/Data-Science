package com.spark.etl.workflows.workflow

import org.apache.log4j.Logger
import com.spark.etl.utils.{StringConstantsUtil, Utils}
import com.spark.etl.workflows.components.extractors.ExtractorTrait
import com.spark.etl.workflows.components.loaders.TransformTrait
import com.spark.etl.workflows.components.transformers.LoaderTrait
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.LinkedHashSet;


trait WorkflowTrait {
  val log = Logger.getLogger(this.getClass.getName)

  val extractorsSet : LinkedHashSet[ExtractorTrait] = LinkedHashSet[ExtractorTrait]()
  val transformersSet : LinkedHashSet[TransformTrait] = LinkedHashSet[TransformTrait]()
  val loadersSet : LinkedHashSet[LoaderTrait] = LinkedHashSet[LoaderTrait]()

  def addExtractors(extractors: ExtractorTrait *):Unit = {

    for (ext <- extractors) {
      extractorsSet.add(ext)
    }

  }

  def addTransformers(transformers: TransformTrait *):Unit = {

    for (tran <- transformers) {
      transformersSet.add(tran)
    }

  }

  def addLoaders(loaders: LoaderTrait *):Unit = {

    for (load <- loaders) {
      loadersSet.add(load)
    }

  }

}
