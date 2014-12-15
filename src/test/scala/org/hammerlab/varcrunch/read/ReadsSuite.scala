package org.hammerlab.varcrunch.read

import org.apache.crunch.scrunch.Pipeline
import org.scalatest.FunSuite


class ReadsSuite extends FunSuite {

  val pipeline: Pipeline = Pipeline.inMemory

  test("test loading reads from sam into mapped reads") {

    val reads = Reads.loadReads(pipeline, "normal.chr20.tough.sam")

    println(reads.length().value())
    assert(reads.length().value() > 0)
  }



}
