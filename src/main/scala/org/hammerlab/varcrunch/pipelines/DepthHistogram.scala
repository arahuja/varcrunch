package org.hammerlab.varcrunch.pipelines

import org.apache.crunch.scrunch.{Writables, PCollection, PipelineApp}
import org.bdgenomics.formats.avro.AlignmentRecord
import org.hammerlab.varcrunch.VarCrunchArgs
import org.hammerlab.varcrunch.read.Reads

object DepthHistogram extends VarCrunchArgs with PipelineApp {

  override def run(args: Array[String]): Unit = {
    super.parseArguments(args)

    val reads: PCollection[AlignmentRecord] = Reads.loadBDGReads(pipeline, inputPath)

    // Filter to mapped reads
    val mappedReads = reads.filter(_.getReadMapped)
    val positions: PCollection[(String, Long)] = mappedReads
      .flatMap(read => {
      Range.Long.apply(read.getStart, read.getEnd, 1).map((read.getContig.getContigName, _))
    }, Writables.tuple2(Writables.strings, Writables.longs))

    // Count reads at every position
    val readDepth = positions.count()

    // Count number of times each depth value appears
    val depthHistogram = readDepth.values().count()

    write(depthHistogram, to.textFile(outputPath))

  }
}
