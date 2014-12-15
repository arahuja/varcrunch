package org.hammerlab.varcrunch.pipelines

import org.apache.crunch.scrunch.{PCollection, PTable, PipelineApp, Writables}
import org.bdgenomics.formats.avro.{AlignmentRecord, Genotype}
import org.hammerlab.guacamole.commands.GermlineStandard
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.{MappedRead, Read}
import org.hammerlab.guacamole.variants.AlleleConversions
import org.hammerlab.guacamole.windowing.SlidingWindow
import org.hammerlab.varcrunch.VarCrunchArgs
import org.hammerlab.varcrunch.read.Reads
import org.kohsuke.args4j.{Option => Opt}

import scala.collection.mutable.ArrayBuffer

object GermlinePipeline extends VarCrunchArgs with PipelineApp {

  @Opt(name = "--intervalLength", aliases = Array("-l"))
  protected var intervalLength: Int = 100000


  override def run(args: Array[String]): Unit = {
    super.parseArguments(args)

    val reads: PCollection[AlignmentRecord] = Reads.loadBDGReads(pipeline, inputPath)

    // Filter to mapped reads
    val mappedReads = reads.filter(_.getReadMapped)

    // Compute depth in interval
    val readsInIntervals: PCollection[(String, Long)] = mappedReads.flatMap( alignmentRecord => {
      val overlappingIntervals =
        Range.Long.
        inclusive(alignmentRecord.getStart, alignmentRecord.getEnd - 1, 1).map( _ / intervalLength).toSet
      overlappingIntervals.map(pos => (alignmentRecord.getContig.getContigName, pos))
    }, Writables.tuple2(Writables.strings, Writables.longs))

    val intervalCounts = readsInIntervals.count()

    val genotypes = callVariants(mappedReads)

    write(genotypes, to.avroFile(outputPath))

  }

  def callVariants(reads: PCollection[AlignmentRecord]): PCollection[Genotype] = {
    val readsPartitioned: PTable[Int, (Long, AlignmentRecord)] = reads.flatMap(r => {
      val overlappingIntervals =
        Range.Long.
          inclusive(r.getStart, r.getEnd - 1, 1).map( _ / intervalLength ).toSet
      overlappingIntervals.map(task => (task.toInt, (r.getStart.toLong, r)))
    })

    readsPartitioned.secondarySortAndFlatMap(callVariantsOnPartition)

  }

  def callVariantsOnPartition(task: Int, readsAndPositions: Iterable[(Long, AlignmentRecord)]): Seq[Genotype] = {

    val readsAndPositionsIterator = readsAndPositions.iterator
    val reads: Iterator[MappedRead] = readsAndPositionsIterator.flatMap( kv =>  Read.fromADAMRecord(kv._2, token = 0).getMappedReadOpt)
    val window = SlidingWindow(0L, reads)
    var nextLocus = window.nextLocusWithRegions()

    val genotypes = new ArrayBuffer[Genotype]()

    while (nextLocus.isDefined) {
      window.setCurrentLocus(nextLocus.get)
      if (!window.currentRegions().isEmpty) {
        val pileup = Pileup(window.currentRegions(), nextLocus.get)
        val calledAlleles = GermlineStandard.Caller.callVariantsAtLocus(pileup)
        genotypes ++= calledAlleles.flatMap(AlleleConversions.calledAlleleToADAMGenotype(_))
      }
      nextLocus = window.nextLocusWithRegions()
    }
    genotypes
  }
}
