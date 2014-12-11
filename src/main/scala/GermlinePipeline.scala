package org.hammerlab.pipeline

import htsjdk.samtools.SAMRecord
import org.apache.crunch.scrunch.{PCollection, PipelineApp, Writables}
import org.apache.hadoop.io.LongWritable
import org.bdgenomics.formats.avro.Genotype
import org.hammerlab.guacamole.commands.GermlineStandard
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.{MappedRead, Read}
import org.hammerlab.guacamole.variants.AlleleConversions
import org.hammerlab.guacamole.windowing.SlidingWindow
import org.hammerlab.varcrunch.VarCrunchArgs
import org.hammerlab.varcrunch.pipelines.VarCrunchTool
import org.seqdoop.hadoop_bam.{AnySAMInputFormat, SAMRecordWritable}

import scala.collection.mutable.ArrayBuffer

object GermlinePipeline extends VarCrunchArgs with PipelineApp {

  type GenomicPosition = (String, Int)

  override def run(args: Array[String]): Unit = {
    super.parseArguments(args)

    val reads: PCollection[SAMRecordWritable] = read(from.formattedFile(
      inputPath,
      classOf[AnySAMInputFormat],
      classOf[LongWritable],
      classOf[SAMRecordWritable]
    )).values()

    // Filter to mapped reads
    val mappedReads = reads.filter(r => {
      val read: SAMRecord = r.get()
      !read.getReadUnmappedFlag()
    })

    // Compute depth in interval
    val readsInIntervals: PCollection[(String, Int)] = mappedReads.flatMap(r => {
      val intervalSize = 100000
      val read: SAMRecord = r.get()
      val overlappingIntervals = (read.getAlignmentStart until read.getAlignmentEnd).map( _ / intervalSize).toSet

      overlappingIntervals.map(pos => (read.getReferenceName, pos))
    }, Writables.tuple2(Writables.strings, Writables.ints))

    val intervalCounts = readsInIntervals.count()

    // Partition reads to overlapping intervals, duplicate reads if they overlap many intervals
    val readsPartitioned = mappedReads.flatMap(r => {
      val intervalSize = 100000
      val read: SAMRecord = r.get()
      val overlappingIntervals = (read.getAlignmentStart until read.getAlignmentEnd).map( _ / intervalSize).toSet
      overlappingIntervals.map( (_, (read.getAlignmentStart, r)) )
    })

    val genotypes = readsPartitioned.secondarySortAndFlatMap(callVariants)

    write(genotypes, to.avroFile(outputPath))

  }

  def callVariants(task: Int, readsAndPositions: Iterable[(Int, SAMRecordWritable)]): Seq[Genotype] = {

    val readsAndPositionsIterator = readsAndPositions.iterator
    val reads: Iterator[MappedRead] = readsAndPositionsIterator.flatMap(kv =>
      Read.fromSAMRecordOpt(kv._2.get(), 0).flatMap(_.getMappedReadOpt)
    )
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
