package org.hammerlab.varcrunch.pipelines

import htsjdk.samtools.SAMRecord
import org.apache.crunch.scrunch.{PCollection, PipelineApp}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.bdgenomics.formats.avro.Genotype
import org.hammerlab.guacamole.commands.GermlineStandard
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.{MappedRead, Read}
import org.hammerlab.guacamole.variants.AlleleConversions
import org.hammerlab.guacamole.windowing.SlidingWindow
import org.hammerlab.varcrunch.VarCrunchArgs
import org.hammerlab.varcrunch.filters.MappedReadFilter
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.seqdoop.hadoop_bam.{AnySAMInputFormat, SAMRecordWritable}

import scala.collection.mutable.ArrayBuffer

object GermlinePipeline extends VarCrunchArgs with PipelineApp {

  type GenomicPosition = (String, Int)

  override def run(args: Array[String]): Unit = {
    super.parseArguments(args)

    val samHeader = SAMHeaderReader.readSAMHeaderFrom(new Path(inputPath), configuration)


    val reads: PCollection[SAMRecordWritable] = read(from.formattedFile(
      inputPath,
      classOf[AnySAMInputFormat],
      classOf[LongWritable],
      classOf[SAMRecordWritable]
    )).values()

    // Filter to mapped reads
    val mappedReads = reads.filter(r => {
      MappedReadFilter.isMapped(r.get())
    })

    // Compute depth in interval
//    val readsInIntervals: PCollection[(String, Int)] = mappedReads.flatMap(r => {
//      val intervalSize = 100000
//      val read: SAMRecord = r.get()
//      val overlappingIntervals = (read.getAlignmentStart until read.getAlignmentEnd).map( _ / intervalSize).toSet
//
//      overlappingIntervals.map(pos => (read.getReferenceName, pos))
//    }, Writables.tuple2(Writables.strings, Writables.ints))
//
//    val intervalCounts = readsInIntervals.count()

    val genotypes = callVariants(mappedReads)

    write(genotypes, to.avroFile(outputPath))

  }

  def callVariants(reads: PCollection[SAMRecordWritable]): PCollection[Genotype] = {
    val readsPartitioned = reads.flatMap(r => {
      val intervalSize = 100000
      val read: SAMRecord = r.get()
      val overlappingIntervals = (read.getAlignmentStart until read.getAlignmentEnd).map( _ / intervalSize).toSet
      overlappingIntervals.map( (_, (read.getAlignmentStart, r)) )
    })

    readsPartitioned.secondarySortAndFlatMap(callVariantsOnPartition)

  }

  def callVariantsOnPartition(task: Int, readsAndPositions: Iterable[(Int, SAMRecordWritable)]): Seq[Genotype] = {

    val readsAndPositionsIterator = readsAndPositions.iterator
    val samRecords: Iterator[SAMRecord] = readsAndPositionsIterator.map(_._2.get())
    val reads: Iterator[MappedRead] = samRecords.flatMap(
      Read.fromSAMRecordOpt(_, 0).flatMap(_.getMappedReadOpt)
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
