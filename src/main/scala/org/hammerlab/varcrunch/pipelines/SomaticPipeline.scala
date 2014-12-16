package org.hammerlab.varcrunch.pipelines

import org.apache.crunch.scrunch.{PCollection, PipelineApp}
import org.bdgenomics.formats.avro.{AlignmentRecord, Genotype}
import org.hammerlab.guacamole.commands.SomaticStandard
import org.hammerlab.guacamole.filters.{SomaticMinimumLikelihoodFilter, SomaticReadDepthFilter}
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.{MappedRead, Read}
import org.hammerlab.guacamole.variants.AlleleConversions
import org.hammerlab.guacamole.windowing.SlidingWindow
import org.hammerlab.varcrunch.VarCrunchSomaticArgs
import org.hammerlab.varcrunch.read.Reads
import org.hammerlab.varcrunch.read.Reads.GenomicPosition
import org.kohsuke.args4j.{Option => Opt}

import scala.collection.mutable.ArrayBuffer

object SomaticPipeline extends VarCrunchSomaticArgs with PipelineApp {


  @Opt(name = "--intervalLength", aliases = Array("-l"))
  protected var intervalLength: Int = 100000

  @Opt(name = "--maxReadDepth")
  protected var maxReadDepth: Int = 120

  @Opt(name = "--minReadDepth")
  protected var minReadDepth: Int = 10

  @Opt(name = "--oddsThreshold")
  protected var oddsThreshold: Int = 110

  @Opt(name = "--minLikelihood", aliases = Array("-p"))
  protected var minLikelihood: Int = 60



  override def run(args: Array[String]): Unit = {
    super.parseArguments(args)

    val tumorReads: PCollection[AlignmentRecord] = Reads.loadBDGReads(pipeline, tumorInputPath).filter(_.getReadMapped)
    val normalReads: PCollection[AlignmentRecord] = Reads.loadBDGReads(pipeline, normalInputPath).filter(_.getReadMapped)

    val genotypes = callSomaticVariants(tumorReads, normalReads)

    write(genotypes, to.avroFile(outputPath))

  }

  def callSomaticVariants(tumorReads: PCollection[AlignmentRecord],
                          normalReads: PCollection[AlignmentRecord]): PCollection[Genotype] = {

    val labeledTumorReads = tumorReads.map(read => {
      read.setRecordGroupSample("tumor")
      read
    })

    val labeledNormalReads = normalReads.map(read => {
      read.setRecordGroupSample("normal")
      read
    })

    val tumorReadsPartitioned = Reads.partitionReadsByRegion(labeledTumorReads, intervalLength)
    val normalReadsPartitioned = Reads.partitionReadsByRegion(labeledNormalReads, intervalLength)

    val groupedReads = tumorReadsPartitioned.union(normalReadsPartitioned)
    groupedReads.secondarySortAndFlatMap(callSomaticVariantsOnPartition)
  }

  def callSomaticVariantsOnPartition(task: GenomicPosition, readsAndPositions: Iterable[(Long, AlignmentRecord)]) = {

    val readsAndPositionsIterator = readsAndPositions.iterator
    val reads: Iterator[MappedRead] =
      readsAndPositionsIterator.flatMap( kv =>  Read.fromADAMRecord(kv._2, 0).getMappedReadOpt)

    val window = SlidingWindow(0L, reads)
    var nextLocus = window.nextLocusWithRegions()
    val genotypes = new ArrayBuffer[Genotype]()

    while (nextLocus.isDefined) {
      window.setCurrentLocus(nextLocus.get)
      if (!window.currentRegions().isEmpty) {
        val currentLocus =   nextLocus.get
        val currentReads = window.currentRegions()
        val (tumorReads, normalReads) = currentReads.partition(_.sampleName == "tumor")
        val referenceBaseAtLocus = Pileup.referenceBaseAtLocus(currentReads, currentLocus)

        val tumorPileup = Pileup(tumorReads, currentLocus, referenceBaseAtLocus)
        val normalPileup = Pileup(normalReads, currentLocus, referenceBaseAtLocus)

        val calledAlleles = SomaticStandard.Caller.findPotentialVariantAtLocus(
          tumorPileup,
          normalPileup,
          oddsThreshold = oddsThreshold
        )

        val filteredAlleles =  calledAlleles
          .filter(SomaticMinimumLikelihoodFilter.hasMinimumLikelihood(_, minLikelihood))
          .filter(SomaticReadDepthFilter.withinReadDepthRange(_, minReadDepth, maxReadDepth, minReadDepth))

        genotypes ++= filteredAlleles.flatMap(AlleleConversions.calledSomaticAlleleToADAMGenotype(_))
      }
      nextLocus = window.nextLocusWithRegions()
    }

    genotypes
  }
}
