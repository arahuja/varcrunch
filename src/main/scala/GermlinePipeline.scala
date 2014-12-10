package org.hammerlab.pipeline

import fi.tkk.ics.hadoop.bam.{AnySAMInputFormat, SAMRecordWritable}
import htsjdk.samtools.SAMRecord
import org.apache.crunch.scrunch.{PCollection, Writables, PipelineApp}
import org.apache.hadoop.io.LongWritable

object GermlinePipeline extends PipelineApp {
  override def run(args: Array[String]): Unit = {

    val inputPath = args(0)

    val reads: PCollection[SAMRecordWritable] = read(from.formattedFile(
      inputPath,
      classOf[AnySAMInputFormat],
      Writables.writables[LongWritable],
      Writables.writables[SAMRecordWritable]
    )).values()

    // Filter to mapped reads
    val mappedReads = reads.filter(r => {
      val read: SAMRecord = r.get()
      !read.getReadUnmappedFlag()
    })

    // Compute depth in interval
    val readsInIntervals: PCollection[(String, Int)] = mappedReads.flatMap( r => {
      val intervalSize = 100000
      val read: SAMRecord = r.get()
      var lastInterval = -1;
      val overlappingIntervals = (read.getAlignmentStart until read.getAlignmentEnd).map( _ / intervalSize).toSet
      overlappingIntervals.map((read.getReferenceName, _))
    })

    val intervalCounts = readsInIntervals.count()

  }
}
