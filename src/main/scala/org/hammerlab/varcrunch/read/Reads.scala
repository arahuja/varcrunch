package org.hammerlab.varcrunch.read

import org.apache.crunch.scrunch.{From, PCollection, Pipeline}
import org.apache.hadoop.io.LongWritable
import org.seqdoop.hadoop_bam.{AnySAMInputFormat, SAMRecordWritable}


object Reads {

  def loadReads(pipeline: Pipeline, inputPath: String): PCollection[SAMRecordWritable] = {
    pipeline.read(From.formattedFile(
      inputPath,
      classOf[AnySAMInputFormat],
      classOf[LongWritable],
      classOf[SAMRecordWritable]
    )).values()
  }

}
