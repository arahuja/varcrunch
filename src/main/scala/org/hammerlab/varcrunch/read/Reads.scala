package org.hammerlab.varcrunch.read

import org.apache.crunch.scrunch.{PTable, From, PCollection, Pipeline}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.bdgenomics.adam.converters.SAMRecordConverter
import org.bdgenomics.adam.models.{RecordGroupDictionary, SequenceDictionary}
import org.bdgenomics.formats.avro.AlignmentRecord
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
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

  def loadBDGReads(pipeline: Pipeline, inputPath: String): PCollection[AlignmentRecord] = {


    val samHeader = SAMHeaderReader.readSAMHeaderFrom(new Path(inputPath), pipeline.getConfiguration())
    val seqDict = SequenceDictionary(samHeader)
    val readGroups = RecordGroupDictionary.fromSAMHeader(samHeader)

    val converter = new SAMRecordConverter()
    pipeline.read(From.formattedFile(
      inputPath,
      classOf[AnySAMInputFormat],
      classOf[LongWritable],
      classOf[SAMRecordWritable]
    )).values.map(r => converter.convert(r.get, seqDict, readGroups))
  }

  def partitionReadsByRegion(reads: PCollection[AlignmentRecord],
                             intervalLength: Int): PTable[Int, (Long, AlignmentRecord)] = {
    reads.flatMap(r => {
      val overlappingIntervals =
        Range.Long.
          inclusive(r.getStart, r.getEnd - 1, 1).map( _ / intervalLength ).toSet
      overlappingIntervals.map(task => (task.toInt, (r.getStart.toLong, r)))
    })
  }

}
