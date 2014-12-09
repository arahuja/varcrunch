package org.hammerlab.varcrunch.partitioning;

import htsjdk.samtools.SAMRecord;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.seqdoop.hadoop_bam.SAMRecordWritable;


public class ComputeDepthInInterval extends DoFn<SAMRecordWritable, Pair<String, Integer>> {


    private final Integer intervalSize;

    public ComputeDepthInInterval(Integer intervalSize) {
        this.intervalSize = intervalSize;
    }

    @Override
    public void process (SAMRecordWritable input, Emitter<Pair<String, Integer >> emitter) {
        // emit each position this read overlaps

        SAMRecord record = input.get();
        Integer startPosition = record.getAlignmentStart();

        if (!record.getReadUnmappedFlag() && startPosition != null) {
            for (int i = startPosition; i < startPosition + record.getReadBases().length; ++i) {
                emitter.emit(
                        // emit contig, interval and record
                        new Pair<String, Integer>(
                                record.getReferenceName(),
                                i % intervalSize)
                );
            }
        }
    }
}
