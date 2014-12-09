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
            int lastInterval = -1;
            for (int i = startPosition; i < record.getAlignmentEnd(); i++) {
                int nextInterval = i % intervalSize;
                // Emit each read once for every interval it covers
                if (nextInterval != lastInterval) {
                    lastInterval = nextInterval;
                    emitter.emit(
                            // emit contig, interval and record
                            new Pair<String, Integer>(
                                    record.getReferenceName(),
                                    nextInterval)
                    );
                    // Skip length of interval or last base
                    i = Math.min(record.getAlignmentEnd(), i + intervalSize);
                }
            }
        }
    }
}
