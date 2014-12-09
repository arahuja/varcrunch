package org.hammerlab.varcrunch.partitioning;

import htsjdk.samtools.SAMRecord;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import java.util.Map;


public class CollectNearbyReadsDoFn extends DoFn<SAMRecordWritable, Pair<Integer,  Pair<Integer, SAMRecordWritable>>> {


    private final Map<Pair<String, Integer>, Long> positionToTaskMapping;
    private final Integer intervalSize;

    public CollectNearbyReadsDoFn(Integer intervalSize, Map<Pair<String, Integer>, Long> positionToTaskMapping) {
        this.positionToTaskMapping = positionToTaskMapping;
        this.intervalSize = intervalSize;
    }

    @Override
    public void process (SAMRecordWritable input, Emitter <Pair<Integer, Pair<Integer, SAMRecordWritable>>> emitter) {
        // emit each position this read overlaps

        SAMRecord record = input.get();
        Integer startPosition = record.getAlignmentStart();

        Integer lastTask = null;
        if (!record.getReadUnmappedFlag() && startPosition != null) {
            for (int i = startPosition; i < startPosition + record.getReadBases().length; ++i) {
                Integer nextTask = i / intervalSize;
                if (nextTask != lastTask) {
                    lastTask = nextTask;
                    emitter.emit(
                            // emit contig, interval and record
                            new Pair<Integer, Pair<Integer, SAMRecordWritable>>(
                                    lastTask,
                                    new Pair(input.get().getAlignmentStart(), input)
                    ));
                }

            }
        }
    }
}
