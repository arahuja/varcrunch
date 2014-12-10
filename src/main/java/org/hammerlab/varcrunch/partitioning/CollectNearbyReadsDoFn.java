package org.hammerlab.varcrunch.partitioning;

import htsjdk.samtools.SAMRecord;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import java.util.Map;


public class CollectNearbyReadsDoFn extends DoFn<SAMRecordWritable, Pair<Long,  Pair<Integer, SAMRecordWritable>>> {


    private final Map<Pair<String, Integer>, Long> positionToTaskMapping;
    private final Integer intervalSize;

    public CollectNearbyReadsDoFn(Integer intervalSize, Map<Pair<String, Integer>, Long> positionToTaskMapping) {
        this.positionToTaskMapping = positionToTaskMapping;
        this.intervalSize = intervalSize;
    }

    @Override
    public void process (SAMRecordWritable input, Emitter <Pair<Long, Pair<Integer, SAMRecordWritable>>> emitter) {
        // emit each position this read overlaps

        SAMRecord record = input.get();
        Integer startPosition = record.getAlignmentStart();

        Long lastTask = null;
        if (!record.getReadUnmappedFlag() && startPosition != null) {
            for (int i = startPosition; i < startPosition + record.getReadBases().length; ++i) {
                Long nextTask = positionToTaskMapping.get(new Pair<String, Integer>(record.getReferenceName(), i));

                // If we haven't mapped this position to a task, do so evenly
                if (nextTask == null) {
                    nextTask = record.getReferenceName().hashCode() + (long) (i / intervalSize);
                }

                if (nextTask != lastTask) {
                    lastTask = nextTask;
                    emitter.emit(
                            // emit contig, interval and record
                            new Pair<Long, Pair<Integer, SAMRecordWritable>>(
                                    lastTask,
                                    new Pair(input.get().getAlignmentStart(), input)
                    ));
                }
                // Skip length of interval or to the last base
                i = Math.min(record.getAlignmentEnd(), i + intervalSize);
            }
        }
    }
}
