package org.hammerlab.varcrunch.mappers;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.seqdoop.hadoop_bam.SAMRecordWritable;


public class MapReadsAtLoci extends DoFn<Iterable<Pair<Integer, SAMRecordWritable>>> {

    private final Integer intervalSize;

    public MapReadsAtLoci(Integer intervalSize) {
        this.intervalSize = intervalSize;
    }

    @Override
    public void process(Integer input, Emitter<Iterable<Pair<Integer, SAMRecordWritable>>> emitter) {

    }
}
