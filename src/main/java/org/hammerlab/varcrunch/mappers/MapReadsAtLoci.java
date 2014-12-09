package org.hammerlab.varcrunch.mappers;

import org.apache.crunch.DoFn;
import org.apache.crunch.Pair;
import org.seqdoop.hadoop_bam.SAMRecordWritable;


abstract public class MapReadsAtLoci<T> extends DoFn<Pair<Integer, Iterable<Pair<Integer, SAMRecordWritable>>>, T> {

    private final Integer intervalSize;

    public MapReadsAtLoci(Integer intervalSize) {
        this.intervalSize = intervalSize;
    }

}
