package org.hammerlab.varcrunch;

import htsjdk.samtools.SAMRecord;
import org.apache.crunch.FilterFn;
import org.seqdoop.hadoop_bam.SAMRecordWritable;


public class UnmappedReadFilter extends FilterFn<SAMRecordWritable> {
    @Override
    public boolean accept(SAMRecordWritable input) {
        return input.get().getReadUnmappedFlag();
    }
}
