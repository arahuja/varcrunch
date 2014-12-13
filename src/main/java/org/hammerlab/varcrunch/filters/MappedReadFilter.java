package org.hammerlab.varcrunch.filters;

import htsjdk.samtools.SAMRecord;
import org.apache.crunch.FilterFn;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public class MappedReadFilter extends FilterFn<SAMRecordWritable> {
    @Override
    public boolean accept(SAMRecordWritable input) {
        return isMapped(input.get());
    }

    public static boolean isMapped(SAMRecord input) {
        return (input.getMappingQuality() != SAMRecord.UNKNOWN_MAPPING_QUALITY &&
                input.getReferenceName() != null &&
                input.getReferenceIndex() >= SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX &&
                input.getAlignmentStart() >= 0 &&
                input.getUnclippedStart() >= 0);
    }
}