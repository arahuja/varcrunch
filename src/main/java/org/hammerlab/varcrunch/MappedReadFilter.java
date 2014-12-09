package org.hammerlab.varcrunch;

import htsjdk.samtools.SAMRecord;
import org.apache.crunch.FilterFn;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public class MappedReadFilter extends FilterFn<SAMRecordWritable> {
    @Override
    public boolean accept(SAMRecordWritable input) {
        return (input.get().getMappingQuality() != SAMRecord.UNKNOWN_MAPPING_QUALITY &&
                input.get().getReferenceName() != null &&
                input.get().getReferenceIndex() >= SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX &&
                input.get().getAlignmentStart() >= 0 &&
                input.get().getUnclippedStart() >= 0);
    }
}