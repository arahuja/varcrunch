package org.hammerlab.varcrunch;


import htsjdk.samtools.SAMRecord;
import org.apache.crunch.io.impl.FileSourceImpl;
import org.apache.crunch.types.PType;
import org.apache.hadoop.fs.Path;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;

import java.util.List;

public class BAMFileSource extends FileSourceImpl<SAMRecord> {

    public BAMFileSource(Path path, PType<SAMRecord> ptype) {
        super(path, ptype, AnySAMInputFormat.class);
    }

    public BAMFileSource(List<Path> paths, PType<SAMRecord> ptype) {
        super(paths, ptype, AnySAMInputFormat.class);
    }

    @Override
    public String toString() {
        return "BAM/SAMFile(" + pathsAsString() + ")";
    }

}