package org.hammerlab.varcrunch.pileup;

import htsjdk.samtools.SAMRecord;

import java.util.Deque;
import java.util.List;

public class Pileup {

    private Deque<SAMRecord> reads;

    public Pileup(Deque<SAMRecord> reads) {
        this.reads = reads;
    }

    public int depth() {
        return reads.size();
    }

    public boolean hasVariant() {
        return false;
    }

//    public int numMismatches() {
//        return
//    }
}
