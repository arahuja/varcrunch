package org.hammerlab.varcrunch.pileup;

import htsjdk.samtools.SAMRecord;

import java.util.Deque;

public class Pileup {

    private Deque<SAMRecord> reads;
    private Long currentPosition;

    public Pileup(Long currentPosition, Deque<SAMRecord> reads) {
        this.currentPosition =  currentPosition;
        this.reads = reads;
    }

    public int depth() {

        return reads.size();
    }

    public boolean hasVariant() {
       // Call variants randomly
        if (currentPosition % 100000 == 0){
            return true;
        } else {
            return false;
        }
    }

//    public int numMismatches() {
//        return
//    }
}
