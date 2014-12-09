package org.hammerlab.varcrunch.mappers;


import htsjdk.samtools.SAMRecord;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.hammerlab.varcrunch.pileup.Pileup;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

public class PileupGermlineVariants extends DoFn<Pair<Integer, Iterable<Pair<Integer, SAMRecordWritable>>>, VariantContextWritable> {

    Integer lociToCover;

    public PileupGermlineVariants(Integer lociToCover) {
        this.lociToCover = lociToCover;
    }

    @Override
    public void process(Pair<Integer, Iterable<Pair<Integer, SAMRecordWritable>>> input, Emitter<VariantContextWritable> emitter) {
        Integer task = input.first();
        Iterator<Pair<Integer, SAMRecordWritable>> positionAndReads = input.second().iterator();

        Deque<SAMRecord> currentPileupReads = new ArrayDeque<SAMRecord>();

        Integer lociLeftToCover = lociToCover;

        while (lociLeftToCover > 0) {

            Integer currentLociOffset = lociToCover - lociLeftToCover;
            Integer currentPosition = task * lociLeftToCover + currentLociOffset;

            // Remove reads from pileup that are before the current position
            while( currentPileupReads.peekFirst().getAlignmentEnd() < currentPosition) {
                currentPileupReads.pop();
            }

            // Add reads that overlap the pileup
            Pair<Integer, SAMRecordWritable> nextStartAndRead;
            while (positionAndReads.hasNext() && (nextStartAndRead = positionAndReads.next()).first() < currentPosition) {
                nextStartAndRead = positionAndReads.next();
                SAMRecord nextRead = nextStartAndRead.second().get();
                if (nextRead.getAlignmentEnd() < currentPosition) {
                    continue;
                } else {
                    currentPileupReads.addLast(nextRead);
                }

            }

            // process pileup
            Pileup pileup = new Pileup(currentPileupReads);

            if (pileup.hasVariant()) {
                emitter.emit(new VariantContextWritable().set);
            }

            lociLeftToCover--;
        }

    }

    private VariantContextWritable buildVariant() {
        return new VariantContextWritable().set(new VariantContext());
    }
}
