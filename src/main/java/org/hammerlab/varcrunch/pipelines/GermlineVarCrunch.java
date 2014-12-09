package org.hammerlab.varcrunch.pipelines;

import fi.tkk.ics.hadoop.bam.VariantContextWritable;
import org.apache.crunch.*;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.lib.SecondarySort;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hammerlab.varcrunch.mappers.PileupGermlineVariants;
import org.hammerlab.varcrunch.partitioning.CollectNearbyReadsDoFn;
import org.hammerlab.varcrunch.partitioning.ComputeDepthInInterval;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import java.util.Map;

import static org.apache.crunch.lib.SecondarySort.sortAndApply;


public class GermlineVarCrunch extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new GermlineVarCrunch(), args);
    }

    private static final int CONTIG_INTERVAL_SIZE = 300000;

    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: yarn jar varcrunch-*-SNAPSHOT-job.jar"
                                      + " [generic options] input output");
            System.err.println();
            GenericOptionsParser.printGenericCommandUsage(System.err);
            return 1;
        }

        String readsInputPath = args[0];
        String outputPath = args[1];

        // Create an object to coordinate pipeline creation and execution.
        Pipeline pipeline = new MRPipeline(GermlineVarCrunch.class, getConf());


        // Set up source to read from BAMs/SAMs
        TableSource<Long, SAMRecordWritable> readsInputSource = From.formattedFile(
                readsInputPath,
                AnySAMInputFormat.class,
                Writables.longs(),
                Writables.writables(SAMRecordWritable.class));

        // Read in SAMRecords
        PCollection<SAMRecordWritable> reads = pipeline.read(readsInputSource).values();

        PCollection<Pair<String, Integer>> contigIntervals = reads.parallelDo(
                new ComputeDepthInInterval(CONTIG_INTERVAL_SIZE),
                Writables.pairs(Writables.strings(), Writables.ints()));

        // Compute read depth distribution
        Map<Pair<String, Integer>, Long> contigIntervalCounts = contigIntervals.count().asMap().getValue();

        // Flatmap reads to task
        PTable<Integer, Pair<Integer, SAMRecordWritable>> partitionedReads = reads.parallelDo(
                new CollectNearbyReadsDoFn(CONTIG_INTERVAL_SIZE, contigIntervalCounts),
                Writables.tableOf(Writables.ints(), Writables.pairs(Writables.ints(), Writables.writables(SAMRecordWritable.class))));

        PCollection<VariantContextWritable> variants = SecondarySort.sortAndApply(
                partitionedReads,
                new PileupGermlineVariants(CONTIG_INTERVAL_SIZE),
                Writables.writables(VariantContextWritable.class));

                //pipeline.writeTextFile(contigIntervalCounts, outputPath);


                // Execute the pipeline as a MapReduce.
                PipelineResult result = pipeline.done();

        return result.succeeded() ? 0 : 1;
    }
}
