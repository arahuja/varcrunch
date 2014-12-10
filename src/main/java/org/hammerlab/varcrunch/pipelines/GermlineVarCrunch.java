package org.hammerlab.varcrunch.pipelines;

import org.apache.crunch.*;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.lib.SecondarySort;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.hammerlab.varcrunch.mappers.PileupGermlineVariants;
import org.hammerlab.varcrunch.partitioning.CollectNearbyReadsDoFn;
import org.hammerlab.varcrunch.partitioning.ComputeDepthInInterval;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import java.util.HashMap;
import java.util.Map;


public class GermlineVarCrunch extends VarCrunchTool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new GermlineVarCrunch(), args);
    }

    private static final int CONTIG_INTERVAL_SIZE = 300000;

    public int run(String[] args) throws Exception {

        super.parseArguments(args);

        // Create an object to coordinate pipeline creation and execution.
        Pipeline pipeline = new MRPipeline(GermlineVarCrunch.class, getConf());


        // Set up source to read from BAMs/SAMs
        TableSource<Long, SAMRecordWritable> readsInputSource = From.formattedFile(
                inputPath,
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

        // Need to coalesce regions into evenly sized partitions
        Map<Pair<String, Integer>, Long> contigIntervalTasks = new HashMap<Pair<String, Integer>, Long>();

        // Flatmap reads to task
        // For now this is just distributes evenly across CONTIG_INTERVAL_SIZE splits
        PTable<Long, Pair<Integer, SAMRecordWritable>> partitionedReads = reads.parallelDo(
                "MapReadsToTask",
                new CollectNearbyReadsDoFn(CONTIG_INTERVAL_SIZE, contigIntervalTasks),
                Writables.tableOf(Writables.longs(), Writables.pairs(Writables.ints(), Writables.writables(SAMRecordWritable.class))));

        DoFn<Pair<Long, Iterable<Pair<Integer, SAMRecordWritable>>>, VariantContextWritable> variantCaller =
                new PileupGermlineVariants(CONTIG_INTERVAL_SIZE);

        PCollection<VariantContextWritable> variants = SecondarySort.sortAndApply(
                partitionedReads,
                variantCaller,
                Writables.writables(VariantContextWritable.class));

        pipeline.writeTextFile(variants, outputPath);

        // Execute the pipeline as a MapReduce.
        PipelineResult result = pipeline.done();

        return result.succeeded() ? 0 : 1;
    }
}
