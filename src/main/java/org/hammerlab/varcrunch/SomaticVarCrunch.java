package org.hammerlab.varcrunch;

import org.apache.crunch.*;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.lib.PTables;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;


public class SomaticVarCrunch extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new SomaticVarCrunch(), args);
    }

    private static final int CONTIG_INTERVAL_SIZE = 10000;

    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: yarn jar varcrunch-*-SNAPSHOT-job.jar"
                                      + " [generic options] input output");
            System.err.println();
            GenericOptionsParser.printGenericCommandUsage(System.err);
            return 1;
        }

        String tumorReadsInputPath = args[0];
        String normalReadsInputPath = args[1];
        String outputPath = args[2];

        // Create an object to coordinate pipeline creation and execution.
        Pipeline pipeline = new MRPipeline(SomaticVarCrunch.class, getConf());


        // Set up source to read from BAMs/SAMs
        TableSource<Long, SAMRecordWritable> tumorInputSource = From.formattedFile(tumorReadsInputPath, AnySAMInputFormat.class, Writables.longs(), Writables.writables(SAMRecordWritable.class));
        TableSource<Long, SAMRecordWritable> normalInputSource = From.formattedFile(normalReadsInputPath, AnySAMInputFormat.class, Writables.longs(), Writables.writables(SAMRecordWritable.class));


        // Read in SAMRecords
        PCollection<SAMRecordWritable> tumorReads = pipeline.read(tumorInputSource).values();
        PCollection<SAMRecordWritable> normalReads = pipeline.read(normalInputSource).values();

        PCollection<Pair<String, Integer>> contigIntervals = tumorReads.parallelDo(new MapDepth(CONTIG_INTERVAL_SIZE), Writables.pairs(Writables.strings(), Writables.ints()));

        PTable<Pair<String, Integer>, Long> contigIntervalCounts = contigIntervals.count();

        pipeline.writeTextFile(contigIntervalCounts, outputPath);


        // Execute the pipeline as a MapReduce.
        PipelineResult result = pipeline.done();

        return result.succeeded() ? 0 : 1;
    }
}
