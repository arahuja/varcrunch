package org.hammerlab.varcrunch.pipelines;

import org.apache.crunch.*;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hammerlab.varcrunch.filters.MappedReadFilter;
import org.hammerlab.varcrunch.partitioning.ComputeDepthInInterval;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;


public class ComputeReadDepthInInterval extends Configured implements Tool {

    @Option(name = "--interval-length", aliases = { "-l" })
    private int intervalLength = 100000;

    @Option(name = "--input", aliases = { "-i" }, required = true)
    private String inputPath;

    @Option(name = "--output", aliases = { "-o" }, required = true)
    private String outputPath;

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new ComputeReadDepthInInterval(), args);
    }

    public int run(String[] args) throws Exception {

        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.out);
            System.exit(-1);
        }

        // Create an object to coordinate pipeline creation and execution.
        Pipeline pipeline = new MRPipeline(ComputeReadDepthInInterval.class, getConf());


        // Set up source to read from BAMs/SAMs
        TableSource<Long, SAMRecordWritable> samSource = From.formattedFile(inputPath,
                AnySAMInputFormat.class,
                Writables.longs(),
                Writables.writables(SAMRecordWritable.class));

        // Read in SAMRecords
        PCollection<SAMRecordWritable> records = pipeline.read(samSource).values();

        // Filter reads to mapped reads
        PCollection<SAMRecordWritable> mappedReads = records.filter(new MappedReadFilter());

        PCollection<Pair<String, Integer>> contigIntervals = mappedReads.parallelDo(
                new ComputeDepthInInterval(intervalLength),
                Writables.pairs(Writables.strings(), Writables.ints()));

        // Compute read depth distribution
        PTable<Pair<String, Integer>, Long> contigIntervalCounts = contigIntervals.count();

        // Instruct the pipeline to write the resulting counts to a text file.
        pipeline.writeTextFile(contigIntervalCounts, outputPath);

        // Execute the pipeline as a MapReduce.
        PipelineResult result = pipeline.done();

        return result.succeeded() ? 0 : 1;
    }
}
