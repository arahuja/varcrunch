package org.hammerlab.varcrunch.pipelines;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.TableSource;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hammerlab.varcrunch.filters.UnmappedReadFilter;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;


public class VarCrunch extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new VarCrunch(), args);
    }

    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: yarn jar varcrunch-*-SNAPSHOT-job.jar"
                                      + " [generic options] input output");
            System.err.println();
            GenericOptionsParser.printGenericCommandUsage(System.err);
            return 1;
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // Create an object to coordinate pipeline creation and execution.
        Pipeline pipeline = new MRPipeline(VarCrunch.class, getConf());


        // Set up source to read from BAMs/SAMs
        TableSource<Long, SAMRecordWritable> samSource = From.formattedFile(inputPath,
                AnySAMInputFormat.class,
                Writables.longs(),
                Writables.writables(SAMRecordWritable.class));

        // Read in SAMRecords
        PCollection<SAMRecordWritable> records = pipeline.read(samSource).values();

        // Filter reads to unmapped reads
        PCollection<SAMRecordWritable> unmappedReads = records.filter(new UnmappedReadFilter());


        // Instruct the pipeline to write the resulting counts to a text file.
        pipeline.writeTextFile(unmappedReads, outputPath);

        // Execute the pipeline as a MapReduce.
        PipelineResult result = pipeline.done();

        return result.succeeded() ? 0 : 1;
    }
}
