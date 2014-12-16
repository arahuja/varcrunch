package org.hammerlab.varcrunch;

import org.apache.hadoop.util.ProgramDriver;
import org.hammerlab.varcrunch.pipelines.ComputeReadDepthInInterval;
import org.hammerlab.varcrunch.pipelines.DepthHistogram;
import org.hammerlab.varcrunch.pipelines.GermlinePipeline;
import org.hammerlab.varcrunch.pipelines.SomaticPipeline;

public class VarCrunchDriver {

    public static void main(String[] args) throws Exception {

        ProgramDriver programDriver = new ProgramDriver();
        int exitCode = -1;
        try {
            programDriver.addClass("readdepth-intervals", ComputeReadDepthInInterval.class, "Computes read depth over a given size interval");
            programDriver.addClass("readdepth-hist", DepthHistogram.class, "Computes distribution of read depths");
            programDriver.addClass("germline", GermlinePipeline.class, "Standard germline variant caller");
            programDriver.addClass("somatic", SomaticPipeline.class, "Standard somatic variant caller, takes tumor/normal input");

            programDriver.driver(args);

            exitCode = 0;
        } catch (Throwable e) {
            e.printStackTrace();
        }

        System.exit(exitCode);
    }
}
