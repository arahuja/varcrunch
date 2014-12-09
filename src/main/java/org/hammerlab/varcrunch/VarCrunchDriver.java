package org.hammerlab.varcrunch;

import org.apache.hadoop.util.ProgramDriver;
import org.hammerlab.varcrunch.pipelines.ComputeReadDepthInInterval;
import org.hammerlab.varcrunch.pipelines.GermlineVarCrunch;
import org.hammerlab.varcrunch.pipelines.SomaticVarCrunch;

public class VarCrunchDriver {

    public static void main(String[] args) throws Exception {

        ProgramDriver programDriver = new ProgramDriver();
        int exitCode = -1;
        try {
            programDriver.addClass("readdepth", ComputeReadDepthInInterval.class, "Computes read depth over a given size interval");
            programDriver.addClass("germline", GermlineVarCrunch.class, "Standard germline variant caller");
            programDriver.addClass("somatic", SomaticVarCrunch.class, "Standard somatic variant caller, takes tumor/normal input");

            programDriver.driver(args);

            exitCode = 0;
        } catch (Throwable e) {
            e.printStackTrace();
        }

        System.exit(exitCode);
    }
}
