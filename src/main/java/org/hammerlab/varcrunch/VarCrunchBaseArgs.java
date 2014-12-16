package org.hammerlab.varcrunch;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;


public abstract class VarCrunchBaseArgs {

  @Option(name = "--output", aliases = {"-o"}, required = true)
  protected String outputPath;

  protected void parseArguments(String [] args) {
    CmdLineParser parser = new CmdLineParser(this);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.out);
      System.exit(-1);
    }
  }

}
