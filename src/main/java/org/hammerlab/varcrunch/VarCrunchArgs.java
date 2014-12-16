package org.hammerlab.varcrunch;

import org.kohsuke.args4j.Option;


public abstract class VarCrunchArgs extends VarCrunchBaseArgs {
  @Option(name = "--input", aliases = {"-i"}, required = true)
  protected String inputPath;

}
