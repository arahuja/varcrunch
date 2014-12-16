package org.hammerlab.varcrunch;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;


public abstract class VarCrunchSomaticArgs extends VarCrunchBaseArgs {
  @Option(name = "--tumor-input", aliases = {"-t"}, required = true)
  protected String tumorInputPath;

  @Option(name = "--normal-input", aliases = {"-n"}, required = true)
  protected String normalInputPath;

}
