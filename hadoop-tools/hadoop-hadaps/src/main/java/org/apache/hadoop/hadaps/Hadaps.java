/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@InterfaceAudience.Private
public class Hadaps extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(Hadaps.class);

  private static final String USAGE = String.format("Usage: java %s%n",
      Hadaps.class.getSimpleName());

  @Override
  public int run(String[] args) throws Exception {
    // Get configuration
    Configuration configuration = getConf();

    // Get parameters
    List<ParameterFile> parameterFiles = HadapsConfiguration.parseFiles(configuration);

    long startTime = Time.now();

    Balancer balancer = new Balancer(parameterFiles, configuration);
    balancer.run();

    long duration = Time.now() - startTime;
    LOG.info("Balancing took {}", Utils.getPrettyTime(duration));

    return 0;
  }

  public static void main(String[] args) {
    if (DFSUtil.parseHelpArgument(args, USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      System.exit(ToolRunner.run(new HadapsConfiguration(), new Hadaps(), args));
    } catch (Throwable e) {
      LOG.error("Exiting " + Hadaps.class.getSimpleName() + " due to an exception", e);
      System.exit(-1);
    }
  }

}
