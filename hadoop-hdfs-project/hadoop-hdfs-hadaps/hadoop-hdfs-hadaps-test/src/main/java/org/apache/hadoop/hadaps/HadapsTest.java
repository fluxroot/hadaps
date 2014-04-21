/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;

public class HadapsTest extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(HadapsTest.class);

  private static final String USAGE = String.format("Usage: java %s <command>%n"
          + "%n"
          + "Available commands are:%n"
          + "  read [-in <input directory>]%n"
          + "       [-out <output directory>]%n"
          + "       [-iteration <number of iterations>]%n"
          + "       [-csv <filename>]%n"
          + "    Reads files from the test directory using MapReduce.%n"
          + "%n"
          + "  write [-out <test directory>]%n"
          + "        [-count <number of files>]%n"
          + "        [-size <minsize in megabytes>:<maxsize in megabytes>]%n"
          + "    Writes files with random content to the test directory.%n",
      HadapsTest.class.getSimpleName()
  );

  private Parameters getParameters(String[] args) {
    assert args != null;

    // Default parameters
    Parameters.Mode mode = Parameters.DEFAULT.mode;
    String inputDirectory = Parameters.DEFAULT.inputDirectory;
    String outputDirectory = Parameters.DEFAULT.outputDirectory;
    String csv = Parameters.DEFAULT.csv;
    int iteration = Parameters.DEFAULT.iteration;
    int count = Parameters.DEFAULT.count;
    int minsize = Parameters.DEFAULT.minsize;
    int maxsize = Parameters.DEFAULT.maxsize;

    // Get arguments
    Iterator<String> tokens = Arrays.asList(args).iterator();
    while (tokens.hasNext()) {
      String token = tokens.next();

      if (token.equalsIgnoreCase("read")) {
        mode = Parameters.Mode.READ;
      } else if (token.equalsIgnoreCase("write")) {
        mode = Parameters.Mode.WRITE;
      } else if (token.equalsIgnoreCase("-in") && tokens.hasNext()) {
        inputDirectory = tokens.next().trim();
      } else if (token.equalsIgnoreCase("-out") && tokens.hasNext()) {
        outputDirectory = tokens.next().trim();
      } else if (token.equalsIgnoreCase("-csv") && tokens.hasNext()) {
        csv = tokens.next().trim();
      } else if (token.equalsIgnoreCase("-iteration") && tokens.hasNext()) {
        iteration = Integer.parseInt(tokens.next().trim());

        if (iteration <= 0) {
          throw new IllegalStateException("Invalid iteration format");
        }
      } else if (token.equalsIgnoreCase("-count") && tokens.hasNext()) {
        count = Integer.parseInt(tokens.next().trim());

        if (count <= 0) {
          throw new IllegalStateException("Invalid count format");
        }
      } else if (token.equalsIgnoreCase("-size") && tokens.hasNext()) {
        String[] sizeTokens = tokens.next().trim().split(":");

        if (sizeTokens.length != 2) {
          throw new IllegalStateException("Invalid size format");
        }

        minsize = Integer.parseInt(sizeTokens[0].trim());
        maxsize = Integer.parseInt(sizeTokens[1].trim());

        if (minsize <= 0 || maxsize <= 0) {
          throw new IllegalStateException("Invalid size format");
        }
      }
    }

    LOG.debug("Using mode: " + mode);
    switch (mode) {
      case READ:
        LOG.debug("Using input directory: " + inputDirectory);
        LOG.debug("Using output directory: " + outputDirectory);
        LOG.debug("Using csv: " + csv);
        LOG.debug("Using iteration: " + iteration);
        break;
      case WRITE:
        LOG.debug("Using output directory: " + outputDirectory);
        LOG.debug("Using count: " + count);
        LOG.debug("Using minsize: " + minsize);
        LOG.debug("Using maxsize: " + maxsize);
        break;
      default:
        throw new IllegalStateException("Invalid mode");
    }

    return new Parameters(mode, inputDirectory, outputDirectory, csv, iteration, count, minsize, maxsize);
  }

  @Override
  public int run(String[] args) throws Exception {
    // Get parameters
    Parameters parameters = getParameters(args);

    // Get configuration
    Configuration configuration = getConf();

    // Run test mode
    switch (parameters.mode) {
      case READ:
        new ReadMode(parameters, configuration).run();
        break;
      case WRITE:
        new WriteMode(parameters, configuration).run();
        break;
      default:
        throw new IllegalStateException("Invalid mode");
    }

    return 0;
  }

  public static void main(String[] args) {
    if (args.length == 1 && args[0].equalsIgnoreCase("-help")) {
      System.out.print(USAGE);
      System.exit(0);
    }

    try {
      System.exit(ToolRunner.run(new Configuration(), new HadapsTest(), args));
    } catch (Throwable e) {
      LOG.error("Exiting " + HadapsTest.class.getSimpleName() + " due to an exception", e);
      System.exit(-1);
    }
  }

}
