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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@InterfaceAudience.Private
class Hadaps {

  private static final Logger LOG = LoggerFactory.getLogger(Hadaps.class);

  private static final String USAGE = "Usage: java "
      + Hadaps.class.getSimpleName();

  private int run(List<Generation> generations, List<ParameterFile> parameterFiles, Configuration configuration)
      throws IOException, InterruptedException {
    assert generations != null;
    assert parameterFiles != null;
    assert configuration != null;

    LOG.info("Configured DataNodes: " + generations.toString());
    LOG.info("Configured Files: " + parameterFiles.toString());

    long startTime = Time.now();

    Collection<URI> nameNodes = DFSUtil.getNsServiceRpcUris(configuration);
    LOG.info("NameNodes: " + nameNodes);

    // For each NameNode run the balancer
    for (URI nameNode : nameNodes) {
      Balancer balancer = new Balancer(nameNode, generations, parameterFiles, configuration);
      balancer.run();
    }

    long duration = Time.now() - startTime;
    LOG.info("Balancing took " + Utils.getPrettyTime(duration));

    return 0;
  }

  private static class Cli extends Configured implements Tool {
    private static final String HADAPS_CONF_BASE = "hadaps";
    private static final String HADAPS_CONF_GENERATIONS = HADAPS_CONF_BASE + ".generations";
    private static final String HADAPS_CONF_HOSTS = ".hosts";
    private static final String HADAPS_CONF_PRIORITY = ".priority";
    private static final String HADAPS_CONF_FILES = HADAPS_CONF_BASE + ".files";

    @Override
    public int run(String[] args) throws Exception {
      // Parse configuration
      Configuration configuration = getConf();

      List<Generation> generations = parseGenerations(configuration);
      List<ParameterFile> parameterFiles = parseFiles(configuration);

      return new Hadaps().run(generations, parameterFiles, configuration);
    }

    private List<Generation> parseGenerations(Configuration configuration) {
      assert configuration != null;

      String generationsValue = configuration.get(HADAPS_CONF_GENERATIONS);
      if (generationsValue != null) {
        List<Generation> generations = new ArrayList<Generation>();

        // For each generation extract hosts and priority
        String[] generationTokens = generationsValue.split(",");
        for (String generationToken : generationTokens) {
          generationToken = generationToken.trim();
          if (!generationToken.equalsIgnoreCase("")) {

            // Extract hosts
            List<String> hosts = new ArrayList<String>();
            String hostsValue = configuration.get(HADAPS_CONF_BASE + "." + generationToken + HADAPS_CONF_HOSTS);
            if (hostsValue != null) {
              String[] hostTokens = hostsValue.split(",");
              for (String hostToken : hostTokens) {
                hostToken = hostToken.trim();
                if (!hostToken.equalsIgnoreCase("")) {
                  hosts.add(hostToken);
                }
              }

              if (hosts.isEmpty()) {
                throw new IllegalStateException(
                    "No valid hosts configured for generation " + generationToken);
              }
            } else {
              throw new IllegalStateException(
                  "No hosts configured for generation " + generationToken);
            }

            // Extract priority
            int priority = configuration.getInt(
                HADAPS_CONF_BASE + "." + generationToken + HADAPS_CONF_PRIORITY,
                0);
            if (priority <= 0) {
              throw new IllegalStateException(
                  "Invalid or no priority configured for generation "
                      + generationToken);
            }

            generations.add(new Generation(generationToken, hosts, priority));
          }
        }

        if (generations.isEmpty()) {
          throw new IllegalStateException("No valid generations configured");
        }

        Collections.sort(generations);

        return generations;
      } else {
        throw new IllegalStateException("No generations configured");
      }
    }

    private List<ParameterFile> parseFiles(Configuration configuration) {
      assert configuration != null;

      String filesValue = configuration.get(HADAPS_CONF_FILES);
      if (filesValue != null) {
        List<ParameterFile> parameterFiles = new ArrayList<ParameterFile>();

        String[] fileTokens = filesValue.split(",");
        for (String fileToken : fileTokens) {
          fileToken = fileToken.trim();
          if (!fileToken.equalsIgnoreCase("")) {

            String[] tokens = fileToken.split(":", 2);
            if (tokens.length == 2) {

              // Extract replication factor
              short replFactor;
              try {
                replFactor = Short.parseShort(tokens[0].trim());
              } catch (NumberFormatException e) {
                LOG.warn("Invalid format. Skipping token: " + fileToken);
                continue;
              }

              // Extract name
              String name = tokens[1].trim();

              parameterFiles.add(new ParameterFile(name, replFactor));
            } else {
              LOG.warn("Invalid format. Skipping token: " + fileToken);
            }
          }
        }

        if (parameterFiles.isEmpty()) {
          throw new IllegalStateException("No valid files configured");
        }

        Collections.sort(parameterFiles);

        return parameterFiles;
      } else {
        throw new IllegalStateException("No files configured");
      }
    }
  }

  public static void main(String[] args) {
    if (DFSUtil.parseHelpArgument(args, USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      System.exit(ToolRunner.run(new HadapsConfiguration(), new Cli(), args));
    } catch (Throwable e) {
      LOG.error("Exiting " + Hadaps.class.getSimpleName() + " due to an exception", e);
      System.exit(-1);
    }
  }

}
