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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@InterfaceAudience.Private
class Hadaps {

  private static final Logger LOG = LoggerFactory.getLogger(Hadaps.class);

  private static final String USAGE = "Usage: java "
      + Hadaps.class.getSimpleName();

  private static String getPrettyTime(long duration) {
    return String.format(
      "%02d:%02d:%02d.%03d",
      TimeUnit.MILLISECONDS.toHours(duration),
      TimeUnit.MILLISECONDS.toMinutes(duration)
          - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(duration)),
      TimeUnit.MILLISECONDS.toSeconds(duration)
          - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(duration)),
      duration - TimeUnit.SECONDS.toMillis(TimeUnit.MILLISECONDS.toSeconds(duration)));
  }

  private int run(List<Generation> generations, Configuration configuration) {
    assert generations != null;
    assert configuration != null;

    LOG.info("Configured DataNodes: " + generations.toString());

    long startTime = Time.now();

    Collection<URI> nameNodes = DFSUtil.getNsServiceRpcUris(configuration);
    LOG.info("NameNodes: " + nameNodes);

    // For each NameNode run the balancer
    for (URI nameNode : nameNodes) {
      Balancer balancer = new Balancer(nameNode, generations, configuration);
      balancer.run();
    }

    long duration = Time.now() - startTime;
    LOG.info("Balancing took " + getPrettyTime(duration));

    return 0;
  }

  private static class Cli extends Configured implements Tool {
    private static final String HADAPS_CONF_BASE = "hadaps";
    private static final String HADAPS_CONF_GENERATIONS = HADAPS_CONF_BASE + ".generations";
    private static final String HADAPS_CONF_HOSTS = ".hosts";
    private static final String HADAPS_CONF_REPLFACTOR = ".replfactor";

    @Override
    public int run(String[] args) throws Exception {
      // Parse configuration
      Configuration configuration = getConf();

      // Parse generations
      String generationsValue = configuration.get(HADAPS_CONF_GENERATIONS);
      if (generationsValue != null) {
        List<Generation> generations = new ArrayList<Generation>();

        // For each generation extract hosts and replfactor
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

            // Extract replication factor
            int replFactor = configuration.getInt(
                HADAPS_CONF_BASE + "." + generationToken + HADAPS_CONF_REPLFACTOR,
                0);
            if (replFactor <= 0) {
              throw new IllegalStateException(
                  "Invalid or no replication factor configured for generation "
                      + generationToken);
            }

            generations.add(new Generation(generationToken, hosts, replFactor));
          }
        }

        Collections.sort(generations);

        return new Hadaps().run(generations, configuration);
      } else {
        throw new IllegalStateException("No generations configured");
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
