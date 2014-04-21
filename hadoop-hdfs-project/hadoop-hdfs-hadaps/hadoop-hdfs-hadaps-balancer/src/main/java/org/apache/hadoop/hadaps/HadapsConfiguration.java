/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class HadapsConfiguration extends HdfsConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(HadapsConfiguration.class);

  private static final String HADAPS_CONF_BASE = "hadaps";
  private static final String HADAPS_CONF_GENERATIONS = HADAPS_CONF_BASE + ".generations";
  private static final String HADAPS_CONF_HOSTS = ".hosts";
  private static final String HADAPS_CONF_WEIGHT = ".weight";
  private static final String HADAPS_CONF_FILES = HADAPS_CONF_BASE + ".files";

  static {
    Configuration.addDefaultResource("hadaps.xml");
  }

  static List<ParameterGeneration> parseGenerations(Configuration configuration) throws UnknownHostException {
    assert configuration != null;

    String generationsValue = configuration.get(HADAPS_CONF_GENERATIONS);
    if (generationsValue == null) {
      throw new IllegalStateException("No generations configured");
    }

    List<ParameterGeneration> parameterGenerations = new ArrayList<ParameterGeneration>();

    // For each generation extract hosts and weight
    String[] generationTokens = generationsValue.split(",");
    for (String generationToken : generationTokens) {
      generationToken = generationToken.trim();
      if (!generationToken.equalsIgnoreCase("")) {

        // Extract hosts
        List<String> hosts = new ArrayList<String>();
        String hostsValue = configuration.get(HADAPS_CONF_BASE + "." + generationToken + HADAPS_CONF_HOSTS);
        if (hostsValue == null) {
          throw new IllegalStateException("No hosts configured for generation " + generationToken);
        }

        String[] hostTokens = hostsValue.split(",");
        for (String hostToken : hostTokens) {
          hostToken = hostToken.trim();
          if (!hostToken.equalsIgnoreCase("")) {
            hosts.add(hostToken);
          }
        }

        if (hosts.isEmpty()) {
          throw new IllegalStateException("No valid hosts configured for generation " + generationToken);
        }

        // Extract weight
        float weight = configuration.getInt(HADAPS_CONF_BASE + "." + generationToken + HADAPS_CONF_WEIGHT, 0);
        if (weight <= 0) {
          throw new IllegalStateException("Invalid or no weight configured for generation " + generationToken);
        }

        parameterGenerations.add(new ParameterGeneration(generationToken, hosts, weight));
      }
    }

    if (parameterGenerations.isEmpty()) {
      throw new IllegalStateException("No valid generations configured");
    }

    Collections.sort(parameterGenerations);

    LOG.info("Configured DataNodes: {}", parameterGenerations.toString());

    return parameterGenerations;
  }

  static List<ParameterFile> parseFiles(Configuration configuration) {
    assert configuration != null;

    String filesValue = configuration.get(HADAPS_CONF_FILES);
    if (filesValue == null) {
      throw new IllegalStateException("No files configured");
    }

    List<ParameterFile> parameterFiles = new ArrayList<ParameterFile>();

    String[] fileTokens = filesValue.split(",");
    for (String fileToken : fileTokens) {
      fileToken = fileToken.trim();
      if (!fileToken.equalsIgnoreCase("")) {

        String[] tokens = fileToken.split(":", 2);
        if (tokens.length == 2) {

          // Extract replication factor
          short replication;
          try {
            replication = Short.parseShort(tokens[0].trim());
          } catch (NumberFormatException e) {
            LOG.warn("Invalid format. Skipping token: {}", fileToken);
            continue;
          }

          // Extract name
          String name = tokens[1].trim();

          parameterFiles.add(new ParameterFile(name, replication));
        } else {
          LOG.warn("Invalid format. Skipping token: {}", fileToken);
        }
      }
    }

    if (parameterFiles.isEmpty()) {
      throw new IllegalStateException("No valid files configured");
    }

    Collections.sort(parameterFiles);

    LOG.info("Configured Files: {}", parameterFiles.toString());

    return parameterFiles;
  }

}
