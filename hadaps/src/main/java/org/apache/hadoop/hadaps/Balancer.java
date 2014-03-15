/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class Balancer {

  private static final Logger LOG = LoggerFactory.getLogger(Balancer.class);

  private final URI nameNode;
  private final List<Generation> generations;
  private final List<File> files;
  private final Configuration configuration;

  Balancer(URI nameNode, List<Generation> generations, List<File> files, Configuration configuration) {
    if (nameNode == null) throw new IllegalArgumentException();
    if (generations == null) throw new IllegalArgumentException();
    if (files == null) throw new IllegalArgumentException();
    if (configuration == null) throw new IllegalArgumentException();

    this.nameNode = nameNode;
    this.generations = generations;
    this.files = files;
    this.configuration = configuration;
  }

  void run() throws IOException {
    // Populate balancer files
    List<BalancerFile> balancerFiles = getBalancerFiles();

    // Now balance each file
    for (BalancerFile balancerFile : balancerFiles) {
      balance(balancerFile);
    }
  }

  private void balance(BalancerFile balancerFile) throws IOException {
    assert balancerFile != null;

    // Check whether the replication factor matches
    if (!balancerFile.hasProperReplication()) {
      balancerFile.setProperReplication();
    }
  }

  private List<BalancerFile> getBalancerFiles() throws IOException {
    List<BalancerFile> balancerFiles = new ArrayList<BalancerFile>();

    // Iterate over each pattern
    for (File file : files) {
      Path globPath = new Path(file.getName());
      FileSystem fileSystem = globPath.getFileSystem(configuration);
      FileStatus[] stats = fileSystem.globStatus(globPath);

      if (stats != null && stats.length > 0) {
        // We have some matching paths

        for (FileStatus stat : stats) {
          populateBalancerFiles(balancerFiles, stat, file, fileSystem);
        }

        Collections.sort(balancerFiles);

        LOG.info("Matching files for pattern \"{}\": {}", globPath.toString(), balancerFiles);
      } else {
        LOG.info("No matching files for pattern \"{}\"", globPath.toString());
      }
    }

    return balancerFiles;
  }

  private void populateBalancerFiles(
      List<BalancerFile> balancerFiles, FileStatus status, File file, FileSystem fileSystem) throws IOException {
    assert balancerFiles != null;
    assert status != null;
    assert file != null;
    assert fileSystem != null;

    if (status.isFile()) {
      balancerFiles.add(new BalancerFile(status, file, fileSystem));
    } else if (status.isDirectory()) {
      // Recurse into directory

      FileStatus[] stats = fileSystem.listStatus(status.getPath());
      for (FileStatus stat : stats) {
        populateBalancerFiles(balancerFiles, stat, file, fileSystem);
      }
    }
  }

}
