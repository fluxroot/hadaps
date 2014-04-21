/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

class BalancerFile implements Comparable<BalancerFile> {

  private static final Logger LOG = LoggerFactory.getLogger(BalancerFile.class);

  private FileStatus fileStatus;
  private final ParameterFile parameterFile;
  private final FileSystem fileSystem;

  BalancerFile(FileStatus fileStatus, ParameterFile parameterFile, FileSystem fileSystem) {
    if (fileStatus == null) throw new IllegalArgumentException();
    if (parameterFile == null) throw new IllegalArgumentException();
    if (fileSystem == null) throw new IllegalArgumentException();

    this.fileStatus = fileStatus;
    this.parameterFile = parameterFile;
    this.fileSystem = fileSystem;
  }

  FileStatus getFileStatus() {
    return fileStatus;
  }

  String getName() {
    return fileStatus.getPath().toString();
  }

  void setProperReplication(boolean verify) throws IOException, InterruptedException {
    LOG.info("Setting replication for {} to {}", getName(), parameterFile.getReplication());
    fileSystem.setReplication(fileStatus.getPath(), parameterFile.getReplication());

    if (verify) {
      LOG.info("Verifying replication for {}", getName());
      boolean done = false;
      while (!done) {
        done = true;

        // Refresh fileStatus
        fileStatus = fileSystem.getFileStatus(fileStatus.getPath());

        // For each block location, check number of hosts
        BlockLocation[] locations = fileSystem.getFileBlockLocations(fileStatus.getPath(), 0, fileStatus.getLen());
        for (BlockLocation location : locations) {
          if (location.getHosts().length != parameterFile.getReplication()) {
            LOG.info("Waiting for replication to adjust for {}", getName());
            done = false;
            break;
          }
        }

        if (!done) {
          Thread.sleep(1000);
        }
      }
    }
  }

  @Override
  public int compareTo(BalancerFile o) {
    if (o == null) throw new IllegalArgumentException();

    return this.parameterFile.compareTo(o.parameterFile);
  }

  @Override
  public String toString() {
    return String.format("{Replication: %d, Path: %s}", fileStatus.getReplication(), getName());
  }

}
