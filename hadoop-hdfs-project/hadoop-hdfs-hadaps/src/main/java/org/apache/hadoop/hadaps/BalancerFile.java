/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

class BalancerFile implements Comparable<BalancerFile> {

  private static final Logger LOG = LoggerFactory.getLogger(BalancerFile.class);

  private FileStatus status;
  private final ParameterFile parameterFile;
  private final DistributedFileSystem fileSystem;

  BalancerFile(FileStatus status, ParameterFile parameterFile, DistributedFileSystem fileSystem) {
    if (status == null) throw new IllegalArgumentException();
    if (parameterFile == null) throw new IllegalArgumentException();
    if (fileSystem == null) throw new IllegalArgumentException();

    this.status = status;
    this.parameterFile = parameterFile;
    this.fileSystem = fileSystem;
  }

  String getName() {
    return status.getPath().toString();
  }

  BlockLocation[] getBlockLocations() throws IOException {
    return fileSystem.getFileBlockLocations(status, 0, status.getLen());
  }

  short getReplication() {
    return status.getReplication();
  }

  void setProperReplication(boolean wait) throws IOException, InterruptedException {
    if (status.getReplication() != parameterFile.getReplication()) {
      LOG.info("Setting replication for {} to {}", getName(), parameterFile.getReplication());
      fileSystem.setReplication(status.getPath(), parameterFile.getReplication());

      if (wait) {
        LOG.info("Waiting for replication to adjust for {}", getName());
        boolean done = false;
        while (!done) {
          done = true;

          // Refresh status
          status = fileSystem.getFileStatus(status.getPath());

          // For each block location, check number of hosts
          BlockLocation[] locations = fileSystem.getFileBlockLocations(status, 0, status.getLen());
          for (BlockLocation location : locations) {
            if (location.getHosts().length != parameterFile.getReplication()) {
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
  }

  @Override
  public int compareTo(BalancerFile o) {
    if (o == null) throw new IllegalArgumentException();

    return this.parameterFile.compareTo(o.parameterFile);
  }

  @Override
  public String toString() {
    return String.format("{Replication Factor: %d, Path: %s}",
        status.getReplication(), getName());
  }

}
