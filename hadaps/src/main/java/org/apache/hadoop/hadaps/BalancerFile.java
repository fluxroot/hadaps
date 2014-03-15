/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

class BalancerFile implements Comparable<BalancerFile> {

  private final FileStatus status;
  private final File file;
  private final FileSystem fileSystem;

  BalancerFile(FileStatus status, File file, FileSystem fileSystem) {
    if (status == null) throw new IllegalArgumentException();
    if (file == null) throw new IllegalArgumentException();
    if (fileSystem == null) throw new IllegalArgumentException();

    this.status = status;
    this.file = file;
    this.fileSystem = fileSystem;
  }

  String getName() {
    return status.getPath().toString();
  }

  boolean hasProperReplication() {
    return status.getReplication() == file.getReplication();
  }

  void setProperReplication() throws IOException {
    fileSystem.setReplication(status.getPath(), file.getReplication());
  }

  @Override
  public int compareTo(BalancerFile o) {
    if (o == null) throw new IllegalArgumentException();

    return this.file.compareTo(o.file);
  }

  @Override
  public String toString() {
    return String.format("{Replication Factor: %d, Path: %s}",
        status.getReplication(), status.getPath().toString());
  }

}
