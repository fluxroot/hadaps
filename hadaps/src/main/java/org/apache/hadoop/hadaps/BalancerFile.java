/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.fs.FileStatus;

class BalancerFile implements Comparable<BalancerFile> {

  private final FileStatus status;
  private final File file;

  BalancerFile(FileStatus status, File file) {
    if (status == null) throw new IllegalArgumentException();
    if (file == null) throw new IllegalArgumentException();

    this.status = status;
    this.file = file;
  }

  @Override
  public int compareTo(BalancerFile o) {
    if (o == null) throw new IllegalArgumentException();

    return this.file.compareTo(o.file);
  }

  @Override
  public String toString() {
    return String.format("Replication Factor: %d, Path: %s",
        status.getReplication(), status.getPath().toString());
  }

}
