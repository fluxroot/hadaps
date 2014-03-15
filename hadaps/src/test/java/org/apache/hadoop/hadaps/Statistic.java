/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

class Statistic {

  private final int iteration;
  private final String filename;
  private final short replication;
  private final long size;
  private final long duration;

  Statistic(int iteration, String filename, short replication, long size, long duration) {
    this.iteration = iteration;
    this.filename = filename;
    this.replication = replication;
    this.size = size;
    this.duration = duration;
  }

  public int getIteration() {
    return iteration;
  }

  public String getFilename() {
    return filename;
  }

  public short getReplication() {
    return replication;
  }

  public long getSize() {
    return size;
  }

  public long getDuration() {
    return duration;
  }

}
