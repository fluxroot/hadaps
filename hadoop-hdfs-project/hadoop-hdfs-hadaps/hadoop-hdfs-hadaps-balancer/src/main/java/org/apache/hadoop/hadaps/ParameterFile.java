/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

class ParameterFile implements Comparable<ParameterFile> {

  private final String name;
  private final short replication;

  ParameterFile(String name, short replication) {
    if (name == null) throw new IllegalArgumentException();
    if (replication <= 0) throw new IllegalArgumentException();

    this.name = name;
    this.replication = replication;
  }

  String getName() {
    return name;
  }

  short getReplication() {
    return replication;
  }

  @Override
  public int compareTo(ParameterFile o) {
    if (o == null) throw new IllegalArgumentException();

    return Short.compare(this.replication, o.replication);
  }

  @Override
  public String toString() {
    return String.format("{Replication: %d, Name: %s}", replication, name);
  }

}
