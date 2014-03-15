/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

class File implements Comparable<File> {

  private final String name;
  private final int replFactor;

  File(String name, int replFactor) {
    if (name == null) throw new IllegalArgumentException();
    if (replFactor <= 0) throw new IllegalArgumentException();

    this.name = name;
    this.replFactor = replFactor;
  }

  String getName() {
    return name;
  }

  @Override
  public int compareTo(File o) {
    if (o == null) throw new IllegalArgumentException();

    if (this.replFactor < o.replFactor) {
      return -1;
    } else if (this.replFactor == o.replFactor) {
      return 0;
    } else {
      return 1;
    }
  }

  @Override
  public String toString() {
    return String.format("{Replication Factor: %d, Name: %s}", replFactor, name);
  }

}
