/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import java.util.List;

class Generation implements Comparable<Generation> {

  private final String name;
  private final List<String> hosts;
  private final int replFactor;

  public Generation(String name, List<String> hosts, int replFactor) {
    this.name = name;
    this.hosts = hosts;
    this.replFactor = replFactor;
  }

  /**
   * We are sorting in descending order!
   */
  @Override
  public int compareTo(Generation o) {
    if (this.replFactor < o.replFactor) {
      return 1;
    } else if (this.replFactor == o.replFactor) {
      return 0;
    } else {
      return -1;
    }
  }

  @Override
  public String toString() {
    return String.format("%s {"
        + "Replication Factor: %d, "
        + "Hosts: %s"
        + "}", name, replFactor, hosts);
  }

}
