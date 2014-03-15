/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import java.util.List;

class ParameterGeneration implements Comparable<ParameterGeneration> {

  private final String name;
  private final List<String> hosts;
  private final int priority;

  ParameterGeneration(String name, List<String> hosts, int priority) {
    if (name == null) throw new IllegalArgumentException();
    if (hosts == null) throw new IllegalArgumentException();
    if (priority <= 0) throw new IllegalArgumentException();

    this.name = name;
    this.hosts = hosts;
    this.priority = priority;
  }

  @Override
  public int compareTo(ParameterGeneration o) {
    if (o == null) throw new IllegalArgumentException();

    if (this.priority < o.priority) {
      return -1;
    } else if (this.priority == o.priority) {
      return 0;
    } else {
      return 1;
    }
  }

  @Override
  public String toString() {
    return String.format("%s {"
        + "Priority: %d, "
        + "Hosts: %s"
        + "}", name, priority, hosts);
  }

}
