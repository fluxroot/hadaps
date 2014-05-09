/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

class ParameterGeneration implements Comparable<ParameterGeneration> {

  private final String name;
  private final List<InetAddress> hosts = new ArrayList<InetAddress>();
  private final float weight;

  ParameterGeneration(String name, List<String> hosts, float weight) throws UnknownHostException {
    if (name == null) throw new IllegalArgumentException();
    if (hosts == null) throw new IllegalArgumentException();
    if (weight <= 0) throw new IllegalArgumentException();

    this.name = name;
    this.weight = weight;

    for (String host : hosts) {
      this.hosts.add(InetAddress.getByName(host));
    }
  }

  List<InetAddress> getHosts() {
    return hosts;
  }

  float getWeight() {
    return weight;
  }

  @Override
  public int compareTo(ParameterGeneration o) {
    if (o == null) throw new IllegalArgumentException();

    return Float.compare(this.weight, o.weight);
  }

  @Override
  public String toString() {
    return String.format("%s {Weight: %.1f, Hosts: %s}", name, weight, hosts);
  }

}
