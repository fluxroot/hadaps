/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

class BalancerDataNode implements Comparable<BalancerDataNode> {

  private final DatanodeInfo dataNode;
  private float weight;

  BalancerDataNode(DatanodeInfo dataNode, float weight) {
    if (dataNode == null) throw new IllegalArgumentException();
    if (weight <= 0) throw new IllegalArgumentException();

    this.dataNode = dataNode;
    this.weight = weight;
  }

  DatanodeInfo getDataNode() {
    return dataNode;
  }

  float getWeight() {
    return weight;
  }

  void setWeight(float weight) {
    this.weight = weight;
  }

  @Override
  public int compareTo(BalancerDataNode o) {
    if (o == null) throw new IllegalArgumentException();

    if (this.weight > o.weight) {
      return -1;
    } else if (this.weight < o.weight) {
      return 1;
    } else {
      return Float.compare(this.dataNode.getDfsUsedPercent(), o.dataNode.getDfsUsedPercent());
    }
  }

}
