/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

class BalancerNode {

  private final DatanodeInfo dataNode;

  public BalancerNode(DatanodeInfo dataNode) {
    if (dataNode == null) throw new IllegalArgumentException();

    this.dataNode = dataNode;
  }

  public String getName() {
    return dataNode.getName();
  }

  @Override
  public String toString() {
    return getName();
  }

}
