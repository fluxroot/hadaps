/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

class HadapsBlockPlacementPolicy implements IBlockPlacementPolicy {

  private final DistributedFileSystem dfs;
  private final List<ParameterGeneration> parameterGenerations;

  private final List<BalancerGeneration> generations = new ArrayList<BalancerGeneration>();
  private final List<BalancerNode> nodes = new ArrayList<BalancerNode>();

  public HadapsBlockPlacementPolicy(DistributedFileSystem dfs, List<ParameterGeneration> parameterGenerations) {
    if (dfs == null) throw new IllegalArgumentException();
    if (parameterGenerations == null) throw new IllegalArgumentException();

    this.dfs = dfs;
    this.parameterGenerations = parameterGenerations;
  }

  @Override
  public void initialize(Configuration configuration) throws IOException {
    // Populate nodes
    DatanodeInfo[] dataNodes = dfs.getDataNodeStats(HdfsConstants.DatanodeReportType.LIVE);

    for (DatanodeInfo dataNode : dataNodes) {
      if (dataNode.isDecommissioned() || dataNode.isDecommissionInProgress()) {
        continue;
      }

      nodes.add(new BalancerNode(dataNode));
    }

    // Populate generations
    for (ParameterGeneration parameterGeneration : parameterGenerations) {
      generations.add(new BalancerGeneration(parameterGeneration));
    }
  }

  @Override
  public List<BalancerNode> chooseTarget(BalancerFile balancerFile) {
    if (balancerFile == null) throw new IllegalArgumentException();

    Random random = new Random();
    List<BalancerNode> targetNodes = new ArrayList<BalancerNode>();

    for (int i = 0; i < balancerFile.getReplication(); ++i) {
      BalancerNode node;
      do {
        int index = random.nextInt(nodes.size());
        node = nodes.get(index);
      } while (targetNodes.contains(node));

      targetNodes.add(node);
    }

    return targetNodes;
  }

}
