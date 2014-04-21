/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import java.io.IOException;
import java.util.*;

class HadapsBlockPlacementPolicy implements IBlockPlacementPolicy {

  private final List<DatanodeInfo> dataNodes;
  private final List<ParameterGeneration> parameterGenerations;
  private final BalancerNameNode nameNode;

  public HadapsBlockPlacementPolicy(List<DatanodeInfo> dataNodes, List<ParameterGeneration> parameterGenerations,
      BalancerNameNode nameNode) {
    if (dataNodes == null) throw new IllegalArgumentException();
    if (parameterGenerations == null) throw new IllegalArgumentException();
    if (nameNode == null) throw new IllegalArgumentException();

    this.dataNodes = dataNodes;
    this.parameterGenerations = parameterGenerations;
    this.nameNode = nameNode;
  }

  @Override
  public Map<ExtendedBlock, List<DatanodeInfo>> chooseTarget(FileStatus fileStatus) throws IOException {
    if (fileStatus == null) throw new IllegalArgumentException();

    Map<ExtendedBlock, List<DatanodeInfo>> blockMap = new HashMap<ExtendedBlock, List<DatanodeInfo>>();

    Random random = new Random();

    // Initialize blockMap
    LocatedBlocks locatedBlocks = nameNode.getLocatedBlocks(fileStatus);
    for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
      blockMap.put(locatedBlock.getBlock(), new ArrayList<DatanodeInfo>());
    }

    for (int replication = 0; replication < fileStatus.getReplication(); ++replication) {
      // Iterate over all blocks and distribute them evenly across all dataNodes
      for (ExtendedBlock extendedBlock : blockMap.keySet()) {
        // Initialize targetNodes with all available dataNodes
        List<DatanodeInfo> targetNodes = new ArrayList<DatanodeInfo>(dataNodes);

        // Get the current list of target nodes for this block
        List<DatanodeInfo> blockTargetNodes = blockMap.get(extendedBlock);

        // Pick a random dataNode
        DatanodeInfo dataNode = targetNodes.get(random.nextInt(targetNodes.size()));
        while (blockTargetNodes.contains(dataNode)) {
          dataNode = targetNodes.get(random.nextInt(targetNodes.size()));
        }

        blockTargetNodes.add(dataNode);
        targetNodes.remove(dataNode);
      }
    }

    return blockMap;
  }

}
