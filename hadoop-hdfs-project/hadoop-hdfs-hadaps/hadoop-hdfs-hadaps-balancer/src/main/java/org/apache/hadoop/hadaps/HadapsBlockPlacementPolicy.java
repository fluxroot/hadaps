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

    Map<ExtendedBlock, List<DatanodeInfo>> blocks = new HashMap<ExtendedBlock, List<DatanodeInfo>>();

    Random random = new Random();

    // Get block locations for file
    LocatedBlocks locatedBlocks = nameNode.getLocatedBlocks(fileStatus);
    for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
      List<DatanodeInfo> targetNodes = new ArrayList<DatanodeInfo>();

      while (targetNodes.size() < locatedBlock.getLocations().length) {
        DatanodeInfo dataNode = dataNodes.get(random.nextInt(dataNodes.size()));
        if (!targetNodes.contains(dataNode)) {
          targetNodes.add(dataNode);
        }
      }

      blocks.put(locatedBlock.getBlock(), targetNodes);
    }

    return blocks;
  }

}
