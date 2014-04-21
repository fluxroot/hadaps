/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.hdfs.server.namenode.FSClusterStats;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

public class HadapsBlockPlacementPolicy extends BlockPlacementPolicy {

  private FSClusterStats stats = null;
  private NetworkTopology clusterMap = null;

  private final Map<String, ParameterGeneration> generationMap = new HashMap<String, ParameterGeneration>();
  private DistributedFileSystem fileSystem = null;
  private BalancerNameNode nameNode = null;

  protected HadapsBlockPlacementPolicy() {
  }

  protected HadapsBlockPlacementPolicy(Configuration conf, FSClusterStats stats, NetworkTopology clusterMap) {
    initialize(conf, stats, clusterMap);
  }

  @Override
  protected void initialize(Configuration conf, FSClusterStats stats, NetworkTopology clusterMap) {
    this.stats = stats;
    this.clusterMap = clusterMap;

    try {
      // Get generations
      List<ParameterGeneration> generations = HadapsConfiguration.parseGenerations(conf);
      for (ParameterGeneration generation : generations) {
        for (InetAddress host : generation.getHosts()) {
          generationMap.put(host.getHostAddress(), generation);
        }
      }

      // Get distributed filesystem
      FileSystem fs = FileSystem.get(conf);
      if (!(fs instanceof DistributedFileSystem)) {
        throw new IllegalStateException("Filesystem " + fs.getUri() + " is not an HDFS filesystem");
      }
      fileSystem = (DistributedFileSystem) fs;

      // Create name node
      nameNode = new BalancerNameNode(fileSystem);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Map<ExtendedBlock, List<DatanodeInfo>> chooseTarget(FileStatus fileStatus) throws IOException {
    if (fileStatus == null) throw new IllegalArgumentException();

    // Populate extended block list
    List<ExtendedBlock> extendedBlocks = new ArrayList<ExtendedBlock>();
    for (LocatedBlock block : nameNode.getLocatedBlocks(fileStatus).getLocatedBlocks()) {
      extendedBlocks.add(block.getBlock());
    }

    // Populate data node list
    List<BalancerDataNode> dataNodes = new ArrayList<BalancerDataNode>();
    for (DatanodeInfo node : fileSystem.getDataNodeStats(HdfsConstants.DatanodeReportType.LIVE)) {
      if (node.isDecommissioned() || node.isDecommissionInProgress()) {
        continue;
      }

      ParameterGeneration generation = generationMap.get(node.getIpAddr());
      if (generation != null) {
        dataNodes.add(new BalancerDataNode(node, generation.getWeight()));
      }
    }

    Collections.sort(dataNodes);

    // Initialize weight factors
    float totalWeight = 0.0f;
    for (BalancerDataNode node : dataNodes) {
      totalWeight += node.getWeight();
    }

    float baseWeight = extendedBlocks.size() / totalWeight;

    for (BalancerDataNode node : dataNodes) {
      node.setWeight((float) Math.ceil(node.getWeight() * baseWeight));
    }

    // Initialize node map
    Map<BalancerDataNode, List<ExtendedBlock>> nodeMap = new HashMap<BalancerDataNode, List<ExtendedBlock>>();
    for (BalancerDataNode node : dataNodes) {
      nodeMap.put(node, new ArrayList<ExtendedBlock>());
    }

    // Iterate over replication factor
    for (int replication = 0; replication < fileStatus.getReplication(); ++replication) {
      List<BalancerDataNode> nodes = new ArrayList<BalancerDataNode>(dataNodes);
      List<ExtendedBlock> blocks = new ArrayList<ExtendedBlock>(extendedBlocks);

      Map<BalancerDataNode, Float> weightMap = new HashMap<BalancerDataNode, Float>();
      for (BalancerDataNode node : dataNodes) {
        weightMap.put(node, node.getWeight());
      }

      // Iterate over all blocks
      Iterator<ExtendedBlock> blockIter = blocks.iterator();
      while (!blocks.isEmpty()) {
        if (!blockIter.hasNext()) {
          blockIter = blocks.iterator();
          nodes = new ArrayList<BalancerDataNode>(dataNodes);
        }

        ExtendedBlock block = blockIter.next();

        // Populate our node list with all data nodes
        if (nodes.isEmpty()) {
          for (BalancerDataNode node : dataNodes) {
            if (weightMap.get(node) > 0) {
              nodes.add(node);
            }
          }
        }

        // Loop over remaining nodes
        for (BalancerDataNode node : nodes) {
          List<ExtendedBlock> list = nodeMap.get(node);
          if (!list.contains(block)) {
            // We've found a valid node
            list.add(block);
            nodes.remove(node);
            blocks.remove(block);
            weightMap.put(node, weightMap.get(node) - 1);

            blockIter = blocks.iterator();
            break;
          }
        }
      }
    }

    // Generate block map
    Map<ExtendedBlock, List<DatanodeInfo>> blockMap = new HashMap<ExtendedBlock, List<DatanodeInfo>>();
    for (Map.Entry<BalancerDataNode, List<ExtendedBlock>> entry : nodeMap.entrySet()) {
      for (ExtendedBlock block : entry.getValue()) {
        List<DatanodeInfo> nodes = blockMap.get(block);
        if (nodes == null) {
          nodes = new ArrayList<DatanodeInfo>();
          blockMap.put(block, nodes);
        }

        nodes.add(entry.getKey().getDataNode());
      }
    }

    return blockMap;
  }

  @Override
  public DatanodeStorageInfo[] chooseTarget(String srcPath,
      int numOfReplicas,
      Node writer,
      List<DatanodeStorageInfo> chosen,
      boolean returnChosenNodes,
      Set<Node> excludedNodes,
      long blocksize,
      StorageType storageType) {
    // TODO
    return new DatanodeStorageInfo[0];
  }

  @Override
  public BlockPlacementStatus verifyBlockPlacement(String srcPath,
      LocatedBlock lBlk,
      int numOfReplicas) {
    // TODO
    return null;
  }

  @Override
  public DatanodeDescriptor chooseReplicaToDelete(BlockCollection srcBC,
      Block block,
      short replicationFactor,
      Collection<DatanodeDescriptor> existingReplicas,
      Collection<DatanodeDescriptor> moreExistingReplicas) {
    // TODO
    return null;
  }

}
