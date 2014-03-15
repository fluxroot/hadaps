/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

class BalancerTask implements Callable<BalancerResult> {

  private static final Logger LOG = LoggerFactory.getLogger(BalancerTask.class);

  private final BalancerFile balancerFile;
  private final IBlockPlacementPolicy policy;
  private final DistributedFileSystem dfs;
  private final List<BalancerNode> dataNodes;

  public BalancerTask(BalancerFile balancerFile, IBlockPlacementPolicy policy, DistributedFileSystem dfs, List<BalancerNode> dataNodes) {
    if (balancerFile == null) throw new IllegalArgumentException();
    if (policy == null) throw new IllegalArgumentException();
    if (dfs == null) throw new IllegalArgumentException();
    if (dataNodes == null) throw new IllegalArgumentException();

    this.balancerFile = balancerFile;
    this.policy = policy;
    this.dfs = dfs;
    this.dataNodes = dataNodes;
  }

  @Override
  public BalancerResult call() throws Exception {
    LOG.info("Start balancing file {}", balancerFile.getName());
    System.out.format("Start balancing file %s%n", balancerFile.getName());

    long startTime = Time.now();

    // First set the proper replication factor
    balancerFile.setProperReplication(false);

    // Get list of optimal target nodes
    List<BalancerNode> targetNodes = policy.chooseTarget(balancerFile);

    // Get locations of all blocks
    BlockLocation[] blockLocations = balancerFile.getBlockLocations();

    // Now balance each block
    for (BlockLocation blockLocation : blockLocations) {
      List<BalancerNode> currentNodes = new ArrayList<BalancerNode>();
      for (BalancerNode dataNode : dataNodes) {
        for (String host : blockLocation.getNames()) {
          if (dataNode.getName().equals(host)) {
            currentNodes.add(dataNode);
          }
        }
      }

      balance(currentNodes, targetNodes);
    }

    long duration = Time.now() - startTime;

    LOG.info("Balanced file {} in {}", balancerFile.getName(), Utils.getPrettyTime(duration));
    System.out.format("Balanced file %s in %s%n", balancerFile.getName(), Utils.getPrettyTime(duration));

    return new BalancerResult();
  }

  private void balance(List<BalancerNode> currentNodes, List<BalancerNode> targetNodes) throws IOException {
    assert currentNodes != null;
    assert targetNodes != null;

    LOG.debug("Block locations: " + currentNodes.toString());
    LOG.debug("Target nodes: " + targetNodes.toString());

//    for (BalancerNode node : targetNodes) {
//      if (node.hasBlock(blockLocation.)) {
//        targetNodes.remove(node);
//      }
//    }
//
//    System.out.format("%s, %s, %s%n", blockLocation.toString(), Arrays.toString(blockLocation.getHosts()), targetNodes.toString());

//    blockLocation.
//
//    Socket socket = new Socket();
//    DataOutputStream outputStream = null;
//    DataInputStream inputStream = null;
//
//    try {
//      socket.connect(NetUtils.createSocketAddr());
//    } finally {
//      IOUtils.closeStream(outputStream);
//      IOUtils.closeStream(inputStream);
//      IOUtils.closeStream(socket);
//    }
  }

}
