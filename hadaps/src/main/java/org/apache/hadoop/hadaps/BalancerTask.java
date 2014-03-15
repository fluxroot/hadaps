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
import java.util.List;
import java.util.concurrent.Callable;

class BalancerTask implements Callable<BalancerResult> {

  private static final Logger LOG = LoggerFactory.getLogger(BalancerTask.class);

  private final BalancerFile balancerFile;
  private final IBlockPlacementPolicy policy;
  private final DistributedFileSystem dfs;

  public BalancerTask(BalancerFile balancerFile, IBlockPlacementPolicy policy, DistributedFileSystem dfs) {
    if (balancerFile == null) throw new IllegalArgumentException();
    if (policy == null) throw new IllegalArgumentException();
    if (dfs == null) throw new IllegalArgumentException();

    this.balancerFile = balancerFile;
    this.policy = policy;
    this.dfs = dfs;
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
      balance(blockLocation, targetNodes);
    }

    long duration = Time.now() - startTime;

    LOG.info("Balanced file {} in {}", balancerFile.getName(), Utils.getPrettyTime(duration));
    System.out.format("Balanced file %s in %s%n", balancerFile.getName(), Utils.getPrettyTime(duration));

    return new BalancerResult();
  }

  private void balance(BlockLocation blockLocation, List<BalancerNode> targetNodes) throws IOException {
    assert blockLocation != null;
    assert targetNodes != null;

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
