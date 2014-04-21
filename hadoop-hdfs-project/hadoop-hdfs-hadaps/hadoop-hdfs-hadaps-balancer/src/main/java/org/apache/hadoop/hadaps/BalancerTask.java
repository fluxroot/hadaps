/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

class BalancerTask implements Callable<Integer> {

  private static final Logger LOG = LoggerFactory.getLogger(BalancerTask.class);

  private final BalancerFile balancerFile;
  private final IBlockPlacementPolicy policy;
  private final BalancerNameNode nameNode;

  BalancerTask(BalancerFile balancerFile, IBlockPlacementPolicy policy, BalancerNameNode nameNode) {
    if (balancerFile == null) throw new IllegalArgumentException();
    if (policy == null) throw new IllegalArgumentException();
    if (nameNode == null) throw new IllegalArgumentException();

    this.balancerFile = balancerFile;
    this.policy = policy;
    this.nameNode = nameNode;
  }

  @Override
  public Integer call() throws Exception {
    LOG.info("Start balancing file {}", balancerFile.getName());

    long startTime = Time.now();

    // First set the proper replication factor
    balancerFile.setProperReplication(false);

    // Get locations of all blocks
    LocatedBlocks locatedBlocks = nameNode.getLocatedBlocks(balancerFile.getFileStatus());

    // Get list of optimal target nodes
    Map<ExtendedBlock, List<DatanodeInfo>> blockMap = policy.chooseTarget(balancerFile.getFileStatus());

    // Now balance each block
    for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {

      // Populate currentNodes
      List<DatanodeInfo> currentNodes = new ArrayList<DatanodeInfo>(Arrays.asList(locatedBlock.getLocations()));

      balance(locatedBlock.getBlock(), currentNodes, blockMap.get(locatedBlock.getBlock()));
    }

    long duration = Time.now() - startTime;
    LOG.info("Balanced file {} in {}", balancerFile.getName(), Utils.getPrettyTime(duration));

    return 0;
  }

  private void balance(ExtendedBlock extendedBlock, List<DatanodeInfo> currentNodes, List<DatanodeInfo> targetNodes)
      throws IOException {
    assert extendedBlock != null;
    assert currentNodes != null;
    assert targetNodes != null;

    LOG.debug("Current nodes for block {}: {}", extendedBlock.getBlockId(), currentNodes.toString());
    LOG.debug("Target nodes for block {}: {}", extendedBlock.getBlockId(), targetNodes.toString());

    List<DatanodeInfo> copyTargetNodes = new ArrayList<DatanodeInfo>(targetNodes);
    List<DatanodeInfo> deleteTargetNodes = new ArrayList<DatanodeInfo>(currentNodes);

    copyTargetNodes.removeAll(currentNodes);
    deleteTargetNodes.removeAll(targetNodes);

    LOG.debug("Block {} missing on nodes: {}", extendedBlock.getBlockId(), copyTargetNodes.toString());
    LOG.debug("Block {} to be deleted on nodes: {}", extendedBlock.getBlockId(), deleteTargetNodes.toString());

    // Transfer block to target nodes
    if (copyTargetNodes.size() != deleteTargetNodes.size()) {
      throw new IllegalStateException("Source and target nodes size mismatch for block " + extendedBlock.getBlockId());
    }
    // TODO: Select nearest source for copy
    for (int i = 0; i < copyTargetNodes.size(); ++i) {
      balance(extendedBlock, deleteTargetNodes.get(i), copyTargetNodes.get(i));
    }
  }

  private void balance(ExtendedBlock extendedBlock, DatanodeInfo sourceNode, DatanodeInfo targetNode)
      throws IOException {
    assert extendedBlock != null;
    assert sourceNode != null;
    assert targetNode != null;

    Socket socket = new Socket();
    DataOutputStream outputStream = null;
    DataInputStream inputStream = null;
    try {
      // Open stream to target node
      socket.connect(NetUtils.createSocketAddr(targetNode.getXferAddr()), HdfsServerConstants.READ_TIMEOUT);
      socket.setSoTimeout(HdfsServerConstants.READ_TIMEOUT);
      socket.setKeepAlive(true);

      OutputStream unbufferedOutputStream = socket.getOutputStream();
      InputStream unbufferedInputStream = socket.getInputStream();

      // TODO: Extend for encryption

      outputStream = new DataOutputStream(new BufferedOutputStream(
          unbufferedOutputStream, HdfsConstants.IO_FILE_BUFFER_SIZE));
      inputStream = new DataInputStream((new BufferedInputStream(
          unbufferedInputStream, HdfsConstants.IO_FILE_BUFFER_SIZE)));

      // Send request
      new Sender(outputStream).replaceBlock(
          extendedBlock, BlockTokenSecretManager.DUMMY_TOKEN, sourceNode.getDatanodeUuid(), sourceNode);

      // Receive response
      DataTransferProtos.BlockOpResponseProto response = DataTransferProtos.BlockOpResponseProto.parseFrom(
          PBHelper.vintPrefixed(inputStream));
      if (response.getStatus() != DataTransferProtos.Status.SUCCESS) {
        throw new IOException("Block move failed: " + response.getMessage());
      }
    } finally {
      IOUtils.closeStream(outputStream);
      IOUtils.closeStream(inputStream);
      IOUtils.closeStream(socket);
    }
  }

}
