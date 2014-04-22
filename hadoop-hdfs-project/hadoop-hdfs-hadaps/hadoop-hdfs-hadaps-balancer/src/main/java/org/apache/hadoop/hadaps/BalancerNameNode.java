/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import java.io.IOException;

class BalancerNameNode {

  private final ClientProtocol clientProtocol;

  BalancerNameNode(DistributedFileSystem fileSystem) {
    if (fileSystem == null) throw new IllegalArgumentException();

    // Get ClientProtocol
    clientProtocol = fileSystem.getClient().getNamenode();
  }

  LocatedBlocks getLocatedBlocks(FileStatus fileStatus) throws IOException {
    if (fileStatus == null) throw new IllegalArgumentException();

    return clientProtocol.getBlockLocations(fileStatus.getPath().toUri().getPath(), 0, fileStatus.getLen());
  }

}
