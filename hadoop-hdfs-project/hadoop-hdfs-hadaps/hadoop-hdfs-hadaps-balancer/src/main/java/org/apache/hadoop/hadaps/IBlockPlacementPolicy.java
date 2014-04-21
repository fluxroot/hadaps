/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

import java.io.IOException;
import java.util.List;
import java.util.Map;

interface IBlockPlacementPolicy {

  Map<ExtendedBlock, List<DatanodeInfo>> chooseTarget(FileStatus fileStatus) throws IOException;

}
