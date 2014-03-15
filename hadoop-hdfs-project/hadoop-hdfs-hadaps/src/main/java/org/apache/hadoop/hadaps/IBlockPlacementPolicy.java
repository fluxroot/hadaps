/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;

interface IBlockPlacementPolicy {

  void initialize(Configuration configuration) throws IOException;
  List<BalancerNode> chooseTarget(BalancerFile balancerFile);

}
