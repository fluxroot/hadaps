/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

class HadapsBlockPlacementPolicy implements IBlockPlacementPolicy {

  private final List<BalancerNode> dataNodes;
  private final List<ParameterGeneration> parameterGenerations;

  private final List<BalancerGeneration> generations = new ArrayList<BalancerGeneration>();

  public HadapsBlockPlacementPolicy(List<BalancerNode> dataNodes, List<ParameterGeneration> parameterGenerations) {
    if (dataNodes == null) throw new IllegalArgumentException();
    if (parameterGenerations == null) throw new IllegalArgumentException();

    this.dataNodes = dataNodes;
    this.parameterGenerations = parameterGenerations;
  }

  @Override
  public void initialize(Configuration configuration) throws IOException {
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
        int index = random.nextInt(dataNodes.size());
        node = dataNodes.get(index);
      } while (targetNodes.contains(node));

      targetNodes.add(node);
    }

    return targetNodes;
  }

}
