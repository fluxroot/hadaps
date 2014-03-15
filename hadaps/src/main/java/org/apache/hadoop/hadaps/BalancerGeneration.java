/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

class BalancerGeneration {

  private final ParameterGeneration parameterGeneration;

  public BalancerGeneration(ParameterGeneration parameterGeneration) {
    if (parameterGeneration == null) throw new IllegalArgumentException();

    this.parameterGeneration = parameterGeneration;
  }

}
