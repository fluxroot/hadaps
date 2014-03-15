/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

class BalancerTask implements Callable<BalancerResult> {

  private static final Logger LOG = LoggerFactory.getLogger(BalancerTask.class);

  private final BalancerFile balancerFile;

  public BalancerTask(BalancerFile balancerFile) {
    if (balancerFile == null) throw new IllegalArgumentException();

    this.balancerFile = balancerFile;
  }

  @Override
  public BalancerResult call() throws Exception {
    LOG.info("Start balancing file {}", balancerFile.getName());

    long startTime = Time.now();

    long duration = Time.now() - startTime;

    LOG.info("Balanced file {} in {}", balancerFile.getName(), Utils.getPrettyTime(duration));

    return new BalancerResult();
  }

}
